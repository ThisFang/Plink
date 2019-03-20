# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
import json
from sqlalchemy import func
from sqlalchemy import not_,or_,desc
from app.utils import Func, logger
from app.utils.constants import *
from app.utils.enums import PayStatusEnum, DepositTypeEnum, IsFirstEnum, TargetEnum
from app.common.request import CurlToAnalysis
from app.stream.sink import base
from app.stream.store.database import ck_table, ClickhouseStore


class Deposit:
    """初始化构造函数"""

    @staticmethod
    def stream_end(data_stream):
        data_stream = data_stream.map(base.ToData())
        
        ck_insert = base.ClickHouseApply()
        ck_insert.set_table(table=ck_table.DetailsDeposit)

        # 详情落地（出入金）
        deposit_stream = data_stream. \
            flat_map(DetailsDepositFlatMap()). \
            flat_map(ck_insert)

        # 报表落地
        deposit_stream.flat_map(ReportsChannelValueFlatMap())
        
        # 出入金发往聚合数据流（支付成功状态值）
        send_to_agg_pay_stream = deposit_stream.split(StatusSelector()). \
            select(TOPIC_L2_NEED_STATUS). \
            flat_map(FormatDepositToAggFlatMap())

        return send_to_agg_pay_stream


class ReportsChannelValueFlatMap(FlatMapFunction):
    """
    stat_channel_value 报表写入
    """
    def flatMap(self, data, collector):
        try:
            data = json.loads(data)
            if data.get('status') == PayStatusEnum.success and data.get('is_first') == IsFirstEnum.yes:
                reports_data = ChannelValueReports(data, collector).to_dict()
                self.update_stat_channel_value(reports_data)
        except BaseException as err:
            logger().error('{}, {}'.format(err, data))
        else:
            pass

    @staticmethod
    def update_stat_channel_value(reports_data):
        if reports_data is None:
            return

        data = {
            'topic': 'channel_value',
            'data': [
                dict(reports_data)
            ]
        }
        CurlToAnalysis('flow', '/channel_value', 'PATCH', data=data).curl()


class ChannelValueReports:
    """
    stat_channel_value 报表聚合
    """
    def __init__(self, details, collector):
        self.details = details
        self.ck_session = ClickhouseStore().get_session()
        self.get_target_info()
        if self.target_info is not None:
            self.is_exists_n = True
        else:
            self.is_exists_n = False

        collector.collect(json.dumps(self.details))

    def get_target_info(self):
        """ 获取N类型指定账号的target详情数据 """
        DetailsTarget = ck_table.DetailsTarget
        target_type = TargetEnum.N

        self.target_info = self.ck_session.query(
            DetailsTarget.target_type,
            DetailsTarget.agent_type,
            DetailsTarget.source,
            DetailsTarget.medium,
            DetailsTarget.account,
            DetailsTarget.req_date,
            DetailsTarget.req_time). \
            filter(DetailsTarget.target_type == target_type). \
            filter(DetailsTarget.account == self.details.get('account')). \
            order_by(desc(DetailsTarget.req_time)). \
            first()

    def _get_agent_type(self):
        """ 以N的数据为准，N未存在则使用A """
        if self.is_exists_n:
            return self.target_info.agent_type
        else:
            return self.details.get('agent_type', 1)
        
    def _get_channel(self):
        if self.is_exists_n and self.target_info.medium:
            return self.target_info.medium
        else:
            return '(not set)'

    def _get_is_n_to_a(self):
        """ 是否当天N转A """
        if self.is_exists_n and str(self.details.get('order_date')) == str(self.target_info.req_date):
            return True
        else:
            return False

    def to_dict(self):
        primary_dict = {
            'plat': self.details.get('plat', 0),
            'agent_type': self._get_agent_type(),
            'channel':self._get_channel(),
            'a_usd': self.details.get('usd', 0),
            'a_rmb': self.details.get('rmb', 0),
            'account': self.details.get('account', 0),
            'is_n_to_a': self._get_is_n_to_a(),
            'stat_time': Func.get_date(time_format='%Y-%m-%d %H:%M:%S'),
            'stat_date': Func.get_date(),
            'stat_hour': 0,
        }

        self.ck_session.close()
        return primary_dict


class DetailsDepositFlatMap(FlatMapFunction):
    def flatMap(self, data, collector):
        try:
            data = json.loads(data)
            primary_dict = PersistenceDetailsDeposit(data).to_dict()
        except BaseException as err:
            logger().error('{}, {}'.format(err, data))
        else:
            if primary_dict is not None:
                collector.collect(json.dumps(primary_dict))
            

class PersistenceDetailsDeposit:
    def __init__(self, details):
        self.details = details
        self.ck_session = ClickhouseStore().get_session()
        
        self.is_insert = None

        # 查询buss_no是否支付完成订单
        # True:删除同一订单未支付数据并传递插入参数，False:直接传递插入参数
        # 组装完整数据插入
        flag = self.is_pay_completed()
        if flag == 0:
            self.is_insert = True
        elif flag == 1:
            if self.details.get('status') == PayStatusEnum.pending:
                self.is_insert = False
            else:
                self.is_insert = True
                self.delete_same_order()
        elif flag == 2 or flag == 3:
            self.is_insert = False
        else:
            self.is_insert = False
            logger().info("数据库中订单状态未知，待插入源数据为：{}".format(self.details))

    def delete_same_order(self):
        buss_no = self.details.get('buss_no')

        DetailsDeposit = ck_table.DetailsDeposit
        filter_obj = self.ck_session.query(DetailsDeposit). \
            filter(DetailsDeposit.buss_no == buss_no). \
            filter(DetailsDeposit.status == PayStatusEnum.pending)
        ClickhouseStore.delete(DetailsDeposit, filter_obj)

    def is_pay_completed(self):
        """ 指定订单是否支付完成订单 0:订单号不存在 1：订单号存在且待支付 2：订单号存在且支付成功 3：订单号存在且支付失败 """
        DetailsDeposit = ck_table.DetailsDeposit

        model = self.ck_session.query(DetailsDeposit). \
            filter(DetailsDeposit.buss_no == self.details.get('buss_no')). \
            order_by(desc(DetailsDeposit.add_time)). \
            first()

        if model is not None:
            if model.status == PayStatusEnum.pending:
                # 订单存在未支付
                return 1
            elif model.status == PayStatusEnum.success:
                # 订单存在且支付成功
                return 2
            elif model.status == PayStatusEnum.fail:
                # 订单存在且支付失败
                return 3
            else:
                # 预留
                return -1
        else:
            # 订单未存在
            return 0

    def to_dict(self):
        primary_dict = None
        if self.is_insert:
            primary_dict = {
                'plat': self.details.get('plat') or 0,
                'agent_type': self.details.get('agent_type') or 0,
                'buss_no': self.details.get('buss_no') or '',
                'order_no': self.details.get('order_no') or '',
                'account': self.details.get('account') or '',
                'rmb': self.details.get('rmb') or 0,
                'usd': self.details.get('usd') or 0,
                'money_rate': self.details.get('money_rate') or 0,
                'status': self.details.get('status') or 0,
                'status_detail': self.details.get('status_detail') or '0',
                'level': self.details.get('level') or 0,
                'is_first': self.details.get('is_first') or 0,
                'type': self.details.get('type') or 0,
                'pay_time': self.details.get('pay_time') or CK_NULL_DATETIME,
                'order_time': self.details.get('order_time'),
                'order_date': self.details.get('order_date'),
                'order_hour': self.details.get('order_hour'),
            }
        return primary_dict


class StatusSelector(OutputSelector):
    def select(self, value):
        data = json.loads(value)
        status = data.get('status')
        deposit_type = data.get('type')
        if status in [PayStatusEnum.success] and deposit_type == DepositTypeEnum.incoming:
            return [TOPIC_L2_NEED_STATUS]
        else:
            return [TOPIC_L2_OTHER_STATUS]


class FormatDepositToAggFlatMap(FlatMapFunction):
    def flatMap(self, data, collector):
        self.details = json.loads(data)
        primary_dict = {
            'plat': self.details.get('plat', 1),
            'agent_type': self.details.get('agent_type', 1),
            'buss_no': self.details.get('buss_no', ''),
            'order_no': self.details.get('order_no', ''),
            'account': self.details.get('account', ''),
            'rmb': self.details.get('rmb', 0),
            'usd': self.details.get('usd', 0),
            'money_rate': self.details.get('money_rate', 0),
            'channel': self.details.get('channel', ''),  # 需重新查找值
            'channel_desc': self.details.get('channel_desc', ''),  # 需重新查找值
            'status': self.details.get('status', 0),
            'is_first': self.details.get('is_first', 0),
            'pay_source': self.details.get('pay_source', ''),    # 需重新查找值
            'order_time': self.details.get('order_time', CK_NULL_DATETIME),
            'order_date': self.details.get('order_date', CK_NULL_DATE),
            'order_hour': self.details.get('order_hour', 0),
            'pay_time': self.details.get('pay_time', CK_NULL_DATETIME),
        }
        collector.collect((TOPIC_L1_AGGPAY, json.dumps(primary_dict)))
