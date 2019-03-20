# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
import json
from sqlalchemy import func
from sqlalchemy import not_,or_,desc
from app.utils import logger, Func
from app.utils.constants import *
from app.utils.enums import PayStatusEnum, DepositTypeEnum
from app.common.request import CurlToGateway
from app.stream.sink import base
from app.stream.store.database import ck_table, ClickhouseStore


class AggPay:
    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())
        
        ck_insert = base.ClickHouseApply()
        ck_insert.set_table(table=ck_table.DetailsAggPayOrder)

        # 详情落地流
        agg_pay_stream = data_stream. \
            flat_map(DetailsAggPayFlatMap()). \
            flat_map(ck_insert)

        # 聚合发往出入金数据流（待支付及支付失败状态值）
        send_to_deposit_stream = agg_pay_stream.split(StatusSelector()). \
            select(TOPIC_L2_NEED_STATUS). \
            flat_map(FormatAggToDepositFlatMap())

        return send_to_deposit_stream


class DetailsAggPayFlatMap(FlatMapFunction):
    def flatMap(self, data, collector):
        try:
            data = json.loads(data)
            primary_dict = PersistenceDetailsAggPay(data).to_dict()
        except BaseException as err:
            logger().error('{}, {}'.format(err, data))
        else:
            if primary_dict is not None:
                collector.collect(json.dumps(primary_dict))


class PersistenceDetailsAggPay:
    def __init__(self, details):
        self.details = details
        self.ck_session = ClickhouseStore().get_session()

        self.current_agg_model_on_db = None
        self.is_insert = None

        # 查询buss_no是否支付完成订单
        # True:删除同一订单未支付数据并传递插入参数，False:直接传递插入参数
        # 组装完整数据插入
        flag = self.is_pay_completed()
        if flag == 0:
            self.is_insert = True
            if self.details.get('status') == PayStatusEnum.success:
                # 聚合数据中支付成功的数据重新组装实时发向Redis队列
                self.reset_to_redis_mq()
        elif flag == 1:
            if self.details.get('status') == PayStatusEnum.pending:
                self.is_insert = False
            else:
                self.is_insert = True
                self.delete_same_order()
                if self.details.get('status') == PayStatusEnum.success:
                    self.reset_to_redis_mq()
        elif flag == 2 or flag == 3:
            self.is_insert = False
        else:
            self.is_insert = False
            logger().info('数据库中订单状态未知，待插入源数据为：{}'.format(self.details))
    
    def delete_same_order(self):
        DetailsAggPayOrder = ck_table.DetailsAggPayOrder
        buss_no = self.details.get('buss_no')
        filter_obj = self.ck_session.query(DetailsAggPayOrder). \
            filter(DetailsAggPayOrder.buss_no == buss_no). \
            filter(DetailsAggPayOrder.status == PayStatusEnum.pending). \
            filter(DetailsAggPayOrder.pay_time == CK_NULL_DATETIME)
        ClickhouseStore.delete(DetailsAggPayOrder, filter_obj)

    def get_current_agg_model(self):
        DetailsAggPayOrder = ck_table.DetailsAggPayOrder

        model = self.ck_session.query(DetailsAggPayOrder). \
            filter(DetailsAggPayOrder.buss_no == self.details.get('buss_no')). \
            order_by(desc(DetailsAggPayOrder.add_time)). \
            first()

        self.current_agg_model_on_db = model

    def is_pay_completed(self):
        """ 指定订单是否支付完成订单 0:订单号不存在 1：订单号存在且待支付 2：订单号存在且支付成功 3：订单号存在且支付失败 """
        self.get_current_agg_model()
        model = self.current_agg_model_on_db

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
                'channel': self.details.get('channel') or '',
                'channel_desc': self.details.get('channel_desc') or '',
                'status': self.details.get('status') or 0,
                'is_first': self.details.get('is_first') or 0,
                'pay_source': self.details.get('pay_source') or '',
                'order_time': self.details.get('order_time') or CK_NULL_DATETIME,
                'order_date': self.details.get('order_date') or CK_NULL_DATE,
                'order_hour': self.details.get('order_hour') or 0,
                'pay_time': self.details.get('pay_time') or CK_NULL_DATETIME,
            }

            # 2019-02-22 新增
            if self.details.get('status') == PayStatusEnum.success and (primary_dict.get('pay_source') is None or primary_dict.get('pay_source') == ""):
                no_value_default = 'unknow'
                if primary_dict['pay_time'] == CK_NULL_DATETIME:
                    primary_dict['pay_time'] = self.details.get('order_time')

                if self.current_agg_model_on_db is not None:
                    primary_dict['pay_source'] = self.current_agg_model_on_db.pay_source
                    primary_dict['channel'] = self.current_agg_model_on_db.channel
                    primary_dict['channel_desc'] = self.current_agg_model_on_db.channel_desc
                else:
                    primary_dict['pay_source'] = no_value_default
                    primary_dict['channel'] = no_value_default
                    primary_dict['channel_desc'] = no_value_default

        self.ck_session.close()
        return primary_dict

    def reset_to_redis_mq(self):
        """ 聚合支付成功订单数据重组装发向Redis队列(点击事件key) """
        pay_route_dict = {
            'topic': 'deposit_funnel',
            'data': [{
                'plat': self.details.get('plat'),
                'agent_type': self.details.get('agent_type'),
                'click_action': self.get_click_action(),
                'click_value': self.details.get('account'),
                'visit_id': '',
                'req_time': self.details.get('pay_time') or self.details.get('order_time')
            }]
        }

        result = CurlToGateway(uri='/mplus/click', json=pay_route_dict).curl()
        logger().info('聚合发往点击事件数据 -> data:{},result:{}'.format(pay_route_dict, result))

    def get_click_action(self):
        prefix = PAY_SOURCE_POLY    # 无 pay_source 标识的自动归到4（聚合支付）类
        separator = '-'
        suffix = 'success'
        # 根据“pay_source”找到对应值作为“pay_source”前辍
        for item in PAY_SOURCE_FLAG_CONFIG.keys():
            if self.current_agg_model_on_db is None:
                break

            if str(self.current_agg_model_on_db.pay_source) in item:
                prefix = PAY_SOURCE_FLAG_CONFIG[str(item)]
        
        return '{}{}{}'.format(prefix, separator, suffix)


class StatusSelector(OutputSelector):
    def select(self, value):
        data = json.loads(value)
        status = data.get('status')
        if status in [PayStatusEnum.pending, PayStatusEnum.fail]:
            return [TOPIC_L2_NEED_STATUS]
        else:
            return [TOPIC_L2_OTHER_STATUS]


class FormatAggToDepositFlatMap(FlatMapFunction):
    def flatMap(self, data, collector):
        self.details = json.loads(data)
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
            'status_detail': self._recovery_status(),
            'is_first': self.details.get('is_first') or 0,
            'order_time': self.details.get('order_time') or CK_NULL_DATETIME,
            'order_date': self.details.get('order_date') or CK_NULL_DATE,
            'order_hour': self.details.get('order_hour') or 0,
            'type': DepositTypeEnum.incoming,
            'level': self._get_level(),
            'pay_time': CK_NULL_DATETIME,
        }
        collector.collect((TOPIC_L1_DEPOSIT, json.dumps(primary_dict)))

    def _recovery_status(self):
        status = self.details.get('status')
        status_dict = {
            1: '0',
            2: '1',
            3: '2'
        }
        status = status_dict.get(status, 1)
        return status

    def _get_level(self):

        level_dict = {
            1: (0, 20000),
            2: (20001, 50000),
            3: (50001, 100000),
            4: (100001, 250000),
            5: (250001, 500000),
            6: (500001, 1000000),
            7: (1000001, float('inf')),
        }

        num = self.details.get('usd')
        level_list = list(level_dict.keys())
        # 从中间等级开始num大了就往后的等级找，小了往前找
        start_pos = int(len(level_list) / 2)
        pos = start_pos
        while len(level_list) != 0:
            level_key = level_list.pop(pos)
            min_money, max_money = level_dict.get(level_key)
            if min_money > num:
                pos = pos - 1
                continue
            if max_money < num:
                continue
            self.level = level_key
            break
        return self.level
