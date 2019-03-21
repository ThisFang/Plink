# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
import time
from app.utils.enums import DepositTypeEnum


class Deposit:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(DepositSave())


class DepositSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            logger(LogName.DEPOSIT).info('deposit_origin:{}'.format(item))
            try:
                deposit_obj = DepositArgs(item)
                deposit_obj = deposit_obj.to_dict()
            except Exception as e:
                logger(LogName.DEPOSIT).error('{}, {}, {}'.format(topic, e, item))
            else:
                logger(LogName.DEPOSIT).info('deposit_obj:{}'.format(deposit_obj))
                collector.collect((topic, json.dumps(deposit_obj)))


class DepositArgs:
    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_order_no()
        self._explode_pay_time()
        self._explode_order_time()
        self._explode_type()
        self._explode_status_detail()   # 保证此项逻辑先于状态处理
        self._explode_status()
        self._explode_is_first()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_account()
        self._explode_money()
        self._explode_level()

    def _explode_order_no(self):
        buss_no = self.args.get('bussid')
        if buss_no is None:
            raise ValueError('cannot get buss_no')
        self.buss_no = str(buss_no)

        order_no = self.args.get('orderid')
        if order_no is not None:
            order_no = str(order_no)
        self.order_no = order_no

    def _explode_pay_time(self):
        pay_time = self.args.get('paytime')
        if pay_time is not None:
            pay_time = time.strptime(pay_time, '%Y-%m-%d %H:%M:%S')
            pay_time = int(time.mktime(pay_time))
        self.pay_time = pay_time

    def _explode_order_time(self):
        pay_time = self.args.get('paytime')
        order_time = self.args.get('ordertime', pay_time)
        if order_time is not None:
            order_time = time.strptime(order_time, '%Y-%m-%d %H:%M:%S')
            self.order_date = time.strftime('%Y-%m-%d', order_time)
            self.order_hour = int(time.strftime('%H', order_time))
            order_time = int(time.mktime(order_time))
        self.order_time = order_time

    def _explode_status(self):
        status = self.args.get('status', 0)
        status = int(status)
        if self.type == DepositTypeEnum.incoming:
            # 入金状态
            # 0 未入金
            # 1 已入金
            # 2 入金失败
            # 3 处理中
            status_dict = {
                1: 2  # 成功
            }
        elif self.type == DepositTypeEnum.withdraw:
            # 出金状态
            # 1 申请出金
            # 2 审核通过
            # 3 系统出金
            # 4 银行出金
            # 5 初审资金不足
            # 6 取消
            # 7 客户确认
            # 8 退回MT4
            # 9 赠金取消
            # 10 初审联系客服
            # 11 出金完成
            # 12 退回MT4成功
            # 13 MT4操作失败
            # 14 等待复审
            # 15 复审资金不足
            # 16 复审联系客服
            status_dict = {
                # 4: 2,  # 成功（modify:2019-03-07 业务上将4定型为过程状态）
                11: 2,  # 成功
            }
        else:
            status_dict = {}

        # 不成功全部算在待支付
        status = status_dict.get(status, 1)
        self.status = status

    def _explode_status_detail(self):
        status = self.args.get('status', 0)
        if status is not None:
            status = str(status)
        self.status_detail = status

    def _explode_is_first(self):
        is_first = self.args.get('isfirst')
        if is_first is None:
            raise ValueError('cannot get isfirst')
        is_first_dict = {
            0: 2,
            1: 1
        }
        is_first = int(is_first)
        is_first = is_first_dict.get(is_first)
        self.is_first = is_first

    def _explode_plat(self):
        plat = self.args.get('plat')
        if plat is None:
            raise ValueError('cannot get plat')
        self.plat = int(plat)

    def _explode_agent_type(self):
        agent_type = self.args.get('fromplat')
        if agent_type is None:
            raise ValueError('cannot get agent_type')
        if agent_type == 0:
            raise ValueError('cannot get right agent_type')
        agent_dict = {
            1: 1,
            2: 2,
            3: 4,
            4: 5
        }
        agent_type = int(agent_type)
        agent_type = agent_dict.get(agent_type)
        self.agent_type = agent_type

    def _explode_account(self):
        account = self.args.get('account')
        if account is not None:
            account = str(account)
        self.account = account

    def _explode_money(self):
        rmb = int(self.args.get('actmoney', 0))
        usd = int(self.args.get('money', 0))
        money_rate = float(self.args.get('rate', 0))
        if rmb < 0 or usd < 0:
            raise ValueError('cannot get right money')
        self.rmb = rmb
        self.usd = usd
        self.money_rate = money_rate

    def _explode_type(self):
        cmd_type = self.args.get('type')
        if cmd_type is None:
            raise ValueError('cannot get type')
        self.type = int(cmd_type)

    def _explode_level(self):
        # 出金不评判等级
        if self.type == 2:
            self.level = 1
            return

        level_dict = {
            1: (0, 20000),
            2: (20001, 50000),
            3: (50001, 100000),
            4: (100001, 250000),
            5: (250001, 500000),
            6: (500001, 1000000),
            7: (1000001, float('inf')),
        }

        num = self.usd
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

    def __dict(self):
        deposit = {
            'order_time': self.order_time,
            'order_date': self.order_date,
            'order_hour': self.order_hour,
            'pay_time': self.pay_time,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'order_no': self.order_no,
            'buss_no': self.buss_no,
            'account': self.account,
            'type': self.type,
            'is_first': self.is_first,
            'rmb': self.rmb,
            'usd': self.usd,
            'money_rate': self.money_rate,
            'status': self.status,
            'status_detail': self.status_detail,
            'level': self.level,
        }
        return deposit

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
