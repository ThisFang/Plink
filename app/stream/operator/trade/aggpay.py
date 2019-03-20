# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger


class AggPay:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(AggPaySave())


class AggPaySave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                agg_pay_obj = AggPayArgs(item)
                agg_pay_obj = agg_pay_obj.to_dict()
            except Exception as e:
                logger('deposit').error('{}, {}, {}'.format(topic, e, item))
            else:
                logger('deposit').info('agg_pay_obj:{}'.format(agg_pay_obj))
                collector.collect((topic, json.dumps(agg_pay_obj)))


class AggPayArgs:
    """
    将redis拿到的数据格式化成对象
    """

    def __str__(self):
        return self.to_json()

    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_plat()
        self._explode_agent_type()
        self._explode_buss_no()
        self._explode_order_no()
        self._explode_account()
        self._explode_money()
        self._explode_channel()
        self._explode_channel_desc()
        self._explode_status()
        self._explode_is_first()
        self._explode_pay_source()
        self._explode_order_time()
        self._explode_pay_time()

    def _explode_plat(self):
        plat = self.args.get('plat')
        if plat is None:
            raise ValueError('cannot get plat')
        self.plat = int(plat)

    def _explode_agent_type(self):
        agent_type = self.args.get('fromplat')
        if agent_type is None:
            raise ValueError('cannot get fromplat')
        if agent_type == 0:
            raise ValueError('cannot get right agent_type')
        agent_dict = {
            1: 1,
            2: 2,
            3: 4,
            4: 5
        }
        agent_type = agent_dict.get(agent_type)
        self.agent_type = int(agent_type)

    def _explode_buss_no(self):
        buss_no = self.args.get('bussid')
        if buss_no is None:
            raise ValueError('cannot get bussid')
        self.buss_no = str(buss_no)
        
    def _explode_order_no(self):
        order_no = self.args.get('orderid')
        if order_no is not None:
            order_no = str(order_no)
        self.order_no = order_no

    def _explode_account(self):
        account = self.args.get('account')
        if account is None:
            raise ValueError('cannot get account')
        self.account = str(account)

    def _explode_money(self):
        rmb = int(self.args.get('actmoney', 0))
        usd = int(self.args.get('money', 0))
        money_rate = int(self.args.get('money_rate', 0))
        if rmb < 0 or usd < 0:
            raise ValueError('cannot get right money')
        if rmb > 0 and usd > 0:
            money_rate = float(rmb)/float(usd)
        self.rmb = rmb
        self.usd = usd
        self.money_rate = money_rate

    def _explode_channel(self):
        channel = self.args.get('paytype')
        if channel is not None:
            channel = str(channel)
        self.channel = channel

    def _explode_channel_desc(self):
        channel_desc = self.args.get('paymethod')
        self.channel_desc = channel_desc

    def _explode_status(self):
        status = self.args.get('status')
        if status is None:
            raise ValueError('cannot get status')
        status_dict = {
            0: 1,
            1: 2,
            2: 3
        }
        status = status_dict.get(status, 0)
        self.status = int(status)

    def _explode_is_first(self):
        is_first = self.args.get('isfirst')
        if is_first is None:
            raise ValueError('cannot get isfirst')
        is_first_dict = {
            0: 2,
            1: 1
        }
        is_first = is_first_dict.get(is_first, 0)
        self.is_first = int(is_first)

    def _explode_pay_source(self):
        pay_source = self.args.get('paysource')
        if pay_source is None:
            raise ValueError('cannot get paysource')
        self.pay_source = str(pay_source)

    def _explode_order_time(self):
        order_time = self.args.get('addtime')
        if order_time is None and order_time != '':
            raise ValueError('cannot get addtime')
        if len(str(order_time)) < 10:
            raise ValueError('cannot get right addtime')
        order_time = int(Func.string2timestamp(order_time))
        order_time = int(order_time * (10 ** (10 - len(str(order_time)))))
        self.order_time = order_time
        self.order_date = Func.get_date(order_time, '%Y-%m-%d')
        self.order_hour = int(Func.get_date(order_time, '%H'))
    
    def _explode_pay_time(self):
        pay_time = self.args.get('paytime')
        if pay_time is not None and pay_time != '':
            if len(str(pay_time)) < 10:
                raise ValueError('cannot get right paytime')
            pay_time = int(Func.string2timestamp(pay_time))
            pay_time = int(pay_time * (10 ** (10 - len(str(pay_time)))))
        self.pay_time = pay_time

    def __dict(self):
        agg_pay = {
            'plat': self.plat,
            'agent_type': self.agent_type,
            'buss_no': self.buss_no,
            'order_no': self.order_no,
            'account': self.account,
            'rmb': self.rmb,
            'usd': self.usd,
            'money_rate': self.money_rate,
            'channel': self.channel,
            'channel_desc': self.channel_desc,
            'status': self.status,
            'is_first': self.is_first,
            'pay_source': self.pay_source,
            'order_time': self.order_time,
            'order_date': self.order_date,
            'order_hour': self.order_hour,
            'pay_time': self.pay_time,
        }
        return agg_pay

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
