# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase
from app.utils.constants import *


class PayRoute:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(PayRouteSave())


class PayRouteSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                pay_route_obj = PayRouteArgs(item)
                pay_route_dict = pay_route_obj.to_dict()
                deposit_funnel_obj = pay_route_obj.to_deposit_funnel()
                pay_route_dict['ip'] = ip
                if deposit_funnel_obj:
                    deposit_funnel_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                if deposit_funnel_obj:
                    collector.collect(('deposit_funnel', json.dumps(deposit_funnel_obj)))


class PayRouteArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('pay_route')

    def _explode_click(self, category):
        self.click_category = category
        self.click_value = self.args.get('click_value', '')
        click_action = self.args.get('click_action', '')
        if not click_action:
            raise ValueError('cannot get click_action')
        click_action = str(click_action)
        self.click_action = click_action

    def to_deposit_funnel(self):
        action_list = {
            '2': 'pay',
            '4': 'cash_arrive',
            '5': 'cash_pay',
            '7': 'success'
        }
        action = action_list.get(self.click_action)
        if not action:
            return None

        click_value = self.click_value if self.click_value else self.visit_id

        click = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'pay_source': PAY_SOURCE_POLY,
            'click_category': 'deposit_funnel',
            'click_action': '{}-{}'.format(PAY_SOURCE_POLY, action),
            'type': action,
            'click_value': click_value,
            'visit_id': self.visit_id,
        }
        return click
