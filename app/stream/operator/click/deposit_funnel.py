# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase


class DepositFunnel:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(DepositFunnelSave())


class DepositFunnelSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                deposit_funnel_obj = DepositFunnelArgs(item)
                deposit_funnel_obj = deposit_funnel_obj.to_dict()
                deposit_funnel_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect((topic, json.dumps(deposit_funnel_obj)))


class DepositFunnelArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('deposit_funnel')

    def _explode_click(self, category):
        self.click_category = category
        self.click_value = self.args.get('click_value', '')
        if not self.click_value:
            self.click_value = self.visit_id
        click_action = self.args.get('click_action', '-')
        if not click_action:
            raise ValueError('cannot get click_action')
        self.click_action = click_action
        self.pay_source, self.type = str(click_action).split('-', 1)

    def _dict(self):
        click = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'click_category': self.click_category,
            'click_action': self.click_action,
            'pay_source': self.pay_source,
            'type': self.type,
            'click_value': self.click_value,
            'visit_id': self.visit_id,
        }
        return click
