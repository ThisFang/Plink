# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase


class Advice:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(AdviceSave())


class AdviceSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                advice_obj = AdviceArgs(item)
                advice_obj = advice_obj.to_dict()
                advice_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect((topic, json.dumps(advice_obj)))


class AdviceArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('advice')
        self._explode_market()

    def _explode_click(self, category):
        self.click_category = category
        click_action = self.args.get('click_action', '')
        self.click_action = str(click_action)

    def _explode_market(self):
        if self.agent_type > 3:
            self.source = ('ios' if self.agent_type == 4 else 'android')
            self.medium = self.args.get('click_value', '')
        else:
            market = self.args.get('market', {})
            self.source = str(market.get('source', '')) or str(self.args.get('source', ''))
            self.medium = str(market.get('medium', '')) or str(self.args.get('medium', ''))

        self.click_value = '{}-{}'.format(self.source, self.medium)

    def _dict(self):
        click = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'click_category': self.click_category,
            'click_action': self.click_action,
            'click_value': self.click_value,
            'source': self.source,
            'medium': self.medium,
            'visit_id': self.visit_id,
        }
        return click
