# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase


class DepositRoute:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(DepositRouteSave())


class DepositRouteSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                deposit_route_obj = DepositRouteArgs(item)
                deposit_route_obj = deposit_route_obj.to_dict()
                deposit_route_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect((topic, json.dumps(deposit_route_obj)))


class DepositRouteArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('deposit_route')

    def _explode_click(self, category):
        self.click_category = category
        self.click_value = self.args.get('click_value', '')
        click_action = self.args.get('click_action', '')
        if not click_action:
            raise ValueError('cannot get click_action')
        self.click_action = click_action
