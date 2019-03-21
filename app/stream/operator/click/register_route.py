# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase


class RegisterRoute:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(RegisterRouteSave())


class RegisterRouteSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                register_route_obj = RegisterRouteArgs(item)
                register_route_obj = register_route_obj.to_dict()
                register_route_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect((topic, json.dumps(register_route_obj)))


class RegisterRouteArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('register_route')

    def _explode_click(self, category):
        self.click_category = category
        click_action = self.args.get('click_action', '')
        if not click_action:
            raise ValueError('cannot get click_action')
        click_action = str(click_action)
        self.click_action = click_action

        click_value = self.args.get('click_value', '')
        if not click_action:
            raise ValueError('cannot get click_value')
        self.click_value = str(click_value)
