# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName
from app.stream.operator.click.click_base import ClickArgsBase


class News:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(NewsSave())


class NewsSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                news_obj = NewsArgs(item)
                news_obj = news_obj.to_dict()
                news_obj['ip'] = ip
            except Exception as e:
                logger(LogName.CLICK).error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect((topic, json.dumps(news_obj)))


class NewsArgs(ClickArgsBase):
    def __init__(self, args):
        ClickArgsBase.__init__(self, args)
        self._explode_click('news')
