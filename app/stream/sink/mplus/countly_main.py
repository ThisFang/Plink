# -- coding: UTF-8 

from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds
from app.stream.sink.base import SinkBase
from app.common.request import CurlToGateway
import json


class CountlyMain(SinkBase):
    def __init__(self, boot_conf):
        SinkBase.__init__(self, boot_conf)

    def write_by_stream(self, stream):
        stream.key_by(CountlyKeyBy()). \
            time_window(milliseconds(1000)). \
            apply(CountlyApply())


class CountlyKeyBy(KeySelector):
    def getKey(self, value):
        uri, topic, ip, data = value
        key = '{}-{}'.format(uri, topic)
        return key


class CountlyApply(WindowFunction):
    def apply(self, key, window, values, collector):
        data_list = []
        for uri, topic, ip, value in values:
            value = json.loads(value)
            # 补充ip
            try:
                for key in range(len(value)):
                    value[key]['ip'] = ip
            except Exception:
                pass
            data_list = data_list + value

        uri, topic, _, _ = values[0]
        uri = '/{}'.format(uri)

        # 过长做切片处理
        piece_len = 10
        for piece in range(0, len(data_list), piece_len):
            data_piece = data_list[piece:piece + piece_len]
            data = {
                'topic': topic,
                'data': data_piece
            }
            CurlToGateway(uri, json=data).curl()
