# -- coding: UTF-8 

from app.stream.sink.mplus.traffic import Traffic
from app.stream.sink.mplus.target import Target
from app.stream.sink.mplus.push_app import PushApp
from app.stream.sink.mplus.push_send import PushSend
from app.stream.sink.base import SinkBase
from app.stream.sink import base


class Main(SinkBase):
    """初始化构造函数"""

    def __init__(self, boot_conf):
        super(Main, self).__init__(boot_conf)

    def write_by_stream(self, stream):
        stream = stream. \
            split(base.TopicSelector())

        # traffic_stream = stream.select('traffic')
        # Traffic.stream_end(traffic_stream)

        target_stream = stream.select('target')
        Target.stream_end(target_stream)

        # push_app_stream = stream.select('push_app')
        # PushApp.stream_end(push_app_stream)

        # push_send_stream = stream.select('push_send')
        # PushSend.stream_end(push_send_stream)
