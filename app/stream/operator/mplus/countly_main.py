# -- coding: UTF-8 

from app.stream.operator.base import OperatorBase
from app.stream.operator import base
from app.stream.operator.mplus.traffic import Traffic
from app.stream.operator.mplus.target import Target
from app.stream.operator.mplus.push_app import PushApp
from app.stream.operator.mplus.push_send import PushSend


class CountlyMain(OperatorBase):
    def __init__(self, boot_conf):
        super(CountlyMain, self).__init__(boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.CountlyInitFlatMap()). \
            split(base.TopicSelector())

        traffic_stream = stream.select('traffic')
        traffic_stream = Traffic.stream_explode(traffic_stream)

        target_stream = stream.select('target')
        target_stream = Target.stream_explode(target_stream)

        push_app_stream = stream.select('push_app')
        push_app_stream = PushApp.stream_explode(push_app_stream)

        push_send_stream = stream.select('push_send')
        push_send_stream = PushSend.stream_explode(push_send_stream)

        stream = traffic_stream.union(
            target_stream,
            push_app_stream,
            push_send_stream
        )
        return stream


