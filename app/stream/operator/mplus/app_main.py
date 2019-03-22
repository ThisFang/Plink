# -- coding: UTF-8 


from app.stream.operator.base import OperatorBase
from app.stream.operator import base
from app.stream.operator.mplus.traffic import Traffic


class AppMain(OperatorBase):
    def __init__(self, boot_conf):
        super(AppMain, self).__init__(boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.NormalInitFlatMap())

        traffic_stream = Traffic.stream_explode(stream)

        return traffic_stream
