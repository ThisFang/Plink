# -- coding: UTF-8 

from app.stream.sink.mplus.traffic import Traffic
from app.stream.sink.base import SinkBase
from app.stream.sink import base


class AppMain(SinkBase):
    def __init__(self, boot_conf):
        SinkBase.__init__(self, boot_conf)

    def write_by_stream(self, stream):
        Traffic.stream_end(stream)
