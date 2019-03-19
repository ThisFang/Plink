# -- coding: UTF-8 

from app.stream.sink.base import SinkBase


class TestSink(SinkBase):
    def write_by_stream(self, data_stream):
        data_stream.output()

