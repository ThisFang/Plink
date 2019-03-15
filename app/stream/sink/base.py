# -- coding: UTF-8 
from app.common import SuperBase


class SinkBase(SuperBase):
    def __init__(self, boot_conf):
        super(SinkBase, self).__init__(boot_conf)

    def write_by_stream(self, data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        raise NotImplementedError
