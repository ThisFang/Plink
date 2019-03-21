# -- coding: UTF-8 

from app.stream.sink.base import SinkBase
from app.stream.sink import base
from app.stream.sink.vip_login.viplist import ViplistReports
from app.stream.sink.vip_login.vip_login_reports import VipLoginReports


class Main(SinkBase):
    """初始化构造函数"""

    def __init__(self, boot_conf):
        super(Main, self).__init__(boot_conf)

    def write_by_stream(self, stream):
        stream = stream. \
            split(base.TopicSelector())

        viplist_stream = stream.select('viplist')
        ViplistReports.stream_end(viplist_stream)

        vip_login_stream = stream.select('vip_login_reports')
        VipLoginReports.stream_end(vip_login_stream)
