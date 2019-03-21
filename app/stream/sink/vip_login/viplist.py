# -- coding: UTF-8

from app.stream.sink import base
from app.stream.sink.vip_login import vip_login_base


class ViplistReports:
    @staticmethod
    def stream_end(data_stream):
        data_stream = data_stream.map(base.ToData())

        # 报表落地
        reports = data_stream. \
            flat_map(vip_login_base.VipLoginReportsFlatMap())
