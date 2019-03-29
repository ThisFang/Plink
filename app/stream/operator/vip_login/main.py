# -- coding: UTF-8 

from app.stream.operator.base import OperatorBase
from app.stream.operator import base
from app.stream.operator.vip_login.app_open import AppOpen
from app.stream.operator.vip_login.login import Login
from app.stream.operator.vip_login.vip_open import VipOpen
from app.stream.operator.vip_login.viplist import Viplist


class Main(OperatorBase):
    def __init__(self, boot_conf):
        OperatorBase.__init__(self, boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.NormalInitFlatMap()). \
            split(base.TopicSelector())

        app_open_stream = stream.select('app_open')
        app_open_stream = AppOpen.stream_explode(app_open_stream)

        login_stream = stream.select('login')
        login_stream = Login.stream_explode(login_stream)

        vip_open_stream = stream.select('app_vip_open')
        vip_open_stream = VipOpen.stream_explode(vip_open_stream)

        viplist_stream = stream.select('vip')
        viplist_stream = Viplist.stream_explode(viplist_stream)

        stream = app_open_stream.union(
            login_stream,
            vip_open_stream,
            viplist_stream,
        )
        return stream


