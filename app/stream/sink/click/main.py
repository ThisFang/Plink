# -- coding: UTF-8 

import time

from app.stream.sink.base import SinkBase
from app.stream.sink import base
from app.stream.sink.click.advice import Advice
from app.stream.sink.click.deposit_funnel import DepositFunnel
from app.stream.sink.click.deposit_route import DepositRoute
from app.stream.sink.click.news import News
from app.stream.sink.click.pay_route import PayRoute
from app.stream.sink.click.register_funnel import RegisterFunnel
from app.stream.sink.click.register_route import RegisterRoute


class Main(SinkBase):
    """初始化构造函数"""

    def __init__(self, boot_conf):
        super(Main, self).__init__(boot_conf)

    def write_by_stream(self, stream):
        stream = stream. \
            split(base.TopicSelector())

        deposit_route_stream = stream.select('deposit_route')
        DepositRoute.stream_end(deposit_route_stream)

        pay_route_stream = stream.select('pay_route')
        PayRoute.stream_end(pay_route_stream)

        deposit_funnel_stream = stream.select('deposit_funnel')
        DepositFunnel.stream_end(deposit_funnel_stream)

        register_funnel_stream = stream.select('register_funnel')
        RegisterFunnel.stream_end(register_funnel_stream)

        register_route_stream = stream.select('register_route')
        RegisterRoute.stream_end(register_route_stream)

        advice_stream = stream.select('advice')
        Advice.stream_end(advice_stream)

        news_stream = stream.select('news')
        News.stream_end(news_stream)
