# -- coding: UTF-8 

from app.stream.operator.base import OperatorBase
from app.stream.operator import base
from app.stream.operator.click.advice import Advice
from app.stream.operator.click.deposit_funnel import DepositFunnel
from app.stream.operator.click.deposit_route import DepositRoute
from app.stream.operator.click.news import News
from app.stream.operator.click.pay_route import PayRoute
from app.stream.operator.click.register_funnel import RegisterFunnel
from app.stream.operator.click.register_route import RegisterRoute


class Main(OperatorBase):
    def __init__(self, boot_conf):
        super(Main, self).__init__(boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.NormalInitFlatMap()). \
            split(base.TopicSelector())

        deposit_route_stream = stream.select('deposit_route')
        deposit_route_stream = DepositRoute.stream_explode(deposit_route_stream)

        pay_route_stream = stream.select('pay_route')
        pay_route_stream = PayRoute.stream_explode(pay_route_stream)

        deposit_funnel_stream = stream.select('deposit_funnel')
        deposit_funnel_stream = DepositFunnel.stream_explode(deposit_funnel_stream)

        register_funnel_stream = stream.select('register_funnel')
        register_funnel_stream = RegisterFunnel.stream_explode(register_funnel_stream)

        register_route_stream = stream.select('register_route')
        register_route_stream = RegisterRoute.stream_explode(register_route_stream)

        advice_stream = stream.select('advice')
        advice_stream = Advice.stream_explode(advice_stream)

        news_stream = stream.select('news')
        news_stream = News.stream_explode(news_stream)

        stream = deposit_route_stream.union(
            pay_route_stream,
            deposit_funnel_stream,
            register_funnel_stream,
            register_route_stream,
            advice_stream,
            news_stream,
        )
        return stream
