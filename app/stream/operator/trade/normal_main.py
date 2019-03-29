# -- coding: UTF-8

from app.stream.operator.base import OperatorBase
from app.stream.operator import base
from app.stream.operator.trade.aggpay import AggPay
from app.stream.operator.trade.deposit import Deposit


class NormalMain(OperatorBase):
    def __init__(self, boot_conf):
        OperatorBase.__init__(self, boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.NormalInitFlatMap()). \
            split(base.TopicSelector())

        # 聚合订单流处理
        agg_pay_stream = stream.select('agg_pay')
        agg_pay_stream = AggPay.stream_explode(agg_pay_stream)

        # 入金流处理
        deposit_stream = stream.select('deposit')
        deposit_stream = Deposit.stream_explode(deposit_stream)

        stream = agg_pay_stream.union(
            deposit_stream
        )
        return stream
