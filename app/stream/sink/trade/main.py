# -- coding: UTF-8 

from app.stream.sink.base import SinkBase, TopicSelector
from app.stream.sink.trade.aggpay import AggPay
from app.stream.sink.trade.deposit import Deposit


class Main(SinkBase):
    def __init__(self, boot_conf):
        SinkBase.__init__(self, boot_conf)

    def write_by_stream(self, stream):
        stream = stream. \
            split(TopicSelector())

        # 聚合数据（将待支付及失败数据发往deposit流）
        agg_pay_stream = stream.select('agg_pay')
        agg_to_deposit_stream = AggPay.stream_end(agg_pay_stream)

        # 出入金数据（将入金支付成功数据发往agg_pay流）
        deposit_stream = stream.select('deposit')
        deposit_stream = deposit_stream.union(agg_to_deposit_stream)
        deposit_success_to_agg_stream = Deposit.stream_end(deposit_stream)

        # 处理聚合支付成功数据流
        AggPay.stream_end(deposit_success_to_agg_stream)
