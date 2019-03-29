# -- coding: UTF-8 


from app.stream.operator.base import OperatorBase
from app.stream.operator import base


class CountlyToGateway(OperatorBase):
    def __init__(self, boot_conf):
        OperatorBase.__init__(self, boot_conf)

    def get_stream(self, stream):
        stream = stream.flat_map(base.CountlyInitToGatewayFlatMap())

        return stream


