# -- coding: UTF-8

from conf import get_conf
from app.stream.source.base import SourceBase
import json
from app.utils import logger
from app.stream.store.cache import RabbitmqStore


class RabbitmqSource(SourceBase):
    def __init__(self, boot_conf):
        SourceBase.__init__(self, boot_conf)
        self.RabbitmqStore = RabbitmqStore(self.boot_conf.get('source_conf'))

    def get_handler(self):
        """获取链接"""
        connection = self.RabbitmqStore.get_connection()
        self.connection = connection
        return connection

    def get_position_key(self):
        return

    def set_position(self, position):
        return

    def get_position(self):
        return

    def restore_hung_up(self, ctx):
        return True

    def mount(self, ctx):
        """
        将缓冲区的内容装载至指定位置
        外围项目启动通过当前环境的解释器去运行脚本程式，否则会出现找不到现有项目模块的问题
        """
        source_topic = self.boot_conf.get('source_topic')

        def callback(ch, method, properties, body):
            ctx.collect(self._collect_from_elements(body))

            # 将成功收集的tag删除
            delivery_tag = int(method.delivery_tag)
            ch.basic_ack(delivery_tag=delivery_tag)

        try:
            channel = self.connection.channel()
            channel.queue_declare(queue=source_topic, durable=True)
            channel.basic_consume(
                queue=source_topic,
                consumer_callback=callback,
                no_ack=False
            )
            channel.start_consuming()
        except Exception as e:
            logger().error('RabbitMQ get exception: {}'.format(e))
            return None

    @staticmethod
    def _collect_from_elements(body):
        """
        内部函数，用于收集来自于适配器中的数据
        """
        body = json.loads(body)
        method = body.get('Method')
        headers = body.get('Headers', {})
        ip = headers.get('host', '0.0.0.0')
        args = json.dumps(body.get('Args'))
        gateway_uri = body.get('Uri', '')
        receive_time = body.get('ReceiveTime', 0)
        return method, headers, ip, args, gateway_uri, receive_time

