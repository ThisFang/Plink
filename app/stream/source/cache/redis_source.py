# -- coding: UTF-8

from conf import get_conf
from app.stream.source.base import SourceBase
import json
from app.utils import logger
from app.stream.store.cache import RedisStore


class RedisSource(SourceBase):
    def __init__(self, boot_conf):
        SourceBase.__init__(self, boot_conf)
        self.RedisStore = RedisStore(self.boot_conf.get('source_conf'))

    def get_handler(self):
        """获取链接"""
        connection = self.RedisStore.get_connection()
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
        try:
            mq_key, mq_elements = self.RedisStore.read_buffer(source_topic)
            return self._collect_from_elements(ctx, mq_key, mq_elements)
        except Exception as e:
            logger().error('Redis get exception: {}'.format(e))
            return None

    @staticmethod
    def _collect_from_elements(ctx, mq_key, mq_elements):
        """
        内部函数，用于收集来自于适配器中的数据
        """
        if mq_key:
            for element in mq_elements:
                element = json.loads(element)
                method = element.get('Method')
                if method != 'POST':
                    continue
                headers = element.get('Headers', {})
                ip = headers.get('host', '0.0.0.0')
                args = json.dumps(element.get('Args'))
                gateway_uri = element.get('Uri', '')
                receive_time = element.get('ReceiveTime', 0)
                ctx.collect((method, ip, args, gateway_uri, receive_time))

        return mq_key

