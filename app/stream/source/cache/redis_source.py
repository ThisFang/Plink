# -- coding: UTF-8

from conf import get_conf
from app.stream.source.base import SourceBase
import json
from app.utils import logger
from app.stream.store.cache import RedisStore


class RedisSource(SourceBase):
    def __init__(self, boot_conf):
        """初始化构造函数"""
        super(RedisSource, self).__init__(boot_conf)
        redis_conf = get_conf('base', 'REDIS').get(self.boot_conf.get('source_conf'))
        self.RedisStore = RedisStore(redis_conf)

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
                element = str(element)
                ctx.collect(element)

        return mq_key

