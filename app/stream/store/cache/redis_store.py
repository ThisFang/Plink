# -- coding: UTF-8

import json
import redis
from conf import get_conf


class RedisStore:
    def __init__(self, conf='base'):
        """初始化构造函数"""
        self.__redis_conf = get_conf('base', 'REDIS').get(conf)
        self.connection = None

    def get_connection(self):
        """获取链接"""
        pool = redis.ConnectionPool(**self.__redis_conf)
        connect_args = {
            'connection_pool': pool,
            'socket_timeout': None,
            # 'charset': 'utf-8',
            'errors': 'strict',
            'unix_socket_path': None
        }
        connection = redis.StrictRedis(**connect_args)
        self.connection = connection
        return connection

    def read_buffer(self, buffer_key):
        mq_list = []
        mq_key = self.connection.rpop(buffer_key)
        if mq_key:
            while True:
                val = self.connection.rpop(mq_key)
                if not val:
                    break
                mq_list.append(val)
        return mq_key, mq_list

    @staticmethod
    def traffic_uv_redis_name(plat, agent_type, date, website):
        """访问uv的redis名字"""
        redis_name = 'mplus:uv:click:{}_{}_{}_{}'.format(
            str(date),
            str(plat),
            str(agent_type),
            website.encode('utf-8')
        )
        return redis_name

    @staticmethod
    def click_uv_redis_name(plat, agent_type, date, click_category, click_action):
        """点击事件uv的redis名字"""
        redis_name = 'mplus:uv:click:{}_{}_{}_{}'.format(
            str(date),
            str(plat),
            str(agent_type),
            click_category.encode('utf-8'),
            click_action.encode('utf-8')
        )
        return redis_name

    @staticmethod
    def push_app_uv_redis_name(plat, agent_type, date, detail_id):
        """极光推送app点击uv的redis名字"""
        redis_name = 'mplus:uv:click:{}_{}_{}_{}'.format(
            str(date),
            str(plat),
            str(agent_type),
            str(detail_id)
        )
        return redis_name
