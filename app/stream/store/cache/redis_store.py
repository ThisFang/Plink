# -- coding: UTF-8

import json
import redis


class RedisStore:
    def __init__(self, conf):
        """初始化构造函数"""
        self.__conf = conf
        self.connection = None

    def get_connection(self):
        """获取链接"""
        pool = redis.ConnectionPool(**self.__conf)
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
