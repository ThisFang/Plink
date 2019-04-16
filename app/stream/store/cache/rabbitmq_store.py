# -- coding: UTF-8

from conf import get_conf
import pika


class RabbitmqStore:
    def __init__(self, conf='base'):
        """初始化构造函数"""
        self.__rabbitmq_conf = get_conf('base', 'RABBITMQ').get(conf)
        self.connection = None

    def get_connection(self):
        """获取链接"""
        credentials = pika.PlainCredentials(
            username=self.__rabbitmq_conf.get('username', 'guest'),
            password=self.__rabbitmq_conf.get('password', 'guest')
        )

        parameters = pika.ConnectionParameters(
            host=self.__rabbitmq_conf.get('host', 'localhost'),
            port=self.__rabbitmq_conf.get('port', 5672),
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        self.connection = connection
        return connection
