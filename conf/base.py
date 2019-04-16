# -*- coding:utf-8 -*-

REDIS = {
    'base': {
        'host': '192.168.1.139',
        'port': '6379',
        'password': '',
    }
}

RABBITMQ = {
    'base': {
        'host': '192.168.1.120',
        'port': 5672,
        'username': 'admin',
        'password': '123456'
    }
}

LOGGER = {
    'path': '/clouddisk/logs/flink',
    'level': 'INFO',
    'format': '[{date}] [{level}] [{module}/{filename}] [{func_name}] [{lineno}] {msg}'
}


BASE = {
    'source_wait_second': 5,  # source未读取到数据时挂起时间
    'curl_retry_max': 3  # curl发送失败重试次数
}
