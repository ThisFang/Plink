# -*- coding:utf-8 -*-


BOOT = [
    {
        'name': 'mplus-traffic-mq',
        'module': 'mplus.normal_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus}mq_keys',
        'sink_driver': 'mplus.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'mplus-traffic-for-app-mq',
        'module': 'mplus.normal_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus/app_traffic}mq_keys',
        'sink_driver': 'mplus.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'mplus-countly-to-gateway-mq',
        'module': 'mplus.countly_to_gateway',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus/i}mq_keys',
        'sink_driver': 'mplus.countly_main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'mplus-deposit-mq',
        'module': 'trade.normal_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus/deposit}mq_keys',
        'sink_driver': 'trade.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'mplus-click-mq',
        'module': 'click.main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus/click}mq_keys',
        'sink_driver': 'click.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'mplus-login-mq',
        'module': 'vip_login.main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{mplus/login}mq_keys',
        'sink_driver': 'vip_login.main',
        'sink_conf': 'base',
        'parallelism': 1
    }
]
