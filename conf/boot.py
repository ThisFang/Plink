# -*- coding:utf-8 -*-


BOOT = [
    {
        'name': 'mplus-traffic-for-app-mq',
        'module': 'mplus.normal_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{test}mq_keys',
        'sink_driver': 'mplus.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    # {
    #     'name': 'mplus-countly-to-gateway-mq',
    #     'module': 'mplus.countly_to_gateway',
    #     'source_driver': 'cache.redis_source',
    #     'source_conf': 'base',
    #     'source_topic': 'buffer->topic{test}mq_keys',
    #     'sink_driver': 'mplus.countly_main',
    #     'sink_conf': 'base',
    #     'parallelism': 1
    # },
    # {
    #     'name': 'mplus-click-mq',
    #     'module': 'click.main',
    #     'source_type': 'stream',
    #     'source_driver': 'redis',
    #     'source_conf': 'redis',
    #     'source_key': 'buffer->topic{mplus/click}mq_keys',
    #     'sink_type': 'click',
    #     'sink_driver': 'main',
    #     'sink_conf': 'clickhouse',
    #     'parallelism': 1
    # }
]
