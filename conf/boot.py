# -*- coding:utf-8 -*-


BOOT = [
    {
        'name': 'mplus-traffic-for-app-mq',
        'module': 'test.test_operator',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{test}mq_keys',
        'sink_driver': 'test.test_sink',
        'sink_conf': 'base',
        'parallelism': 1
    },
    # {
    #     'name': 'mplus-countly-to-gateway-mq',
    #     'module': 'mplus.countly_to_gateway',
    #     'source_type': 'stream',
    #     'source_driver': 'redis',
    #     'source_conf': 'redis',
    #     'source_key': 'buffer->topic{app/i}mq_keys',
    #     'sink_type': 'mplus',
    #     'sink_driver': 'countly_main',
    #     'sink_conf': 'clickhouse',
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
