# -*- coding:utf-8 -*-


BOOT = [
    {
        'name': 'test-traffic-mq',
        'module': 'test.normal_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{test}mq_keys',
        'sink_driver': 'test.main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'test-traffic-for-app-mq',
        'module': 'test.app_main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{test/app_traffic}mq_keys',
        'sink_driver': 'test.app_main',
        'sink_conf': 'base',
        'parallelism': 1
    },
    {
        'name': 'test-click-mq',
        'module': 'click.main',
        'source_driver': 'cache.redis_source',
        'source_conf': 'base',
        'source_topic': 'buffer->topic{test/click}mq_keys',
        'sink_driver': 'click.main',
        'sink_conf': 'base',
        'parallelism': 1
    }
]
