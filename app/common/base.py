# -- coding: UTF-8 


class SuperBase(object):
    def __init__(self, boot_conf):
        self.boot_conf = boot_conf


ALLOW_TOPIC_DICT = {
    'traffic': 'mplus/app_traffic',
    'target': 'mplus',
    'push_app': 'mplus',
    'push_send': 'mplus',

    'agg_pay': 'mplus/deposit',
    'deposit': 'mplus/deposit',

    'click': 'mplus/click',
    'deposit_route': 'mplus/click',
    'pay_route': 'mplus/click',
    'deposit_funnel': 'mplus/click',
    'register_route': 'mplus/click',
    'register_funnel': 'mplus/click',
    'advice': 'mplus/click',
    'news': 'mplus/click',

    'app_open': 'mplus/login',
    'app_vip_open': 'mplus/login',
    'vip_open': 'mplus/login',
    'vip': 'mplus/login',
    'login': 'mplus/login',
}
ALLOW_TOPIC = list(ALLOW_TOPIC_DICT.keys())


WEBSITE_TO_STAT_REQUEST = [
        'APP5个模块-首页',
        'APP5个模块-行情',
        'APP5个模块-交易',
        'APP5个模块-我的',
        '行情图表页-日K线',
        '行情图表页-风向标',
        '行情图表页-分时',
        '行情图表页-M1',
        '行情图表页-M5',
        '行情图表页-M15',
        '行情图表页-M30',
        '行情图表页-H1',
        '行情图表页-H4',
        '行情图表页-W1',
        '行情图表页-WN',
]

WEBSITE_NOT_IN_CK = [
        '行情图表页-日K线',
        '行情图表页-风向标',
        '行情图表页-分时',
        '行情图表页-M1',
        '行情图表页-M5',
        '行情图表页-M15',
        '行情图表页-M30',
        '行情图表页-H1',
        '行情图表页-H4',
        '行情图表页-W1',
        '行情图表页-WN',
]
