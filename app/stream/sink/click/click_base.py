# -- coding: UTF-8 

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.api.java.functions import KeySelector
import json


class GetDetailsClick(FlatMapFunction):
    # plat = Column(types.Int8)               # 平台 1/金业
    # agent_type = Column(types.Int8)         # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # click_category = Column(types.String)   # 事件分类
    # click_action = Column(types.String)     # 时间动作
    # click_value = Column(types.String)      # 事件值
    # visit_id = Column(types.String)       # 用户id
    # ip = Column(types.String)             # ip
    # req_time = Column(types.DateTime)     # 事件时间 10位时间戳
    # req_date = Column(types.Date)         # 事件时间 Y-m-d
    # req_hour = Column(types.Int)          # 事件时间 H
    def flatMap(self, click, collector):
        click = json.loads(click)
        primary_dict = {
            'req_time': click.get('req_time'),
            'req_date': click.get('req_date'),
            'req_hour': click.get('req_hour'),
            'plat': click.get('plat'),
            'agent_type': click.get('agent_type'),
            'click_category': click.get('click_category'),
            'click_action': click.get('click_action'),
            'click_value': click.get('click_value'),
            'ip': click.get('ip'),
            'visit_id': click.get('visit_id'),
        }
        collector.collect(json.dumps(primary_dict))


class ReportsClickKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        value['click_category'] = value['click_category'].encode('utf-8')
        value['click_action'] = value['click_action'].encode('utf-8')
        key = '{req_date}-{req_hour}-{plat}-{agent_type}-{click_category}-{click_action}'.format(**value)
        return key


class ReportsClickWithValueKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        value['click_category'] = value['click_category'].encode('utf-8')
        value['click_action'] = value['click_action'].encode('utf-8')
        value['click_value'] = value['click_value'].encode('utf-8')
        key = '{req_date}-{req_hour}-{plat}-{agent_type}-{click_category}-{click_action}-{click_value}'.format(**value)
        return key
