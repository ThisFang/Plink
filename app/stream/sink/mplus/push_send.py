# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds, seconds
from app.stream.sink import base
from app.utils import logger
from app.common.request import CurlToAnalysis
from app.stream.store.database import ck_table, ClickhouseStore
import json


class PushSend:

    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())

        details = data_stream.flat_map(GetDetailsPushSend())

        # 报表推送
        details.flat_map(ReportsPushSendFlatMap())

        # 详情落地
        ck_insert = base.ClickHouseApply()
        ck_insert.set_table(table=ck_table.DetailsPushSend)
        details.key_by(PushSendKeyBy()). \
            time_window(seconds(5)). \
            apply(ck_insert)


class GetDetailsPushSend(FlatMapFunction):
    # start_time = Column(types.DateTime)  # 开始时间 10位时间戳
    # start_date = Column(types.Date)  # 开始时间 Y-m-d
    # start_hour = Column(types.Int)  # 开始时间 H
    # plat = Column(types.Int8)          # 平台 1/金业
    # agent_type = Column(types.Int8)    # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # push_type = Column(types.Int8)     # 推送类别   1/广播(notice)
    # msg_id = Column(types.String)      # 极光消息id
    # detail_id = Column(types.String)   # 消息detail_id
    # title = Column(types.String)    # 消息标题
    # content = Column(types.String)  # 消息内容
    # url = Column(types.String)      # 消息url
    # extra = Column(types.String)    # 额外信息
    def flatMap(self, push_send, collector):
        push_send = json.loads(push_send)
        primary_dict = {
            'start_time': push_send.get('start_time'),
            'start_date': push_send.get('start_date'),
            'start_hour': push_send.get('start_hour'),
            'plat': push_send.get('plat'),
            'agent_type': push_send.get('agent_type'),
            'push_category': push_send.get('push_category'),
            'push_type': push_send.get('push_type'),
            'msg_id': push_send.get('msg_id'),
            'detail_id': push_send.get('detail_id'),
            'title': push_send.get('title'),
            'content': push_send.get('content'),
            'url': push_send.get('url'),
            'extra': push_send.get('extra'),
        }
        collector.collect(json.dumps(primary_dict))


class PushSendKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        key = '{start_date}-{start_hour}-{plat}-{agent_type}-{push_type}-{msg_id}'.format(**value)
        return key


class ReportsPushSendFlatMap(FlatMapFunction):
    """
    stat_push_send报表写入
    """
    def flatMap(self, value, collector):
        value = json.loads(value)
        try:
            reports = PushSendReports(value).to_dict()
        except Exception as e:
            logger().error('REPORT INIT ERROR error:{} data:{}', e, value)
        else:
            self.update_stat_push_reports(reports)
            collector.collect(json.dumps(value))

    @staticmethod
    def update_stat_push_reports(reports):
        data = {
            'topic': 'push_send',
            'data': [
                reports
            ]
        }
        CurlToAnalysis('flow', '/push/send', 'PATCH', json=data).curl()


class PushSendReports:
    """
    针对单一stat_time,plat,agent_type,website
    聚合报表
    """
    def __init__(self, detail):

        self.ck_session = ClickhouseStore.get_session()

        self.detail = detail
        self._explode_exist()
        self.__data_dict()

    def _explode_exist(self):
        DetailsPushSend = ck_table.DetailsPushSend
        count = self.ck_session.query(DetailsPushSend).\
            filter(DetailsPushSend.plat == self.detail.get('plat')). \
            filter(DetailsPushSend.agent_type == self.detail.get('agent_type')). \
            filter(DetailsPushSend.push_type == self.detail.get('push_category')). \
            filter(DetailsPushSend.push_type == self.detail.get('push_type')). \
            filter(DetailsPushSend.msg_id == self.detail.get('msg_id')). \
            filter(DetailsPushSend.detail_id == self.detail.get('detail_id')). \
            count()
        self.ck_session.close()
        if count > 0:
            raise Exception('push_send already exist')

    def __data_dict(self):
        self._data_dict = {
            'start_time': self.detail.get('start_time'),
            'plat': self.detail.get('plat'),
            'agent_type': self.detail.get('agent_type'),
            'push_category': self.detail.get('push_category'),
            'push_type': self.detail.get('push_type'),
            'msg_id': self.detail.get('msg_id'),
            'detail_id': self.detail.get('detail_id'),
            'title': self.detail.get('title'),
            'content': self.detail.get('content'),
            'url': self.detail.get('url'),
        }

    def to_dict(self):
        self.ck_session.close()
        return self._data_dict

