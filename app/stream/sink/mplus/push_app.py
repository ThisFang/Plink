# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds, seconds
import time
from app.stream.sink import base
from app.utils import logger
from app.common.request import CurlToAnalysis
import json
from app.common.uv_calc import UvCalc
from app.stream.store.database import ck_table


class PushApp:

    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())

        data_stream = data_stream.flat_map(GetDetailsPushApp())

        window_stream = data_stream.key_by(PushAppKeyBy()). \
            time_window(seconds(5))

        # 详情落地
        # ck_insert = base.ClickHouseApply()
        # ck_insert.set_table(table=ck_table.DetailsPushApp)
        # window_stream.apply(ck_insert)

        # 报表推送
        window_stream.apply(ReportsPushAppApply())


class GetDetailsPushApp(FlatMapFunction):
    # req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    # req_date = Column(types.Date)  # 请求时间 Y-m-d
    # req_hour = Column(types.Int)  # 请求时间 H
    # plat = Column(types.Int8)  # 平台 1/金业
    # agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # push_type = Column(types.Int8)  # 推送类别   1/广播(notice)
    # ip = Column(types.String)  # ip
    # visit_id = Column(types.String)  # 唯一vid
    # detail_id = Column(types.String)  # 消息detail_id
    # oper_type = Column(types.Int8)  # 操作类别   1/打开数
    # extra = Column(types.String)  # 额外信息
    def flatMap(self, push_app, collector):
        push_app = json.loads(push_app)
        primary_dict = {
            'req_time': push_app.get('req_time'),
            'req_date': push_app.get('req_date'),
            'req_hour': push_app.get('req_hour'),
            'plat': push_app.get('plat'),
            'agent_type': push_app.get('agent_type'),
            'push_type': push_app.get('push_type'),
            'oper_type': push_app.get('oper_type'),
            'ip': push_app.get('ip'),
            'visit_id': push_app.get('visit_id'),
            'detail_id': push_app.get('detail_id'),
            'extra': push_app.get('extra'),
        }
        collector.collect(json.dumps(primary_dict))


class PushAppKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        key = '{req_date}-{req_hour}-{plat}-{agent_type}-{push_type}-{oper_type}-{detail_id}'.format(**value)
        return key


class ReportsPushAppApply(WindowFunction):
    def apply(self, key, window, values, collector):
        for value in values:
            collector.collect(value)

        values = [json.loads(value) for value in values]
        try:
            reports_key, reports_data = PushAppReports(values).to_dict()
        except Exception as e:
            logger().error('REPORT INIT ERROR error:{} data:{}', e, values)
        else:
            self.update_stat_push_details(reports_key, reports_data)

    @staticmethod
    def update_stat_push_details(reports_key, reports_data):
        if reports_data.get('count') == 0:
            return
        reports = {
            'stat_date': reports_key.get('stat_date'),
            'stat_hour': reports_key.get('stat_hour'),
            'stat_time': reports_key.get('stat_time'),
            'agent_type': reports_key.get('agent_type'),
            'oper_type': reports_key.get('oper_type'),
            'push_type': reports_key.get('push_type'),
            'plat': reports_key.get('plat'),
            'detail_id': reports_key.get('detail_id'),
            'count': reports_data.get('count')
        }

        data = {
            'topic': 'push_app',
            'data': [
                reports
            ]
        }
        CurlToAnalysis('flow', '/push/app', 'PATCH', json=data).curl()


class PushAppReports:
    def __init__(self, details_list):
        self.details_list = details_list
        self.__key_dict()
        self._explode_count()
        self.__data_dict()

    def _explode_count(self):
        visit_id_list = [value.get('visit_id') for value in self.details_list]
        visit_id_list = list(set(visit_id_list))

        uv_list = UvCalc().push_app_uv(
            self.plat,
            self.agent_type,
            self.stat_date,
            self.detail_id,
            visit_id_list
        )

        self.count = len(uv_list)

    def __key_dict(self):
        self.stat_date = self.details_list[0].get('req_date')
        self.stat_hour = self.details_list[0].get('req_hour')
        stat_time = time.strptime('{} {}0000'.format(self.stat_date, self.stat_hour), '%Y-%m-%d %H%M%S')
        self.stat_time = int(time.mktime(stat_time))
        self.plat = self.details_list[0].get('plat')
        self.agent_type = self.details_list[0].get('agent_type')
        self.push_type = self.details_list[0].get('push_type')
        self.oper_type = self.details_list[0].get('oper_type')
        self.detail_id = self.details_list[0].get('detail_id')

        self._key_dict = {
            'stat_time': self.stat_time,
            'stat_date': self.stat_date,
            'stat_hour': self.stat_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'push_type': self.push_type,
            'oper_type': self.oper_type,
            'detail_id': self.detail_id,
        }

    def __data_dict(self):
        self._data_dict = {
            'count': self.count,
        }

    def to_dict(self):
        return self._key_dict, self._data_dict
