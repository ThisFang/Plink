# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds, seconds
import json
import time
import datetime
from sqlalchemy import func
from app.stream.sink import base
from app.stream.store.database import ck_table, ClickhouseStore
from app.common.view_time_analysis import ViewTimeAnalysis
from app.common.uv_calc import UvCalc
from app.common.base import WEBSITE_NOT_IN_CK, WEBSITE_TO_STAT_REQUEST
from app.utils import logger, Func, LogName
from app.common.request import CurlToAnalysis


class Traffic:

    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())

        ck_insert = base.ClickHouseApply()

        # 报表落地
        data_stream.key_by(ReportsTrafficKeyBy()). \
            time_window(seconds(30)). \
            apply(ReportsTrafficApply())

        # 详情落地
        ck_insert.set_table(table=ck_table.DetailsTraffic)
        data_stream.flat_map(GetDetailsTraffic()). \
            key_by(base.KeyBy()). \
            time_window(seconds(10)). \
            apply(ck_insert)

        # 收集visit_id
        ck_insert.set_table(table=ck_table.TrafficVisitId)
        data_stream.flat_map(GetTrafficVisitId()). \
            key_by(base.KeyBy()). \
            time_window(seconds(10)). \
            apply(ck_insert)

        # 详情落地（访问时长）
        ck_insert.set_table(table=ck_table.DetailsViewTime)
        data_stream.flat_map(GetDetailsViewTime()). \
            key_by(VTTrafficKeyBy()). \
            time_window(seconds(31)). \
            apply(ck_insert)


class GetTrafficVisitId(FlatMapFunction):
    # req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    # plat = Column(types.Int8)  # 平台 1/金业
    # agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # visit_id = Column(types.String)  # 唯一vid
    def flatMap(self, traffic, collector):
        traffic = json.loads(traffic)
        primary_dict = {
            'visit_id': traffic.get('visit_id'),
            'plat': traffic.get('plat'),
            'agent_type': traffic.get('agent_type'),
            'req_time': traffic.get('req_time'),
            'req_date': traffic.get('req_date'),
        }
        collector.collect(json.dumps(primary_dict))


class GetDetailsTraffic(FlatMapFunction):
    # req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    # plat = Column(types.Int8)  # 平台 1/金业
    # agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # website = Column(types.String)  # 网页标题
    # url = Column(types.String)  # url
    # host = Column(types.String)  # 域名
    # ref_url = Column(types.String)  # 上级url
    # ip = Column(types.String)  # ip
    # visit_id = Column(types.String)  # 唯一vid
    # loading_time = Column(types.Int8)  # 网页打开完成时间
    # req_status = Column(types.Int8)  # 打开状态 1/成功 2/失败
    # extra = Column(types.String)  # 额外信息
    # market_source = Column(types.String)  # 来源
    # market_medium = Column(types.String)  # 媒介
    # market_campaign = Column(types.String)  # 系列
    # market_content = Column(types.String)  # 内容
    # market_term = Column(types.String)  # 关键字
    def flatMap(self, traffic, collector):
        
        traffic = json.loads(traffic)
        primary_dict = {
            'req_time': traffic.get('req_time'),
            'req_date': traffic.get('req_date'),
            'req_hour': traffic.get('req_hour'),
            'plat': traffic.get('plat'),
            'agent_type': traffic.get('agent_type'),
            'website': traffic.get('website'),
            'url': traffic.get('url'),
            'host': traffic.get('host'),
            'ref_url': traffic.get('ref_url'),
            'ip': traffic.get('ip'),
            'visit_id': traffic.get('visit_id'),
            'loading_time': traffic.get('loading_time'),
            'req_status': traffic.get('req_status'),
            'extra': traffic.get('extra'),
            'market_source': traffic.get('market').get('source', ''),
            'market_medium': traffic.get('market').get('medium', ''),
            'market_campaign': traffic.get('market').get('campaign', ''),
            'market_content': traffic.get('market').get('content', ''),
            'market_term': traffic.get('market').get('term', ''),
        }

        if traffic.get('website').encode('utf-8') not in WEBSITE_NOT_IN_CK:
            collector.collect(json.dumps(primary_dict))


class VTTrafficKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        value['website'] = value['website'].encode('utf-8')
        value['channel'] = value['channel'].encode('utf-8')
        key = '{req_date}-{plat}-{agent_type}-{website}-{channel}'.format(**value)
        return key


class GetDetailsViewTime(FlatMapFunction):
    def flatMap(self, value, collector):
        traffic = json.loads(value)
        plat = traffic.get('plat')
        agent_type = traffic.get('agent_type')
        channel = traffic.get('market').get('medium')
        visit_id = traffic.get('visit_id')
        website = traffic.get('website')
        req_time = traffic.get('req_time')
        if website.encode('utf-8') in WEBSITE_NOT_IN_CK:
            return
        try:
            vt_dict = ViewTimeAnalysis().analysis(plat, agent_type, channel, visit_id, website, req_time)
        except Exception as e:
            logger(LogName.TRAFFIC).error('{}, {}'.format(e, value))  # 调试时使用
        else:
            if vt_dict is not None:
                primary_dict = {
                    'visit_id': vt_dict.get('visit_id'),
                    'plat': vt_dict.get('plat'),
                    'agent_type': vt_dict.get('agent_type'),
                    'channel': vt_dict.get('channel'),
                    'website': vt_dict.get('website'),
                    'req_time': vt_dict.get('sign'),
                    'req_date': Func.get_date(vt_dict.get('sign'), '%Y-%m-%d'),
                    'view_time': vt_dict.get('view_time')
                }
                collector.collect(json.dumps(primary_dict))


class ReportsTrafficKeyBy(KeySelector):
    def getKey(self, value):
        value = json.loads(value)
        value['website'] = value['website'].encode('utf-8')
        key = '{req_date}-{req_hour}-{plat}-{agent_type}-{website}'.format(**value)
        return key


class ReportsTrafficApply(WindowFunction):
    def apply(self, key, window, values, collector):
        for value in values:
            collector.collect(value)
        values = [json.loads(value) for value in values]
        try:
            reports_key, reports_data = TrafficReports(values).to_dict()
        except Exception as e:
            logger(LogName.TRAFFIC).error('REPORT INIT ERROR error:{} data:{}'.format(e, values))
        else:
            data = {
                'topic': 'traffic',
                'data': [
                    dict(reports_data, **reports_key)
                ]
            }
            CurlToAnalysis('flow', '/traffic', 'PATCH', json=data).curl()


class TrafficReports:
    def __init__(self, details_list):

        self.ck_session = ClickhouseStore().get_session()

        self.details_list = details_list
        self.success_details_list = list(filter(lambda x: x.get('req_status') == 1, self.details_list))
        self.__key_dict()
        self._explode_req_count()
        self._explode_pv_count()
        self._explode_uv_count()
        self._explode_ip_count()
        self._explode_loading_time()
        self.__data_dict()

    def _explode_pv_count(self):
        if self.website.encode('utf-8') in WEBSITE_NOT_IN_CK:
            self.pv_count = 0
            return
        count = len(self.success_details_list)
        self.pv_count = count

    def _explode_uv_count(self):
        if self.website.encode('utf-8') in WEBSITE_NOT_IN_CK:
            self.uv_count = 0
            self.new_count = 0
            return
        visit_id_list = [value.get('visit_id') for value in self.success_details_list]
        visit_id_list = list(set(visit_id_list))

        date = time.strptime(str(self.stat_date), '%Y-%m-%d')
        date = time.strftime('%Y-%m-%d', date)

        uv_id_list = UvCalc().traffic_uv(
            self.plat,
            self.agent_type,
            date,
            self.website,
            visit_id_list
        )

        uv_count = len(uv_id_list)

        if not uv_count:
            old_count = uv_count
        else:
            act_date = datetime.datetime.strptime(self.stat_date, '%Y-%m-%d')
            act_date = act_date - datetime.timedelta(days=30)
            # act_date = str(act_date.strftime('%Y-%m-%d'))

            TrafficVisitId = ck_table.TrafficVisitId
            old_count, = self.ck_session.query(func.uniq(TrafficVisitId.visit_id)). \
                filter(TrafficVisitId.visit_id.in_(uv_id_list)). \
                filter(TrafficVisitId.plat == self.plat). \
                filter(TrafficVisitId.agent_type == self.agent_type). \
                filter(TrafficVisitId.req_date >= act_date). \
                first()

        self.uv_count = uv_count
        self.new_count = uv_count - old_count

    def _explode_ip_count(self):
        # 预留算法，考虑性能暂时舍弃
        self.ip_count = 0

    def _explode_loading_time(self):
        self.loading_time = sum([value.get('loading_time') for value in self.details_list])

    def _explode_req_count(self):
        self.req_total_count = len(self.details_list)
        self.req_success_count = len(self.success_details_list)
        self.req_fail_count = self.req_total_count-self.req_success_count

    def __key_dict(self):
        self.stat_date = self.details_list[0].get('req_date')
        self.stat_hour = self.details_list[0].get('req_hour')
        stat_time = time.strptime('{} {}:00:00'.format(self.stat_date, self.stat_hour), '%Y-%m-%d %H:%M:%S')
        self.stat_time = int(time.mktime(stat_time))
        self.plat = self.details_list[0].get('plat')
        self.agent_type = self.details_list[0].get('agent_type')
        self.website = self.details_list[0].get('website')

        self._key_dict = {
            'stat_time': self.stat_time,
            'stat_date': self.stat_date,
            'stat_hour': self.stat_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'website': self.website,
        }

    def __data_dict(self):
        self._data_dict = {
            'pv_count': self.pv_count,
            'uv_count': self.uv_count,
            'ip_count': self.ip_count,
            'new_count': self.new_count,
            'loading_time': self.loading_time,
            'req_total_count': self.req_total_count,
            'req_fail_count': self.req_fail_count,
            'req_success_count': self.req_success_count,
        }

    def to_dict(self):
        self.ck_session.close()
        return self._key_dict, self._data_dict


