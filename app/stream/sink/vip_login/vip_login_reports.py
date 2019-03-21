# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds, seconds
from app.stream.sink.vip_login import vip_login_base
import json


class VipLoginReports:
    @staticmethod
    def stream_end(data_stream):
        data_stream = data_stream.map(ToData())

        # 报表落地
        reports = data_stream.key_by(ReportsKeyBy()). \
            time_window(seconds(30)). \
            reduce(VipLoginReportsReduce()). \
            map(TupleToDict()). \
            flat_map(vip_login_base.VipLoginReportsFlatMap())


class VipLoginReportsReduce(ReduceFunction):
    def reduce(self, input1, input2):
        stat_time1, stat_date1, stat_hour1, plat1, agent_type1, \
            startup_count_pv1, startup_count_uv1, login_count_pv1, login_count_uv1, \
            vip_count_pv1, vip_count_uv1, vip_login_count_pv1, vip_login_count_uv1, enjoy_count_pv1, enjoy_count_uv1 \
            = input1
        stat_time2, stat_date2, stat_hour2, plat2, agent_type2, \
            startup_count_pv2, startup_count_uv2, login_count_pv2, login_count_uv2, \
            vip_count_pv2, vip_count_uv2, vip_login_count_pv2, vip_login_count_uv2, enjoy_count_pv2, enjoy_count_uv2 \
            = input2

        reports = (
            stat_time1,
            stat_date1,
            stat_hour1,
            plat1,
            agent_type1,
            startup_count_pv1 + startup_count_pv2,
            startup_count_uv1 + startup_count_uv2,
            login_count_pv1 + login_count_pv2,
            login_count_uv1 + login_count_uv2,
            vip_count_pv1 + vip_count_pv2,
            vip_count_uv1 + vip_count_uv2,
            vip_login_count_pv1 + vip_login_count_pv2,
            vip_login_count_uv1 + vip_login_count_uv2,
            enjoy_count_pv1 + enjoy_count_pv2,
            enjoy_count_uv1 + enjoy_count_uv2,
        )
        return reports


class ToData(MapFunction):
    def map(self, value):
        topic, data = value
        data = json.loads(data)
        data_tuple = (
            data.get('stat_time'),
            data.get('stat_date'),
            data.get('stat_hour'),
            data.get('plat'),
            data.get('agent_type'),
            data.get('startup_count_pv'),
            data.get('startup_count_uv'),
            data.get('login_count_pv'),
            data.get('login_count_uv'),
            data.get('vip_count_pv'),
            data.get('vip_count_uv'),
            data.get('vip_login_count_pv'),
            data.get('vip_login_count_uv'),
            data.get('enjoy_count_pv'),
            data.get('enjoy_count_uv'),
        )
        return data_tuple


class TupleToDict(MapFunction):
    def map(self, value):
        stat_time, stat_date, stat_hour, plat, agent_type, \
            startup_count_pv, startup_count_uv, login_count_pv, login_count_uv, \
            vip_count_pv, vip_count_uv, vip_login_count_pv, vip_login_count_uv, enjoy_count_pv, enjoy_count_uv \
            = value
        data_dict = {
            'stat_time': stat_time,
            'stat_date': stat_date,
            'stat_hour': stat_hour,
            'plat': plat,
            'agent_type': agent_type,
            'startup_count_pv': startup_count_pv,
            'startup_count_uv': startup_count_uv,
            'login_count_pv': login_count_pv,
            'login_count_uv': login_count_uv,
            'vip_count_pv': vip_count_pv,
            'vip_count_uv': vip_count_uv,
            'vip_login_count_pv': vip_login_count_pv,
            'vip_login_count_uv': vip_login_count_uv,
            'enjoy_count_pv': enjoy_count_pv,
            'enjoy_count_uv': enjoy_count_uv,
        }
        return json.dumps(data_dict)


class ReportsKeyBy(KeySelector):
    def getKey(self, value):
        stat_time, stat_date, stat_hour, plat, agent_type, \
            startup_count_pv, startup_count_uv, login_count_pv, login_count_uv, \
            vip_count_pv, vip_count_uv, vip_login_count_pv, vip_login_count_uv, enjoy_count_pv, enjoy_count_uv \
            = value
        key = '{}-{}-{}-{}'.format(stat_date, stat_hour, plat, agent_type)
        return key
