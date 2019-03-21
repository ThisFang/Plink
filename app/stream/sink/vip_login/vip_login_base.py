# -- coding: UTF-8 

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from app.common.request import CurlToAnalysis
import json
import time


class VipLoginReportsFlatMap(FlatMapFunction):
    def flatMap(self, report, collector):
        report = json.loads(report)
        data = {
            'data': [
                get_report(report)
            ]
        }
        CurlToAnalysis('flow', '/vip_login', 'PATCH', json=data).curl()


def get_report(report):
    stat_time = time.strptime('{stat_date} {stat_hour}:00:00'.format(**report), '%Y-%m-%d %H:%M:%S')
    stat_time = int(time.mktime(stat_time))

    reports = {
        'stat_time': stat_time,
        'stat_date': report.get('stat_date'),
        'stat_hour': report.get('stat_hour'),
        'plat': report.get('plat'),
        'agent_type': report.get('agent_type'),

        'startup_count_pv': report.get('startup_count_pv'),
        'startup_count_uv': report.get('startup_count_uv'),

        'login_count_pv': report.get('login_count_pv'),
        'login_count_uv': report.get('login_count_uv'),

        'vip_count_pv': report.get('vip_count_pv'),
        'vip_count_uv': report.get('vip_count_uv'),

        'vip_login_count_pv': report.get('vip_login_count_pv'),
        'vip_login_count_uv': report.get('vip_login_count_uv'),

        'enjoy_count_pv': report.get('enjoy_count_pv'),
        'enjoy_count_uv': report.get('enjoy_count_uv'),
    }
    return reports
