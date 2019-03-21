# -- coding: UTF-8

from org.apache.flink.streaming.api.windowing.time.Time import milliseconds
from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from sqlalchemy import func
from app.utils import Func, logger, LogName
from app.stream.sink import base
from app.stream.sink.click import click_base
from app.stream.store.database import ck_table, ClickhouseStore
from app.common.request import CurlToAnalysis
from app.common.uv_calc import UvCalc
import time
import json


class DepositFunnel:
    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())

        ck_insert = base.ClickHouseApply()

        # 报表落地
        reports = data_stream.key_by(click_base.ReportsClickKeyBy()). \
            time_window(milliseconds(5000)). \
            apply(ReportsDepositFunnelApply())

        # 详情落地
        ck_insert.set_table(table=ck_table.DetailsClick)
        reports.flat_map(click_base.GetDetailsClick()). \
            key_by(base.KeyBy()). \
            time_window(milliseconds(1000)). \
            apply(ck_insert)


class ReportsDepositFunnelApply(WindowFunction):
    def apply(self, key, window, values, collector):
        for value in values:
            collector.collect(value)

        values = [json.loads(value) for value in values]
        try:
            reports_key, reports_data = DepositFunnelReports(values).to_dict()
        except Exception as e:
            logger(LogName.CLICK).error('REPORT INIT ERROR error:{} data:{}', e, values)
        else:
            data = {
                'data': [
                    dict(reports_data, **reports_key)
                ]
            }
            CurlToAnalysis('flow', '/click/deposit_funnel', 'PATCH', json=data).curl()


class DepositFunnelReports:
    def __init__(self, details_list):
        self.details_list = details_list
        self.__key_dict()
        self._explode_pv_count()
        self._explode_uv_count()
        self.__data_dict()

    def _explode_pv_count(self):
        count = len(self.details_list)
        self.pv_count = count

    def _explode_uv_count(self):
        column = 'click_value'
        column_list = [value.get(column) for value in self.details_list]
        column_list = list(set(column_list))

        date = time.strptime(str(self.stat_date), '%Y-%m-%d')
        date = time.strftime('%Y-%m-%d', date)

        uv_list = UvCalc().click_uv(
            self.plat,
            self.agent_type,
            date,
            self.click_category,
            self.click_action,
            column_list
        )

        self.uv_count = len(uv_list)

    def __key_dict(self):
        self.stat_date = self.details_list[0].get('req_date')
        self.stat_hour = self.details_list[0].get('req_hour')
        stat_time = time.strptime('{} {}:00:00'.format(self.stat_date, self.stat_hour), '%Y-%m-%d %H:%M:%S')
        self.stat_time = int(time.mktime(stat_time))
        self.plat = self.details_list[0].get('plat')
        self.agent_type = self.details_list[0].get('agent_type')
        self.click_category = self.details_list[0].get('click_category')
        self.click_action = self.details_list[0].get('click_action')
        self.pay_source = self.details_list[0].get('pay_source')
        self.type = self.details_list[0].get('type')

        self._key_dict = {
            'stat_time': self.stat_time,
            'stat_date': self.stat_date,
            'stat_hour': self.stat_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'click_category': self.click_category,
            'click_action': self.click_action,
            'pay_source': self.pay_source,
            'type': self.type,
        }

    def __data_dict(self):
        self._data_dict = {
            'pv_count': self.pv_count,
            'uv_count': self.uv_count,
        }

    def to_dict(self):
        return self._key_dict, self._data_dict
