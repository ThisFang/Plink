# -- coding: UTF-8

from app.stream.store.database import ck_table, ClickhouseStore, mysql_table, MysqlStore
from app.utils import Func
from sqlalchemy import func
import time
import json
import uuid


DetailsAggPayOrder = ck_table.DetailsAggPayOrder
TradingStatAggPay = mysql_table.TradingStatAggPay


class StatAggPayOrderReports:
    def __init__(self, date):
        self.date = date
        self.reports = []

    def get_report(self):
        """获取报表"""
        self.ck_session = ClickhouseStore().get_session()
        self.__report_combine(
            self.__total_all_agg_pay_order(),
            self.__total_success_agg_pay_order()
        )

        self.__report_combine(
            self.__is_first_all_agg_pay_order(),
            self.__is_first_success_agg_pay_order()
        )

    def save_report(self):
        """保存报表"""
        self.mysql_session = MysqlStore().get_session('trading')
        for report in self.reports:
            exist = self.mysql_session.query(TradingStatAggPay).\
                filter(TradingStatAggPay.plat == report.get('plat')).\
                filter(TradingStatAggPay.agent_type == report.get('agent_type')).\
                filter(TradingStatAggPay.pay_source == report.get('pay_source')).\
                filter(TradingStatAggPay.channel == report.get('channel')).\
                filter(TradingStatAggPay.is_first == report.get('is_first')).\
                filter(TradingStatAggPay.stat_time == report.get('stat_time')).\
                first()
            if exist is None:
                stat_agg_pay = mysql_table.TradingStatAggPay(
                    id=str(uuid.uuid4()),
                    plat=report.get('plat'),
                    agent_type=report.get('agent_type'),
                    pay_source=report.get('pay_source'),
                    channel=report.get('channel'),
                    is_first=report.get('is_first'),
                    acc_count=report.get('acc_count'),
                    success_acc_count=report.get('success_acc_count'),
                    order_count=report.get('order_count'),
                    success_order_count=report.get('success_order_count'),
                    stat_date=report.get('stat_date'),
                    stat_hour=report.get('stat_hour'),
                    stat_time=report.get('stat_time'),
                    add_time=Func.get_timestamp(),
                    update_time=Func.get_timestamp(),
                )
                self.mysql_session.add(stat_agg_pay)
            else:
                exist.acc_count = report.get('acc_count')
                exist.success_acc_count = report.get('success_acc_count')
                exist.order_count = report.get('order_count')
                exist.success_order_count = report.get('success_order_count')
                exist.update_time = Func.get_timestamp()

        self.mysql_session.commit()
        self.mysql_session.close()

    def __total_success_agg_pay_order(self):
        """所有成功订单(不按is_first分组)"""
        res_list = self.ck_session.query(
            func.any(DetailsAggPayOrder.plat),
            func.any(DetailsAggPayOrder.agent_type),
            func.any(DetailsAggPayOrder.pay_source),
            func.any(DetailsAggPayOrder.channel),
            '0',
            func.count(DetailsAggPayOrder.order_no),
            func.sum(DetailsAggPayOrder.usd),
            func.sum(DetailsAggPayOrder.rmb),
            func.uniqExact(DetailsAggPayOrder.account),
        ). \
            filter(DetailsAggPayOrder.status == 2). \
            filter(DetailsAggPayOrder.order_date <= self.date). \
            filter(DetailsAggPayOrder.agent_type > 0). \
            group_by(
            DetailsAggPayOrder.plat,
            DetailsAggPayOrder.agent_type,
            DetailsAggPayOrder.pay_source,
            DetailsAggPayOrder.channel,
        ).all()
        return res_list

    def __total_all_agg_pay_order(self):
        """所有订单(按is_first分组)"""
        res_list = self.ck_session.query(
            func.any(DetailsAggPayOrder.plat),
            func.any(DetailsAggPayOrder.agent_type),
            func.any(DetailsAggPayOrder.pay_source),
            func.any(DetailsAggPayOrder.channel),
            '0',
            func.count(DetailsAggPayOrder.order_no),
            func.sum(DetailsAggPayOrder.usd),
            func.sum(DetailsAggPayOrder.rmb),
            func.uniqExact(DetailsAggPayOrder.account),
        ). \
            filter(DetailsAggPayOrder.order_date <= self.date). \
            filter(DetailsAggPayOrder.agent_type > 0). \
            group_by(
            DetailsAggPayOrder.plat,
            DetailsAggPayOrder.agent_type,
            DetailsAggPayOrder.pay_source,
            DetailsAggPayOrder.channel,
        ).all()
        return res_list

    def __is_first_success_agg_pay_order(self):
        """所有成功订单(按is_first分组)"""
        res_list = self.ck_session.query(
            func.any(DetailsAggPayOrder.plat),
            func.any(DetailsAggPayOrder.agent_type),
            func.any(DetailsAggPayOrder.pay_source),
            func.any(DetailsAggPayOrder.channel),
            func.any(DetailsAggPayOrder.is_first),
            func.count(DetailsAggPayOrder.order_no),
            func.sum(DetailsAggPayOrder.usd),
            func.sum(DetailsAggPayOrder.rmb),
            func.uniqExact(DetailsAggPayOrder.account),
        ). \
            filter(DetailsAggPayOrder.status == 2). \
            filter(DetailsAggPayOrder.order_date <= self.date). \
            filter(DetailsAggPayOrder.agent_type > 0). \
            group_by(
            DetailsAggPayOrder.plat,
            DetailsAggPayOrder.agent_type,
            DetailsAggPayOrder.pay_source,
            DetailsAggPayOrder.channel,
            DetailsAggPayOrder.is_first,
        ).all()
        return res_list

    def __is_first_all_agg_pay_order(self):
        """所有订单(按is_first分组)"""
        res_list = self.ck_session.query(
            func.any(DetailsAggPayOrder.plat),
            func.any(DetailsAggPayOrder.agent_type),
            func.any(DetailsAggPayOrder.pay_source),
            func.any(DetailsAggPayOrder.channel),
            func.any(DetailsAggPayOrder.is_first),
            func.count(DetailsAggPayOrder.order_no),
            func.sum(DetailsAggPayOrder.usd),
            func.sum(DetailsAggPayOrder.rmb),
            func.uniqExact(DetailsAggPayOrder.account),
        ). \
            filter(DetailsAggPayOrder.order_date <= self.date). \
            filter(DetailsAggPayOrder.agent_type > 0). \
            group_by(
            DetailsAggPayOrder.plat,
            DetailsAggPayOrder.agent_type,
            DetailsAggPayOrder.pay_source,
            DetailsAggPayOrder.channel,
            DetailsAggPayOrder.is_first,
        ).all()
        return res_list

    def __report_combine(self, total_report, success_report):
        """组装报告"""
        time_tuples = time.strptime(self.date, '%Y-%m-%d')
        stat_date = int(time.strftime('%Y%m%d', time_tuples))
        stat_hour = 0
        stat_time = int(time.mktime(time_tuples))

        success_report_dict = {}
        for plat, agent_type, pay_source, channel, is_first, order_count, usd, rmb, acc_count in success_report:
            success_report_key = '{}-{}-{}-{}-{}'.format(
                plat, agent_type, pay_source, channel, is_first
            )
            success_report_dict.setdefault(success_report_key, {
                'order_count': order_count,
                'usd': usd,
                'rmb': rmb,
                'acc_count': acc_count
            })

        for plat, agent_type, pay_source, channel, is_first, order_count, usd, rmb, acc_count in total_report:
            report_key = '{}-{}-{}-{}-{}'.format(
                plat, agent_type, pay_source, channel, is_first
            )
            report_dict = {
                'plat': plat,
                'agent_type': agent_type,
                'pay_source': pay_source,
                'channel': channel,
                'is_first': is_first,
                'acc_count': acc_count,
                'success_acc_count': success_report_dict.get(report_key, {}).get('acc_count', 0),
                'order_count': order_count,
                'success_order_count': success_report_dict.get(report_key, {}).get('order_count', 0),
                'acc_usd': usd,
                'success_usd': success_report_dict.get(report_key, {}).get('usd', 0),
                'rmb': rmb,
                'success_rmb': success_report_dict.get(report_key, {}).get('rmb', 0),
                'stat_date': int(stat_date),
                'stat_hour': stat_hour,
                'stat_time': stat_time,
            }
            self.reports.append(report_dict)
