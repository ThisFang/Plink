# -- coding: UTF-8

from app.utils.enums import *
from app.stream.store.database import ck_table, ClickhouseStore, mysql_table, MysqlStore
from app.utils import Func
from sqlalchemy import func
import time
import json
import uuid


DetailsDeposit = ck_table.DetailsDeposit
TradingStatDeposit = mysql_table.TradingStatDeposit


class StatDeposit:
    def __init__(self, date):
        # mysql
        self.mysql_session = MysqlStore().get_session('trading')

        # clickhouse
        self.ck_session = ClickhouseStore().get_session()

        # params
        self._date = date
        self.reports = []

    def get_report(self):
        # 获取“总计”（拼装成功、总的）
        # 获取“首入/非首入”（拼装成功、总的）
        self.__report_combine(
            self.get_total_all(),
            self.get_total_success()
        )

        self.__report_combine(
            self.get_isfirst_all(),
            self.get_isfirst_success()
        )

    def save_report(self):
        for report in self.reports:
            exist = self.mysql_session.query(TradingStatDeposit).\
                filter(TradingStatDeposit.plat == report.get('plat')).\
                filter(TradingStatDeposit.agent_type == report.get('agent_type')).\
                filter(TradingStatDeposit.is_first == report.get('is_first')).\
                filter(TradingStatDeposit.type == report.get('type')).\
                filter(TradingStatDeposit.stat_time == report.get('stat_time'))

            if exist.first() is None:
                stat_deposit = TradingStatDeposit(
                    id=str(uuid.uuid4()),
                    plat=report.get('plat'),
                    agent_type=report.get('agent_type'),
                    is_first=report.get('is_first'),
                    type=report.get('type'),
                    usd=report.get('usd'),
                    success_usd=report.get('success_usd'),
                    rmb=report.get('rmb'),
                    success_rmb=report.get('success_rmb'),
                    order_count=report.get('order_count'),
                    success_order_count=report.get('success_order_count'),
                    acc_count=report.get('acc_count'),
                    success_acc_count=report.get('success_acc_count'),
                    stat_date=report.get('stat_date'),
                    stat_hour=report.get('stat_hour'),
                    stat_time=report.get('stat_time'),
                    add_time=Func.get_timestamp(),
                    update_time=Func.get_timestamp(),
                )
                self.mysql_session.add(stat_deposit)
            else:
                exist.usd = report.get('usd'),
                exist.success_usd = report.get('success_usd'),
                exist.rmb = report.get('rmb'),
                exist.success_rmb = report.get('success_rmb'),
                exist.acc_count = report.get('acc_count')
                exist.success_acc_count = report.get('success_acc_count')
                exist.order_count = report.get('order_count')
                exist.success_order_count = report.get('success_order_count')
                exist.update_time = Func.get_timestamp()
                exist.update({TradingStatDeposit.usd: exist.usd,
                              TradingStatDeposit.success_usd: exist.success_usd,
                              TradingStatDeposit.rmb: exist.rmb,
                              TradingStatDeposit.success_rmb: exist.success_rmb,
                              TradingStatDeposit.acc_count: exist.acc_count,
                              TradingStatDeposit.success_acc_count: exist.success_acc_count,
                              TradingStatDeposit.order_count: exist.order_count,
                              TradingStatDeposit.success_order_count: exist.success_order_count},
                             synchronize_session=False)

        self.mysql_session.commit()
        self.mysql_session.close()

    def get_isfirst_base(self):
        _condition = self.ck_session.query(
            func.any(DetailsDeposit.type),
            func.any(DetailsDeposit.plat),
            func.any(DetailsDeposit.agent_type), 
            func.any(DetailsDeposit.is_first), 
            func.count(DetailsDeposit.order_no), 
            func.sum(DetailsDeposit.rmb),
            func.sum(DetailsDeposit.usd),
            func.uniqExact(DetailsDeposit.account)). \
            filter(DetailsDeposit.order_date <= self._date)
        return _condition

    def get_isfirst_success(self):
        tup_models = self.get_isfirst_base(). \
            filter(DetailsDeposit.status == PayStatusEnum.success). \
            group_by(
            DetailsDeposit.type,
            DetailsDeposit.plat,
            DetailsDeposit.agent_type,
            DetailsDeposit.is_first
        ).all()

        return tup_models

    def get_isfirst_all(self):
        tup_models = self.get_isfirst_base(). \
            group_by(
            DetailsDeposit.type,
            DetailsDeposit.plat,
            DetailsDeposit.agent_type,
            DetailsDeposit.is_first
        ).all()

        return tup_models

    def get_total_base(self):
        _condition = self.ck_session.query(
            func.any(DetailsDeposit.type),
            func.any(DetailsDeposit.plat),
            func.any(DetailsDeposit.agent_type), 
            '0',
            func.count(DetailsDeposit.order_no), 
            func.sum(DetailsDeposit.rmb),
            func.sum(DetailsDeposit.usd),
            func.uniqExact(DetailsDeposit.account)). \
            filter(DetailsDeposit.order_date <= self._date)
        return _condition

    def get_total_success(self):
        """ 出/入金成功总额、出/入金成功用户数、出/入金成功订单数 """
        tup_models = self.get_total_base(). \
            filter(DetailsDeposit.status == PayStatusEnum.success). \
            group_by(
            DetailsDeposit.type,
            DetailsDeposit.plat,
            DetailsDeposit.agent_type
        ).all()

        return tup_models

    def get_total_all(self):
        """ 出/入金总额、出/入金总用户数、出/入金总订单数 """
        tup_models = self.get_total_base(). \
            group_by(
            DetailsDeposit.type,
            DetailsDeposit.plat,
            DetailsDeposit.agent_type
        ).all()
        return tup_models

    def __report_combine(self, total_report, success_report):
        time_tuples = time.strptime(self._date, '%Y-%m-%d')
        stat_date = int(time.strftime('%Y%m%d', time_tuples))
        stat_hour = 0
        stat_time = int(time.mktime(time_tuples))

        success_report_dict = {}
        for report in success_report:
            type, plat, agent_type, is_first, order_count, rmb, usd, acc_count = report
            if type != DepositTypeEnum.withdraw or is_first != IsFirstEnum.no:
                success_report_key = '{}-{}-{}-{}'.format(
                    type, plat, agent_type, is_first
                )
                success_report_dict.setdefault(success_report_key, {
                    'order_count': order_count,
                    'usd': usd,
                    'rmb': rmb,
                    'acc_count': acc_count
                })
                
        for type, plat, agent_type, is_first, order_count, rmb, usd, acc_count in total_report:
            if type != DepositTypeEnum.withdraw or is_first != IsFirstEnum.no:
                report_key = '{}-{}-{}-{}'.format(
                    type, plat, agent_type, is_first
                )
                report_dict = {
                    'plat': plat,
                    'agent_type': agent_type,
                    'is_first': is_first,
                    'type': type,
                    'usd': usd,
                    'success_usd': success_report_dict.get(report_key, {}).get('usd', 0),
                    'rmb': rmb,
                    'success_rmb': success_report_dict.get(report_key, {}).get('rmb', 0),
                    'order_count': order_count,
                    'success_order_count': success_report_dict.get(report_key, {}).get('order_count', 0),
                    'acc_count': acc_count,
                    'success_acc_count': success_report_dict.get(report_key, {}).get('acc_count', 0),
                    'stat_date': int(stat_date),
                    'stat_hour': stat_hour,
                    'stat_time': stat_time,
                }
                self.reports.append(report_dict)
