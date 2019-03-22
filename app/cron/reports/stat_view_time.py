# -- coding: UTF-8

from app.stream.store.database import ck_table, ClickhouseStore, mysql_table, MysqlStore
from app.utils import Func
from sqlalchemy import func
import time
import uuid


DetailsViewTime = ck_table.DetailsViewTime
UserStatViewTime = mysql_table.UserStatViewTime


class StatViewTimeReports:
    def __init__(self, date):
        self.date = date
        self.reports = []

    def get_report(self):
        """获取报表"""
        self.ck_session = ClickhouseStore().get_session()
        self.__report_combine(self.__total_view_time())

    def save_report(self):
        """保存报表"""
        self.mysql_session = MysqlStore().get_session('user')
        print('stat_view_time update start !!!!')
        print('{} records will be update !!!!!!'.format(len(self.reports)))

        piece_len = 50
        for piece in range(0, len(self.reports), piece_len):
            data_piece = self.reports[piece:piece + piece_len]

            for report in data_piece:
                exist = self.mysql_session.query(UserStatViewTime).\
                    filter(UserStatViewTime.plat == report.get('plat')).\
                    filter(UserStatViewTime.agent_type == report.get('agent_type')).\
                    filter(UserStatViewTime.channel == report.get('channel')).\
                    filter(UserStatViewTime.website == report.get('website')).\
                    filter(UserStatViewTime.stat_time == report.get('stat_time')).\
                    first()
                if exist is None:
                    stat_view_time = mysql_table.UserStatViewTime(
                        id=str(uuid.uuid4()),
                        plat=report.get('plat'),
                        agent_type=report.get('agent_type'),
                        channel=report.get('channel'),
                        website=report.get('website'),
                        total_view_time=report.get('total_view_time'),
                        pv_count=report.get('pv_count'),
                        uv_count=report.get('uv_count'),
                        stat_date=report.get('stat_date'),
                        stat_hour=report.get('stat_hour'),
                        stat_time=report.get('stat_time'),
                        add_time=Func.get_timestamp(),
                        update_time=Func.get_timestamp(),
                    )
                    self.mysql_session.add(stat_view_time)
                else:
                    exist.total_view_time = report.get('total_view_time')
                    exist.pv_count = report.get('pv_count')
                    exist.uv_count = report.get('uv_count')
                    exist.update_time = Func.get_timestamp()

            self.mysql_session.commit()
            print('finish {} records !!!!'.format(piece + piece_len))
        self.mysql_session.close()

    def __total_view_time(self):
        """获取访问报表"""
        res_list = self.ck_session.query(
            func.any(DetailsViewTime.plat),
            func.any(DetailsViewTime.agent_type),
            func.any(DetailsViewTime.website),
            func.any(DetailsViewTime.channel),
            func.uniqExact(DetailsViewTime.visit_id),
            func.count(DetailsViewTime.visit_id),
            func.sum(DetailsViewTime.view_time),
            func.toHour(DetailsViewTime.req_time),
            func.any(DetailsViewTime.req_date)
        ). \
            filter(DetailsViewTime.req_date == self.date). \
            group_by(
            DetailsViewTime.plat,
            DetailsViewTime.agent_type,
            DetailsViewTime.website,
            DetailsViewTime.channel,
            func.toHour(DetailsViewTime.req_time),
        ).all()
        return res_list

    def __report_combine(self, report):
        """组装报告"""

        for plat, agent_type, website, channel, uv_count, pv_count, total_view_time, stat_hour, stat_date in report:
            time_tuples = time.strptime('{} {}'.format(stat_date, stat_hour), '%Y-%m-%d %H')
            stat_date = int(time.strftime('%Y%m%d', time_tuples))
            stat_time = int(time.mktime(time_tuples))

            report_dict = {
                'plat': plat,
                'agent_type': agent_type,
                'channel': channel,
                'website': website,
                'total_view_time': total_view_time,
                'pv_count': pv_count,
                'uv_count': uv_count,
                'stat_date': int(stat_date),
                'stat_hour': stat_hour,
                'stat_time': stat_time,
            }
            self.reports.append(report_dict)
