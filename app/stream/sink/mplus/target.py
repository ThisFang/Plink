# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
import copy
import json
from sqlalchemy import func
from sqlalchemy import not_, or_, desc
from app.utils import Func
from app.stream.sink import base
from app.utils import logger
from app.utils.enums import TargetEnum
from app.common.request import CurlToAnalysis
from app.stream.store.database import ck_table, ClickhouseStore
import time
from app.utils import LogName


class Target:
    @staticmethod
    def stream_end(data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        data_stream = data_stream.map(base.ToData())

        # 报表落地
        data_stream = data_stream.flat_map(ReportsTargetFlatMap())

        ck_insert = base.ClickHouseApply()
        ck_insert.set_table(table=ck_table.DetailsTarget)
        data_stream.flat_map(GetDetailsTarget()). \
            flat_map(ck_insert)


class GetDetailsTarget(FlatMapFunction):
    # req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    # plat = Column(types.Int8)  # 平台 1/金业
    # agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    # target_type = Column(types.Int8)  # 指标：1/访问(V) 2/咨询(NL) 3/模拟开户(D) 4/真实开户(N) 5/首次入金(A)
    # ip = Column(types.String)  # ip
    # visit_id = Column(types.String)  # 唯一vid
    # account = Column(types.String)  # 开户账户
    # mobile = Column(types.String)  # 开户手机
    # extra = Column(types.String)  # 额外信息
    def flatMap(self, target, collector):
        target = json.loads(target)
        primary_dict = {
            'req_time': target.get('req_time'),
            'req_date': target.get('req_date'),
            'req_hour': target.get('req_hour'),
            'plat': target.get('plat'),
            'agent_type': target.get('agent_type'),
            'target_type': target.get('target_type'),
            'ip': target.get('ip'),
            'visit_id': target.get('visit_id'),
            'account': target.get('account'),
            'mobile': target.get('mobile'),
            'source': target.get('source'),
            'medium': target.get('medium'),
            'extra': target.get('extra'),
        }
        collector.collect(json.dumps(primary_dict))


class ReportsTargetFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        value = json.loads(value)
        # 增加渠道价值报表推送
        self.update_stat_channel_value(value)

        reports_list = TargetReports(value, collector).to_dict()
        if reports_list:
            for reports_key, reports_data in reports_list:
                self.update_stat_target(reports_key, reports_data)

    @staticmethod
    def update_stat_target(reports_key, reports_data):
        if reports_data['count'] == 0:
            return
        data = {
            'topic': 'target',
            'data': [
                dict(reports_data, **reports_key)
            ]
        }
        CurlToAnalysis('flow', '/target', 'PATCH', json=data).curl()

    @staticmethod
    def update_stat_channel_value(target):
        target_type = target.get('target_type')
        if target_type is not TargetEnum.N:
            return

        req_time = target.get('req_time')
        stat_date = int(Func.get_date(req_time, '%Y%m%d'))
        stat_hour = int(Func.get_date(req_time, '%H'))
        stat_time = time.strptime('{} {}:00:00'.format(stat_date, stat_hour), '%Y%m%d %H:%M:%S')
        stat_time = int(time.mktime(stat_time))

        data = {
            'topic': 'channel_value',
            'data': [
                {
                    'plat': target.get('plat'),
                    'agent_type': target.get('agent_type'),
                    'channel': target.get('medium'),
                    'is_n': True,
                    'account': target.get('account'),
                    'stat_time': stat_time,
                    'stat_date': stat_date,
                    'stat_hour': 0,
                }
            ]
        }
        CurlToAnalysis('flow', '/channel_value', 'PATCH', json=data).curl()


class TargetReports:
    def __init__(self, details, collector):
        self.details = details
        self.ck_session = ClickhouseStore().get_session()
        # 判断是否NL,根据结果走不同分支
        if self.details.get('target_type') in [TargetEnum.D, TargetEnum.N, TargetEnum.A]:
            self._explode_exist('account')
        else:
            self._explode_exist('mobile')

        self.reports_list = []
        # 判断是否重复,重复直接退出,否则进入后续处理
        if not self.exist:
            self._a_complete_mobile()
            self._reports_easy()
            collector.collect(json.dumps(self.details))

    def _a_complete_mobile(self):
        target_type = self.details.get('target_type')
        DetailsTarget = ck_table.DetailsTarget
        if target_type == TargetEnum.N:
            mobile = self.details.get('mobile')
            update_dict = {'mobile': '\'{}\''.format(mobile)}
            filter_obj = self.ck_session.query(
                func.anyLast(DetailsTarget.mobile)). \
                filter(DetailsTarget.plat == self.details.get('plat')). \
                filter(DetailsTarget.account == self.details.get('account')). \
                filter(DetailsTarget.target_type == TargetEnum.A)
            ClickhouseStore.update(DetailsTarget, filter_obj, update_dict)
        elif target_type == TargetEnum.A:
            mobile = self.ck_session.query(
                func.anyLast(DetailsTarget.mobile)). \
                filter(DetailsTarget.plat == self.details.get('plat')). \
                filter(DetailsTarget.account == self.details.get('account')). \
                filter(DetailsTarget.target_type == TargetEnum.N). \
                first()
            if mobile:
                mobile, = mobile
                self.details['mobile'] = mobile

    def _reports_easy(self):
        self._get_all_target()
        reports = self._get_expect_all_route()
        for i in range(len(reports)):
            self.reports_list.append(reports[i])

    def _get_all_target(self):
        """获取所有路径"""
        DetailsTarget = ck_table.DetailsTarget
        target_type = self.details.get('target_type')

        if target_type == TargetEnum.V:
            self.all_route = [(TargetEnum.V, None, None, None, None)]
        else:
            self.all_route = self.ck_session.query(
                func.anyLast(DetailsTarget.target_type),
                func.anyLast(DetailsTarget.agent_type),
                func.anyLast(DetailsTarget.visit_id),
                func.anyLast(DetailsTarget.mobile),
                func.min(DetailsTarget.req_time)). \
                filter(DetailsTarget.plat == self.details.get('plat')). \
                filter(DetailsTarget.mobile == self.details.get('mobile')). \
                filter(DetailsTarget.target_type > TargetEnum.V). \
                group_by(DetailsTarget.target_type). \
                all()
        return self.all_route

    def _get_expect_all_route(self):
        """ 根据当前target_type获取预期所有route """
        reports = []

        source_route_tuple_list = [(target_type, agent_type, req_time) for target_type, agent_type, _, _, req_time in self.all_route]
        _le_route_tuple_list = list(filter(lambda x: x[0] < self.details.get('target_type'), source_route_tuple_list))
        _gt_route_tuple_list = list(filter(lambda x: x[0] > self.details.get('target_type'), source_route_tuple_list))

        source_route = [target_type for target_type, agent_type, req_time in source_route_tuple_list]
        _le_route = [target_type for target_type, agent_type, req_time in _le_route_tuple_list]
        _gt_route = [target_type for target_type, agent_type, req_time in _gt_route_tuple_list]

        route = copy.copy(_le_route)
        route.append(self.details.get('target_type'))

        reports.append(self.__reports_dict(
            self.details.get('req_time'),
            self.details.get('agent_type'),
            self.details.get('target_type'),
            self._get_expand_route(route),
            1
        ))

        source_route_max_target_type = max(source_route) if len(source_route) > 0 else 0
        if int(self.details.get('target_type')) < int(source_route_max_target_type):
            for target_type, fix_agent_type, req_time in _gt_route_tuple_list:
                route.append(target_type)
                _le_route.append(target_type)
                add_route = sorted(route)
                sub_route = sorted(_le_route)
                try:
                    fix_time = int(time.mktime(req_time.timetuple()))
                except Exception:
                    fix_time = 0
                reports.append(self.__reports_dict(
                    fix_time,
                    fix_agent_type,
                    max(add_route),
                    self._get_expand_route(add_route),
                    1
                ))
                reports.append(self.__reports_dict(
                    fix_time,
                    fix_agent_type,
                    max(sub_route),
                    self._get_expand_route(sub_route),
                    -1
                ))

        return reports

    def _explode_exist(self, column):
        """判断target是否存在，存在就不更新报表"""
        DetailsTarget = ck_table.DetailsTarget

        exist = self.ck_session.query(
            func.anyLast(DetailsTarget.target_type),
            func.anyLast(DetailsTarget.mobile),
            func.anyLast(DetailsTarget.req_time)). \
            group_by(DetailsTarget.target_type). \
            filter(getattr(DetailsTarget, column) == self.details.get(column)). \
            filter(DetailsTarget.plat == self.details.get('plat')). \
            filter(DetailsTarget.target_type == self.details.get('target_type')). \
            filter(getattr(DetailsTarget, column).isnot(None)). \
            filter(not_(getattr(DetailsTarget, column) == '')). \
            count()

        # 访问的去重数据量太大，日志无参考价值
        if exist and self.details.get('target_type') != TargetEnum.V:
            logger(LogName.TARGET).info('target exist !{}'.format(self.details))

        if exist > 0:
            self.exist = True
        else:
            self.exist = False

    @staticmethod
    def _get_expand_route(route_list):

        # 默认补齐访问数据
        if TargetEnum.V not in route_list:
            route_list.insert(0, TargetEnum.V)

        # 如果只有访问和入金即1,5则默认加上真实开户
        if (TargetEnum.A in route_list) and len(route_list) == 2:
            route_list = [TargetEnum.V, TargetEnum.N, TargetEnum.A]

        route_list = [str(route) for route in route_list]
        expand_route = '-'.join(route_list)
        return expand_route

    def __reports_dict(self, req_time, agent_type, target_type, expand_route, count):
        stat_date = int(Func.get_date(req_time, '%Y%m%d'))
        stat_hour = int(Func.get_date(req_time, '%H'))
        stat_time = time.strptime('{} {}:00:00'.format(stat_date, stat_hour), '%Y%m%d %H:%M:%S')
        stat_time = int(time.mktime(stat_time))

        self.plat = self.details.get('plat')

        key_dict = {
            'stat_time': stat_time,
            'stat_date': stat_date,
            'stat_hour': stat_hour,
            'plat': self.plat,
            'agent_type': agent_type,
            'expand_route': expand_route,
            'target_type': target_type,
        }
        data_dict = {
            'count': count
        }
        return key_dict, data_dict

    def to_dict(self):
        self.ck_session.close()
        return self.reports_list
