# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger


class AppOpen:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(AppOpenSave())


class AppOpenSave(FlatMapFunction):
    def flatMap(self, stream, collector):
        topic, ip, data_list = stream
        data_list = json.loads(data_list)
        for item in data_list:
            try:
                app_open_obj = AppOpenArgs(item)
                app_open_obj = app_open_obj.to_dict()
            except Exception as e:
                logger().error('{}, {}, {}'.format(topic, e, item))
            else:
                collector.collect(('vip_login_reports', json.dumps(app_open_obj)))


class AppOpenArgs:
    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_stat_date()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_reports()

    def _explode_stat_date(self):
        stat_time = self.args.get('dateday')
        stat_time = int(stat_time)
        if not stat_time:
            raise ValueError('cannot get dateday')
        if len(str(stat_time)) < 10:
            raise ValueError('cannot get right dateday')
        stat_time = int(stat_time * (10 ** (10 - len(str(stat_time)))))
        self.stat_time = stat_time
        self.stat_date = Func.get_date(stat_time, '%Y-%m-%d')
        self.stat_hour = int(Func.get_date(stat_time, '%H'))

    def _explode_plat(self):
        plat = self.args.get('plat', 1)
        plat = int(plat)
        if not plat:
            raise ValueError('cannot get plat')
        self.plat = plat

    def _explode_agent_type(self):
        agent_type = self.args.get('agent_type', 4)
        agent_type = int(agent_type)
        if not agent_type:
            raise ValueError('cannot get agent_type')
        
        self.agent_type = agent_type

    def _explode_reports(self):
        number = self.args.get('number')
        number = int(number)

        self.startup_count_pv = 1
        self.startup_count_uv = number

        self.vip_count_pv = 0
        self.vip_count_uv = 0

        self.login_count_pv = 0
        self.login_count_uv = 0

        self.vip_login_count_pv = 0
        self.vip_login_count_uv = 0

        self.enjoy_count_pv = 0
        self.enjoy_count_uv = 0

    def __dict(self):
        reports = {
            'stat_time': self.stat_time,
            'stat_date': self.stat_date,
            'stat_hour': self.stat_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,

            'startup_count_pv': self.startup_count_pv,
            'startup_count_uv': self.startup_count_uv,

            'login_count_pv': self.login_count_pv,
            'login_count_uv': self.login_count_uv,

            'vip_count_pv': self.vip_count_pv,
            'vip_count_uv': self.vip_count_uv,

            'vip_login_count_pv': self.vip_login_count_pv,
            'vip_login_count_uv': self.vip_login_count_uv,

            'enjoy_count_pv': self.enjoy_count_pv,
            'enjoy_count_uv': self.enjoy_count_uv,
        }
        return reports

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
