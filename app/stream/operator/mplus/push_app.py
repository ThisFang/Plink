# -- coding: UTF-8

import json
from app.utils import Func
from app.utils import logger
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction


class PushApp:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(PushAppSave())


class PushAppSave(FlatMapFunction):
    """
    格式化并去掉不标准数据
    :return str(dict)
    """
    def flatMap(self, stream, collector):
        topic, ip, push_app_list = stream
        push_app_list = json.loads(push_app_list)
        for push_app in push_app_list:
            try:
                push_app_obj = PushAppArgs(push_app)
                push_app_obj = push_app_obj.to_dict()
                push_app_obj['ip'] = ip
            except Exception as e:
                logger().error('{}, {}, {}'.format(topic, e, push_app))
            else:
                """收集供后面使用"""
                collector.collect((topic, str(push_app_obj)))


class PushAppArgs:
    """
    将redis拿到的PushApp格式化成对象
    """
    def __str__(self):
        return self.to_json()

    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_req_time()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_push_type()
        self._explode_oper_type()
        self._explode_detail_id()
        self._explode_ip()
        self._explode_visit_id()
        self._explode_extra()

    def _explode_req_time(self):
        req_time = self.args.get('req_time')
        req_time = int(req_time)
        if not req_time:
            raise ValueError('cannot get req_time')
        if len(str(req_time)) < 10:
            raise ValueError('cannot get right req_time')
        req_time = int(req_time * (10 ** (10 - len(str(req_time)))))
        self.req_time = req_time
        self.req_date = Func.get_date(req_time, '%Y-%m-%d')
        self.req_hour = int(Func.get_date(req_time, '%H'))

    def _explode_plat(self):
        plat = self.args.get('plat')
        plat = int(plat)
        if not plat:
            raise ValueError('cannot get plat')
        self.plat = plat

    def _explode_agent_type(self):
        agent_type = self.args.get('agent_type')
        agent_type = int(agent_type)
        if not agent_type:
            raise ValueError('cannot get agent_type')
        if agent_type not in [4, 5]:
            raise ValueError('cannot get right agent_type')

        self.agent_type = agent_type

    def _explode_push_type(self):
        push_type = self.args.get('push_type')
        push_type = int(push_type)
        if not push_type:
            raise ValueError('cannot get push_type')
        self.push_type = push_type

    def _explode_oper_type(self):
        oper_type = self.args.get('oper_type', 1)
        oper_type = int(oper_type)
        if not oper_type:
            raise ValueError('cannot get oper_type')
        self.oper_type = oper_type

    def _explode_detail_id(self):
        detail_id = self.args.get('detail_id')
        if not detail_id:
            raise ValueError('cannot get detail_id')
        detail_id = str(detail_id)
        self.detail_id = detail_id

    def _explode_ip(self):
        self.ip = self.args.get('ip', '')

    def _explode_visit_id(self):
        visit_id = self.args.get('visit_id', '')
        self.visit_id = str(visit_id)

    def _explode_extra(self):
        extra = self.args.get('extra', '')
        self.extra = extra.decode('utf-8')

    def __dict(self):
        push_app = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'detail_id': self.detail_id,
            'push_type': self.push_type,
            'oper_type': self.oper_type,
            'ip': self.ip,
            'visit_id': self.visit_id,
            'extra': self.extra
        }
        return push_app

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
