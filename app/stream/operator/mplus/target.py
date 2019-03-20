# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func
from app.utils import logger
import uuid
from app.utils.enums import TargetEnum


class Target:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(TargetSave())


class TargetSave(FlatMapFunction):
    """
    格式化并去掉不标准数据
    :return str(dict)
    """
    def flatMap(self, stream, collector):
        topic, ip, target_list = stream
        target_list = json.loads(target_list)
        for target in target_list:
            try:
                logger('target').notice('target get {}'.format(target))
                target_obj = TargetArgs(target)
                target_obj = target_obj.to_dict()
            except Exception as e:
                logger().error('{}, {}, {}'.format(topic, e, target))
            else:
                """收集供后面使用"""
                collector.collect((topic, json.dumps(target_obj)))


class TargetArgs:
    """
    将redis拿到的target格式化成对象
    """
    def __str__(self):
        return self.to_json()

    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_req_time()
        self._explode_plat()
        self._explode_ip()
        self._explode_account()
        self._explode_mobile()
        self._explode_target_type()
        self._explode_agent_type()
        self._explode_visit_id()
        self._explode_market()
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
        
        self.agent_type = agent_type
        
    def _explode_ip(self):
        self.ip = self.args.get('ip', '')

    def _explode_account(self):
        account = self.args.get('account', '')
        self.account = str(account)

    def _explode_mobile(self):
        mobile = self.args.get('mobile', '')
        self.mobile = str(mobile)

    def _explode_extra(self):
        extra = self.args.get('extra', '')
        self.extra = extra

    def _explode_target_type(self):
        target_type = int(self.args.get('target_type'))
        target_type = int(target_type)
        if not target_type:
            raise ValueError('cannot get target_type')
        self.target_type = target_type
        self.__check_target_type()

    def _explode_visit_id(self):
        visit_id = self.args.get('visit_id', '')
        visit_id = str(visit_id)
        if visit_id == '00000000-0000-0000-0000-000000000000' and (self.target_type in [TargetEnum.D, TargetEnum.N]):
            visit_id = str(uuid.uuid4())
        self.visit_id = visit_id

    def _explode_market(self):
        self.source = self.args.get('source', '(not set)')
        self.medium = self.args.get('medium', '(not set)')

    def __check_target_type(self):
        """不同target必须字段检测"""
        target_check_list = {
            TargetEnum.V: [],
            TargetEnum.NL: ['mobile'],
            TargetEnum.D: ['mobile', 'account'],
            TargetEnum.N: ['mobile', 'account'],
            TargetEnum.A: ['account'],
        }
        check_list = target_check_list.get(self.target_type)
        for attr in check_list:
            if not getattr(self, attr):
                raise ValueError('target {} cannot get {}'.format(self.target_type, attr))

    def __dict(self):
        target = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'target_type': self.target_type,
            'ip': self.ip,
            'visit_id': self.visit_id,
            'account': self.account,
            'mobile': self.mobile,
            'source': self.source,
            'medium': self.medium,
            'extra': self.extra
        }
        return target

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
