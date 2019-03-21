# -- coding: UTF-8
from app.utils import Func
import json


class ClickArgsBase:
    """
    将redis拿到的数据格式化成对象
    """
    def __str__(self):
        return self.to_json()

    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_req_time()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_visit_id()

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

    def _explode_visit_id(self):
        visit_id = self.args.get('visit_id', '')
        visit_id = str(visit_id)
        self.visit_id = visit_id

    def _explode_click(self, category):
        self.click_category = category
        click_action = self.args.get('click_action', '')
        self.click_action = str(click_action)
        click_value = self.args.get('click_value', '')
        self.click_value = str(click_value)

    def _dict(self):
        click = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'visit_id': self.visit_id,
            'click_category': self.click_category,
            'click_action': self.click_action,
            'click_value': self.click_value,
        }
        return click

    def to_dict(self):
        return self._dict()

    def to_json(self):
        return json.dumps(self._dict())
