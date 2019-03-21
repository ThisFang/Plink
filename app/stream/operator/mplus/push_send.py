# -- coding: UTF-8

import json
from app.utils import Func
from app.utils import logger
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction


class PushSend:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        return stream.flat_map(PushSendSave())


class PushSendSave(FlatMapFunction):
    """
    格式化并去掉不标准数据
    :return str(dict)
    """
    def flatMap(self, stream, collector):
        topic, ip, push_send_list = stream
        push_send_list = json.loads(push_send_list)
        for push_send in push_send_list:
            try:
                push_send_obj = PushSendArgs(push_send)
                push_send_obj = push_send_obj.to_dict()
                push_send_obj['ip'] = ip
            except Exception as e:
                logger().error('{}, {}, {}'.format(topic, e, push_send))
            else:
                collector.collect((topic, json.dumps(push_send_obj)))


class PushSendArgs:
    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_req_time()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_push_type()
        self._explode_push_category()
        self._explode_msg_id()
        self._explode_detail_id()
        self._explode_content()
        self._explode_extra()

    def _explode_req_time(self):
        start_time = self.args.get('start_time')
        start_time = int(start_time)
        if not start_time:
            raise ValueError('cannot get start_time')
        if len(str(start_time)) < 10:
            raise ValueError('cannot get right start_time')
        start_time = int(start_time * (10 ** (10 - len(str(start_time)))))
        self.start_time = start_time
        self.start_date = Func.get_date(start_time, '%Y-%m-%d')
        self.start_hour = int(Func.get_date(start_time, '%H'))

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

    def _explode_push_category(self):
        push_category = self.args.get('push_category', 1)
        push_category = int(push_category)
        if not push_category:
            raise ValueError('cannot get push_category')
        self.push_category = push_category

    def _explode_push_type(self):
        push_type = self.args.get('push_type', 4)
        push_type = int(push_type)
        if not push_type:
            raise ValueError('cannot get push_type')
        self.push_type = push_type

    def _explode_msg_id(self):
        msg_id = self.args.get('msg_id')
        if not msg_id:
            raise ValueError('cannot get msg_id')
        msg_id = str(msg_id)
        self.msg_id = msg_id

    def _explode_detail_id(self):
        detail_id = self.args.get('detail_id')
        if not detail_id:
            raise ValueError('cannot get detail_id')
        detail_id = str(detail_id)
        self.detail_id = detail_id

    def _explode_content(self):
        title = self.args.get('title', '')
        self.title = title

        content = self.args.get('content', '')
        self.content = content

        url = self.args.get('url', '')
        self.url = url

    def _explode_extra(self):
        extra = self.args.get('extra', '')
        self.extra = extra

    def __dict(self):
        push_send = {
            'start_time': self.start_time,
            'start_date': self.start_date,
            'start_hour': self.start_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'push_category': self.push_category,
            'push_type': self.push_type,
            'msg_id': self.msg_id,
            'detail_id': self.detail_id,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'extra': self.extra
        }
        return push_send

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())
