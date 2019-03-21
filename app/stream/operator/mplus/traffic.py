# -- coding: UTF-8

from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction
import json
from app.utils import Func, logger, LogName


class Traffic:
    @staticmethod
    def stream_explode(stream):
        """
        流逻辑，必须实现
        """
        stream = stream.flat_map(TrafficSave())
        return stream


class TrafficSave(FlatMapFunction):
    """
    格式化并去掉不标准数据
    :return str(dict)
    """
    def flatMap(self, stream, collector):
        topic, ip, traffic_list = stream
        traffic_list = json.loads(traffic_list)
        for traffic in traffic_list:
            try:
                traffic_args = TrafficArgs(traffic)
                traffic_obj = traffic_args.to_dict()
                if traffic_obj.get('agent_type') not in [4, 5]:
                    traffic_obj['ip'] = ip
            except Exception as e:
                logger(LogName.TRAFFIC).error('{}, {}, {}'.format(topic, e, traffic))
            else:
                collector.collect((topic, json.dumps(traffic_obj)))


class TrafficArgs:
    def __init__(self, args):
        self.args = args
        # 解析参数
        self._explode_req_time()
        self._explode_plat()
        self._explode_agent_type()
        self._explode_website()
        self._explode_url()
        self._explode_ip()
        self._explode_visit_id()
        self._explode_loading_time()
        self._explode_req_status()
        self._explode_extra()
        self._explode_market()

    def _explode_req_time(self):
        req_time = self.args.get('req_time')
        req_time = int(req_time)
        if not req_time:
            raise ValueError('cannot get req_time')
        if len(str(req_time)) < 10:
            raise ValueError('cannot get right req_time')
        req_time = int(req_time * (10 ** (10 - len(str(req_time)))))
        if abs(req_time - Func.get_timestamp()) > (86400 * 30):
            raise ValueError('cannot get nearly req_time')
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

    def _explode_website(self):
        website = self.args.get('website')
        self.website = website

    def _explode_url(self):
        self.url = self.args.get('url', '')
        self.host = Func.url_parse(self.url).hostname or ''
        self.ref_url = self.args.get('ref_url', '')

    def _explode_ip(self):
        self.ip = self.args.get('ip', '')

    def _explode_visit_id(self):
        visit_id = self.args.get('visit_id', '')
        self.visit_id = str(visit_id)

    def _explode_loading_time(self):
        loading_time = self.args.get('loading_time', 0)
        loading_time = int(loading_time)
        loading_time = abs(loading_time)

        self.loading_time = loading_time

    def _explode_req_status(self):
        req_status = self.args.get('req_status', 1)
        req_status = int(req_status)
        if req_status is 0:
            req_status = 1
        self.req_status = req_status

    def _explode_extra(self):
        extra = self.args.get('extra', '')
        self.extra = extra

    def _explode_market(self):
        market_args = self.args.get('market')
        if market_args:
            if type(market_args) is not dict:
                raise ValueError('market error')
            market = {
                'source': market_args.get('source', ''),
                'medium': market_args.get('medium', ''),
                'campaign': market_args.get('campaign', ''),
                'content': market_args.get('content', ''),
                'term': market_args.get('term', ''),
            }
        else:
            market = {
                'source': self.args.get('source', ''),
                'medium': self.args.get('medium', ''),
                'campaign': self.args.get('campaign', ''),
                'content': self.args.get('content', ''),
                'term': self.args.get('term', ''),
            }

        self.market = market

    def __dict(self):
        traffic = {
            'req_time': self.req_time,
            'req_date': self.req_date,
            'req_hour': self.req_hour,
            'plat': self.plat,
            'agent_type': self.agent_type,
            'website': self.website,
            'url': self.url,
            'host': self.host,
            'ref_url': self.ref_url,
            'ip': self.ip,
            'visit_id': self.visit_id,
            'loading_time': self.loading_time,
            'req_status': self.req_status,
            'extra': self.extra,
            'market': self.market
        }
        return traffic

    def to_dict(self):
        return self.__dict()

    def to_json(self):
        return json.dumps(self.__dict())

