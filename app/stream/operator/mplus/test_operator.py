# -- coding: UTF-8 

from app.stream.operator.base import OperatorBase
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
import json
from app.stream.store.database import ClickhouseStore, ck_table
from app.utils import Func, logger
import uuid
import time


class TestOperator(OperatorBase):
    def get_stream(self, stream):
        stream = stream.map(MapTest())
        return stream


class MapTest(MapFunction):
    def map(self, value):
        value = json.loads(value)
        value = value.get('Args').get('data')[0]

        DetailsTraffic = ck_table.DetailsTraffic
        ck_store = ClickhouseStore()
        ck_session = ck_store.get_session()
        value_dict = {
            'id': str(uuid.uuid4()),
            'add_date': Func.get_date(),
            'add_time': Func.get_date(time_format='%Y-%m-%d %H:%M:%S'),
            'update_time': Func.get_date(time_format='%Y-%m-%d %H:%M:%S'),
            'website': value.get('website').decode('utf-8'),
            'plat': value.get('plat'),
            'req_status': value.get('req_status'),
            'agent_type': value.get('agent_type'),
            'req_date': '2019-03-14',
            'url': value.get('url'),
            'visit_id': value.get('visit_id'),
        }
        update_dict = {
            'host': '\'123\'',
            'ref_url': '\'{}\''.format(str(time.time()))
        }
        ck_store.insert_list(ck_session, DetailsTraffic, [value_dict])
        filter_obj = ck_session.query(DetailsTraffic).\
            filter(DetailsTraffic.req_date == '2019-03-15').\
            filter(DetailsTraffic.req_date <= '2019-03-16'). \
            filter(DetailsTraffic.plat == 1)
        ck_store.update(DetailsTraffic, filter_obj, update_dict)
        ck_store.delete(DetailsTraffic, filter_obj)
        ck_session.close()
        return str(value)
