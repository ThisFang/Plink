# -- coding: UTF-8 
from app.common import SuperBase
from app.utils import Func
import uuid
import json
from app.stream.store.database import ck_table, ClickhouseStore
from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.collector.selector import OutputSelector


class SinkBase(SuperBase):
    def __init__(self, boot_conf):
        super(SinkBase, self).__init__(boot_conf)

    def write_by_stream(self, data_stream):
        """
        该函数指示将指定数据流写入至目标处
        """
        raise NotImplementedError


class ToData(MapFunction):
    """去掉分流标识topic,只留下data"""
    def map(self, value):
        topic, data = value
        return data


class KeyBy(KeySelector):
    def getKey(self, value):
        return 1


class TopicSelector(OutputSelector):
    def select(self, value):
        topic, data = value
        return [str(topic)]


class ClickHouseApply(WindowFunction, FlatMapFunction):
    """
    公共的写入CK,格式请在外部组装好
    """
    def __init__(self):
        self._connection = 'base'
        self._table = None

    def apply(self, key, window, values, collector):
        self.__insert(values)
        for value in values:
            collector.collect(value)

    def flatMap(self, value, collector):
        self.__insert([value])
        collector.collect(value)

    def __insert(self, values):
        ck_list = []
        for value in values:
            value = json.loads(value)
            value['id'] = str(uuid.uuid4())
            value['add_date'] = Func.get_date()
            value['add_time'] = Func.get_date(time_format='%Y-%m-%d %H:%M:%S')
            value['update_time'] = Func.get_date(time_format='%Y-%m-%d %H:%M:%S')
            ck_list.append(value)

        session = ClickhouseStore.get_session(self._connection)
        piece_len = 50
        for piece in range(0, len(ck_list), piece_len):
            ck_list_piece = ck_list[piece:piece + piece_len]
            ClickhouseStore.insert_list(session, self._table, ck_list_piece)
        session.close()

    def set_table(self, table):
        """设置表名"""
        self._table = table
        return self

    def set_connection(self, connection):
        """设置链接"""
        self._connection = connection
        return self
