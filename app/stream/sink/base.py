# -- coding: UTF-8 
from app.common import SuperBase
from app.utils import Func
import uuid
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


class ToString(MapFunction):
    """通用的字符串化"""
    def map(self, value):
        return str(value)


class ToData(MapFunction):
    """去掉分流标识topic,只留下data"""
    def map(self, value):
        topic, data = value
        return str(data)


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
        self._table = None
        self._connection = 'clickhouse'

    def apply(self, key, window, values, collector):
        self.__insert(values)
        collector.collect(str(values))

    def flatMap(self, value, collector):
        self.__insert([value])
        collector.collect(str(value))
        pass

    def __insert(self, values):
        ck = StoreDbClickHouse()
        session = ck.get_session(self._connection)
        ck_list = []
        for value in values:
            value = eval(value)
            value['id'] = str(uuid.uuid4())
            value['add_date'] = Func.get_date()
            value['add_time'] = Func.get_date(time_format='%Y-%m-%d %H:%M:%S')
            value['update_time'] = Func.get_date(time_format='%Y-%m-%d %H:%M:%S')
            ck_list.append(value)

        # self.logger.info('insert list {}'.format(ck_list))

        piece_len = 50
        for piece in range(0, len(ck_list), piece_len):
            ck_list_piece = ck_list[piece:piece + piece_len]
            try:
                session.execute(ck.get_table(self._table).__table__.insert(), ck_list_piece)
            except Exception as e:
                self.logger.error('CK INSERT ERROR error:{} data:{}', e, ck_list_piece)

        session.close()

    def insert(self,values):
        self.__insert(values)

    def delete(self,table_name,condition):
        ck = StoreDbClickHouse()
        conn = ck.get_connection(self._connection)
        exec_sql = "ALTER TABLE {} DELETE WHERE {};".format(table_name,condition)
        conn.detach()
        conn.execute(exec_sql)
        conn.close()

    def update(self,table_name,expr,condition):
        """ ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr """
        ck = StoreDbClickHouse()
        conn = ck.get_connection(self._connection)
        exec_sql = "ALTER TABLE {} UPDATE {} WHERE {};".format(table_name,expr,condition)
        conn.detach()
        conn.execute(exec_sql)
        conn.close()

    def set_table(self, table):
        """设置表名"""
        self._table = table
        return self

    def set_connection(self, connection):
        """设置表名"""
        self._connection = connection
        return self
