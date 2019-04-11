# -- coding: UTF-8

from sqlalchemy import create_engine, Column, MetaData, literal
from clickhouse_sqlalchemy import make_session, get_declarative_base, types, engines
import uuid
from app.utils.func import Func
from sqlalchemy.dialects import registry
from conf import get_conf
from app.utils import logger


class ClickhouseStore:
    @staticmethod
    def get_session(connection='base'):
        connection = ClickhouseStore.connection_str(connection)
        engine = create_engine(connection)
        session = make_session(engine)
        return session

    @staticmethod
    def connection_str(connection='base'):
        registry.register('clickhouse', 'clickhouse_sqlalchemy.drivers.base', 'dialect')
        conf = get_conf('database', 'CLICKHOUSE').get(connection)
        connection = 'clickhouse://{user}:{password}@{host}:{port}/{db}'.format(**conf)
        return connection

    @staticmethod
    def insert_list(session, table, data_list):
        """
        插入列表
        :param session: clickhouse链接
        :param table: clickhouse的model
        :param data_list: 数据, 参考官方文档
        :return:
        """
        try:
            session.execute(table.__table__.insert(), data_list)
        except Exception as e:
            logger().error('CK INSERT ERROR error:{} data:{}', e, data_list)

    @staticmethod
    def insert_single(session, table, data):
        """
        插入单条
        :param session: clickhouse链接
        :param table: clickhouse的model
        :param data: 数据, 参考官方文档
        :return:
        """
        try:
            session.execute(table.__table__.insert(), [data])
        except Exception as e:
            logger().error('CK INSERT ERROR error:{} data:{}', e, data)

    @staticmethod
    def delete(table, filter_obj):
        """
        手工组装delete语句, 如果clickhouse_sqlalchemy支持delete操作, 请尽快停用此方法
        :param table: store中clickhouse的model
        :param filter_obj: orm类,务必包含filter
        Example: session.query(Table).filter(Table.id=1).filter(Table.name='Fang')
        :return:
        """
        filter_sql, filter_value = ClickhouseStore.__filter_format(filter_obj, table.__tablename__)
        exec_sql = 'ALTER TABLE {} DELETE WHERE {}'.format(table.__table__, filter_sql)
        try:
            filter_obj.session.execute(exec_sql, filter_value)
        except Exception as e:
            logger().error('CK DELETE ERROR error:{} sql:{}, data:{}', e, exec_sql, filter_value)

    @staticmethod
    def update(table, filter_obj, update_dict):
        """
        手工组装update语句, 如果clickhouse_sqlalchemy支持update操作, 请尽快停用此方法
        :param table: store中clickhouse的model
        :param filter_obj: orm类,务必包含filter
        Example: session.query(Table).filter(Table.id=1).filter(Table.name='Fang')
        :param update_dict: 需要更新的值字典,字符串请转义,不能更新字段不验证,直接由底层抛出异常
        Example: {'id': 4, 'name': '\'Hu\''}
        :return:
        """
        filter_sql, filter_value = ClickhouseStore.__filter_format(filter_obj, table.__tablename__)
        update_sql = ClickhouseStore.__update_format(update_dict)
        exec_sql = 'ALTER TABLE {} UPDATE {} WHERE {}'.format(table.__table__, update_sql, filter_sql)
        try:
            filter_obj.session.execute(exec_sql, filter_value)
        except Exception as e:
            logger().error('CK UPDATE ERROR error:{} sql:{}, data:{}', e, exec_sql, filter_value)

    @staticmethod
    def __filter_format(filter_obj, table_name):
        """where条件的语句和替代值组装"""
        whereclause = filter_obj.whereclause
        filter_sql = str(whereclause).replace('{}.'.format(table_name), '')
        filter_value = ClickhouseStore.__clause_list_split(whereclause)
        return filter_sql, filter_value

    @staticmethod
    def __clause_list_split(whereclause):
        """寻找替换值"""
        filter_dict = {}
        if hasattr(whereclause, 'clauses'):
            clauses = whereclause.clauses
        else:
            clauses = [whereclause]
        for clause in clauses:
            if type(clause).__name__ == 'BinaryExpression':
                filter_key = str(clause.right).split(':')[-1]
                filter_value = clause.right.value
                filter_key = ClickhouseStore.__clause_key_combine(filter_key, filter_dict)
                filter_dict[filter_key] = filter_value
            elif type(clause).__name__ == 'BooleanClauseList':
                filter_dict = dict(filter_dict, **ClickhouseStore.__clause_list_split(clause))
        return filter_dict

    @staticmethod
    def __clause_key_combine(filter_key, filter_dict):
        """键名替代"""
        if not filter_dict.get(filter_key):
            return filter_key
        else:
            filter_key = filter_key.split('_')
            filter_key[-1] = str(int(filter_key[-1]) + 1)
            filter_key = '_'.join(filter_key)
            return ClickhouseStore.__clause_key_combine(filter_key, filter_dict)

    @staticmethod
    def __update_format(update_dict):
        update_sql = ','.join(['{}={}'.format(key, value) for key, value in update_dict.items()])
        return update_sql


CkBase = get_declarative_base()


class DetailsTraffic(CkBase):
    __tablename__ = 'details_traffic'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())  # 主键id
    req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    req_date = Column(types.Date)  # 请求时间 Y-m-d
    req_hour = Column(types.Int)  # 请求时间 H
    plat = Column(types.Int8)          # 平台 1/金业
    agent_type = Column(types.Int8)    # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    website = Column(types.String)     # 网页标题
    url = Column(types.String)         # url
    host = Column(types.String)         # 域名
    ref_url = Column(types.String)     # 上级url
    ip = Column(types.String)          # ip
    visit_id = Column(types.String)    # 唯一vid
    loading_time = Column(types.Int64)   # 网页打开完成时间
    req_status = Column(types.Int8)    # 打开状态 1/成功 2/失败
    extra = Column(types.String)     # 额外信息
    market_source = Column(types.String)    # 来源
    market_medium = Column(types.String)    # 媒介
    market_campaign = Column(types.String)  # 系列
    market_content = Column(types.String)   # 内容
    market_term = Column(types.String)      # 关键字
    add_date = Column(types.Date, default=Func.get_date())   # 添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, req_time, agent_type), index_granularity=8192),
    )