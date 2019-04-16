# -- coding: UTF-8

import uuid
from app.utils.func import Func
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# from runtime import get_env_conf
from conf import get_conf


class MysqlStore:
    @staticmethod
    def get_session(connection='preview'):
        conf = get_conf('database', 'MYSQL').get(connection)
        connection = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8'.format(**conf)
        engine = create_engine(connection)
        session = sessionmaker(bind=engine)
        return session()

    @staticmethod
    def get_table(table, **args):
        return Func.create_instance('app.stream.store.database.mysql_store', table, **args)


# 创建对象的基类:
MysqlBase = declarative_base()


class PreviewStatTraffic(MysqlBase):
    __tablename__ = 'stat_traffic'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台
    agent_type = Column(Integer)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Andriod
    website = Column(String(100))  # 页面
    pv_count = Column(Integer, default=0)  # 访问pv统计
    uv_count = Column(Integer, default=0)  # 访问uv统计
    ip_count = Column(Integer, default=0)  # 访问ip统计
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


