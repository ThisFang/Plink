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


class DetailsTarget(CkBase):
    __tablename__ = 'details_target'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    req_date = Column(types.Date)  # 请求时间 Y-m-d
    req_hour = Column(types.Int)  # 请求时间 H
    plat = Column(types.Int8)          # 平台 1/金业
    agent_type = Column(types.Int8)    # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    target_type = Column(types.Int8)   # 指标：1/访问(V) 2/咨询(NL) 3/模拟开户(D) 4/真实开户(N) 5/首次入金(A)
    ip = Column(types.String)          # ip
    visit_id = Column(types.String)    # 唯一vid
    account = Column(types.String)  # 开户账户
    mobile = Column(types.String)  # 开户手机
    source = Column(types.String)  # 来源
    medium = Column(types.String)  # 媒介
    extra = Column(types.String)  # 额外信息
    add_date = Column(types.Date, default=Func.get_date())  # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, req_time, agent_type), index_granularity=8192),
    )
    # replacing引擎示例，不要删除
    # __table_args__ = (
    #     engines.ReplacingMergeTree(
    #         add_date,
    #         (plat, agent_type, target_type, ip, visit_id),
    #         version_col='update_time',
    #         index_granularity=10
    #     ),
    # )


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


class DetailsPushApp(CkBase):
    __tablename__ = 'details_push_app'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())  # 主键id
    req_time = Column(types.DateTime)  # 请求时间 10位时间戳
    req_date = Column(types.Date)  # 请求时间 Y-m-d
    req_hour = Column(types.Int)  # 请求时间 H
    plat = Column(types.Int8)          # 平台 1/金业
    agent_type = Column(types.Int8)    # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    push_type = Column(types.Int8)     # 推送类别   1/广播(notice)
    ip = Column(types.String)          # ip
    visit_id = Column(types.String)    # 唯一vid
    detail_id = Column(types.String)    # 消息detail_id
    oper_type = Column(types.Int8)     # 操作类别   1/打开数
    extra = Column(types.String)  # 额外信息
    add_date = Column(types.Date, default=Func.get_date())    # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, push_type, req_time, agent_type), index_granularity=8192),
    )


class DetailsPushSend(CkBase):
    __tablename__ = 'details_push_send'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())  # 主键id
    start_time = Column(types.DateTime)  # 开始时间 10位时间戳
    start_date = Column(types.Date)  # 开始时间 Y-m-d
    start_hour = Column(types.Int)  # 开始时间 H
    plat = Column(types.Int8)          # 平台 1/金业
    agent_type = Column(types.Int8)    # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    push_category = Column(types.Int8)     # 推送类别
    push_type = Column(types.Int8)     # 推送场景
    msg_id = Column(types.String)      # 极光消息id
    detail_id = Column(types.String)   # 消息detail_id
    title = Column(types.String)    # 消息标题
    content = Column(types.String)  # 消息内容
    url = Column(types.String)      # 消息url
    extra = Column(types.String)    # 额外信息
    add_date = Column(types.Date, default=Func.get_date())    # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, push_type, start_time, agent_type), index_granularity=8192),
    )


class VisitMarketRecord(CkBase):
    __tablename__ = 'visit_market_record'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    visit_id = Column(types.String)   # 唯一vid
    ip = Column(types.String)  # ip
    plat = Column(types.Int8)  # 平台 1/金业
    agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    website = Column(types.String)   # 网页标题
    url = Column(types.String)       # 网页url
    market_source = Column(types.String)    # 来源
    market_medium = Column(types.String)    # 媒介
    market_campaign = Column(types.String)  # 系列
    market_content = Column(types.String)   # 内容
    market_term = Column(types.String)      # 关键字
    add_date = Column(types.Date, default=Func.get_date())   # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.ReplacingMergeTree(
            add_date,
            (plat, agent_type, visit_id),
            version_col='update_time',
            index_granularity=10),
    )


class VisitAccount(CkBase):
    __tablename__ = 'visit_account'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    visit_id = Column(types.String)  # 唯一vid
    ip = Column(types.String)   # ip
    plat = Column(types.Int8)   # 平台 1/金业
    agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    account = Column(types.String)   # 账户
    mobile = Column(types.String)  # 手机
    account_type = Column(types.Int8)  # 账户类别 1/正式 2/模拟
    add_date = Column(types.Date, default=Func.get_date())  # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, agent_type, account, account_type), index_granularity=8192),
    )


class DetailsAggPayOrder(CkBase):
    """ 聚合订单 """
    __tablename__ = 'details_agg_pay_order'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    plat = Column(types.Int8)               # 平台 1/金业
    agent_type = Column(types.Int8)         # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    order_no = Column(types.String)         # 聚合单号
    buss_no = Column(types.String)          # 业务单号
    account = Column(types.String)          # 账户
    rmb = Column(types.Int64)               # 金额(人民币)[单位：分]
    usd = Column(types.Int64)               # 金额(美元)[单位：分]
    money_rate = Column(types.Float32 )     # 汇率
    channel = Column(types.String)          # 通道（第三方、微信、支付宝）
    channel_desc = Column(types.String)     # 通道详细,如:7080_快捷
    status = Column(types.Int8)             # 订单状态:0=待付款,1=已付款,2=付款失败
    is_first = Column(types.Int8)           # 是否首入:0=否,1=是
    pay_source = Column(types.String)       # 订单来源,如:800001=网上存款,800002=兑换商城,钜丰=800008
    order_time = Column(types.DateTime)     # 订单时间(DateTime)
    order_date = Column(types.Date)         # 订单时间(Date)
    order_hour = Column(types.Int8)         # 订单时间(Hour)
    pay_time = Column(types.DateTime, nullable=True)       # 支付成功时间
    add_date = Column(types.Date, default=Func.get_date())                                          # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))       # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, order_no, account), index_granularity=8192),
    )


class DetailsDeposit(CkBase):
    """ 出入金订单 """
    __tablename__ = 'details_deposit'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    plat = Column(types.Int8)               # 平台 1/金业
    agent_type = Column(types.Int8)         # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    order_no = Column(types.String)         # 单号
    buss_no = Column(types.String)          # 平台业务单号
    account = Column(types.String)          # 账户
    rmb = Column(types.Int64)               # 金额(人民币)[单位：分]
    usd = Column(types.Int64)               # 金额(美元)[单位：分]
    money_rate = Column(types.Float32)      # 汇率
    status = Column(types.Int8)             # 订单状态:0=待付款,1=已付款,2=付款失败
    status_detail = Column(types.String)    # 数据源订单出入金状态
    level = Column(types.Int8)              # 金额等级 1/0~200 2/200~500 3/500~1000 4/1000~2500 5/2500~5000 6/5000~10000 7/>10000
    is_first = Column(types.Int8)           # 是否首入:0=否,1=是
    type = Column(types.Int8)               # 类型 1入金 2出金
    pay_time = Column(types.DateTime)       # 支付时间 Y-m-d H:i:s
    order_time = Column(types.DateTime)     # 订单时间 Y-m-d H:i:s
    order_date = Column(types.Date)         # 订单时间 Y-m-d
    order_hour = Column(types.Int)          # 订单时间 H
    add_date = Column(types.Date, default=Func.get_date())                                          # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))       # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, order_no, account), index_granularity=8192),
    )


class DetailsClick(CkBase):
    """ 点击事件相关 """
    __tablename__ = 'details_click'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    plat = Column(types.Int8)               # 平台 1/金业
    agent_type = Column(types.Int8)         # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    click_category = Column(types.String)   # 事件分类
    click_action = Column(types.String)     # 时间动作
    click_value = Column(types.String)      # 事件值
    visit_id = Column(types.String)       # 用户id
    ip = Column(types.String)             # ip
    req_time = Column(types.DateTime)     # 事件时间 10位时间戳
    req_date = Column(types.Date)         # 事件时间 Y-m-d
    req_hour = Column(types.Int)          # 事件时间 H
    add_date = Column(types.Date, default=Func.get_date())                                          # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))       # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, agent_type, click_category), index_granularity=8192),
    )


class DetailsTrading(CkBase):
    """ 交易数据(弃用) """
    __tablename__ = 'details_trading'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    plat = Column(types.Int8)               # 平台 1/金业
    agent_type = Column(types.Int8)         # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    order_no = Column(types.String)         # 单号
    account = Column(types.String)          # 账户
    account_level = Column(types.String)    # 账户等级
    type = Column(types.Int8)               # 类型 1建仓 2平仓
    volume = Column(types.Float32)          # 手数
    order_time = Column(types.DateTime)     # 订单时间 10位时间戳
    order_date = Column(types.Date,nullable=True)         # 订单时间 Y-m-d
    order_hour = Column(types.Int)          # 订单时间 H
    add_date = Column(types.Date, default=Func.get_date())                                          # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))       # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))    # 数据更新时间

    __table_args__ = (
        engines.MergeTree(add_date, (plat, order_no, account), index_granularity=8192),
    )


class TrafficVisitId(CkBase):
    __tablename__ = 'traffic_visit_id'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    visit_id = Column(types.String)   # 唯一vid
    plat = Column(types.Int8)  # 平台 1/金业
    agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    req_time = Column(types.DateTime)     # 事件时间 10位时间戳
    req_date = Column(types.Date)         # 事件时间 Y-m-d
    add_date = Column(types.Date, default=Func.get_date())   # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.ReplacingMergeTree(
            req_date,
            (plat, agent_type, visit_id),
            version_col='req_time',
            index_granularity=8192),
    )


class DetailsViewTime(CkBase):
    __tablename__ = 'details_view_time'
    id = Column(types.String, primary_key=True, default=uuid.uuid4())
    visit_id = Column(types.String)   # 唯一vid
    plat = Column(types.Int8)  # 平台 1/金业
    agent_type = Column(types.Int8)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    channel = Column(types.String)   # 渠道
    website = Column(types.String)          # 网页
    req_time = Column(types.DateTime)     # 事件时间 10位时间戳
    req_date = Column(types.Date)         # 事件时间 Y-m-d
    view_time = Column(types.Int64)         # 访问时长
    add_date = Column(types.Date, default=Func.get_date())   # 数据添加日期
    add_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据添加时间
    update_time = Column(types.DateTime, default=Func.get_date(time_format='%Y-%m-%d %H:%M:%S'))  # 数据更新时间

    __table_args__ = (
        engines.SummingMergeTree(
            req_date,
            (plat, agent_type, channel, visit_id, website, req_time),
            (view_time,)),
    )
