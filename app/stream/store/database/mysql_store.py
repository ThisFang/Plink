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


class ClientStatPushDetails(MysqlBase):
    __tablename__ = 'stat_push_details'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer)  # 来源平台： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    push_id = Column(String(36))  # push_reports的push_id主键
    online_user = Column(Integer, default=0)  # 当天的在线用户数
    total_arrive_count = Column(Integer, default=0)  # 总到达数
    online_count = Column(Integer, default=0)  # 在线消息下发数
    click_yd = Column(Integer, default=0)  # 点击总数
    arrive_count = Column(Integer, default=0)  # 当日到达数
    real_push_count = Column(Integer, default=0)  # 实际下发数
    show_yd = Column(Integer, default=0)  # 展示总数数
    open_count = Column(Integer, default=0)  # app埋点数
    click_count = Column(Integer, default=0)  # 极光click数
    show_count = Column(Integer, default=0)  # 展示数
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时（未使用）
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class ClientStatPushReports(MysqlBase):
    __tablename__ = 'stat_push_reports'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer, default=1)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    push_id = Column(String(36))  # 推送id(极光推送id
    detail_id = Column(String(36))  # 运营平台 id
    start_time = Column(Integer)  # 推送开始时间
    end_time = Column(Integer)  # 推送结束时间(最后推送时间)
    valid_start = Column(Integer)  # 有效期-起始时间
    valid_end = Column(Integer)  # 有效期-结束时间
    title = Column(String(100))  # 标题
    content = Column(String)  # 消息内容
    url = Column(String(255))  # 消息跳转url
    total_count = Column(Integer, default=0)  # 实际下发数
    arrive_count = Column(Integer, default=0)  # 到达数
    open_count = Column(Integer, default=0)  # app埋点数
    click_count = Column(Integer, default=0)  # 极光click数
    status = Column(Integer, default=1)  # 消息状态 1/推送中 2/已完成 3/推送失败
    stat_date = Column(Integer)  # 统计日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class ClientStatRequest(MysqlBase):
    __tablename__ = 'stat_request'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer, default=1)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Andriod
    website = Column(String(100), default='')  # 页面
    loading_time = Column(Integer, default=0)  # 加载时间(ms)
    req_total_count = Column(Integer, default=0)  # 请求次数
    req_fail_count = Column(Integer, default=0)  # 加载失败次数
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class PreviewStatTraffic(MysqlBase):
    __tablename__ = 'stat_traffic'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
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


class PreviewStatTarget(MysqlBase):
    __tablename__ = 'stat_target'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    expand_route = Column(String(100))  # 转化路径 1-2-3-4-5 || 1-2-4-5 || 1-3-4-5 || 1-4-5
    target_type = Column(Integer)  # 指标：1/访问(V) 2/咨询(NL) 3/模拟开户(D) 4/真实开户(N) 5/首次入金(A)
    count = Column(Integer, default=0)  # 访问页面统计
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class TradingStatDeposit(MysqlBase):
    __tablename__ = 'stat_deposit'

    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    is_first = Column(Integer)  # 是否首入 0/总计 1/首入 2/非首入
    type = Column(Integer)  # 1：入金 2：出金
    usd = Column(Integer, default=0)  # 累计总美元(分) 
    success_usd = Column(Integer, default=0)  # 累计成功美元(分)
    rmb = Column(Integer, default=0)  # 累计总人民币(分)
    success_rmb = Column(Integer, default=0)  # 累计成功人民币(分)
    order_count = Column(Integer, default=0)  # 累计总订单
    success_order_count = Column(Integer, default=0)  # 累计成功订单
    acc_count = Column(Integer, default=0)  # 累计总用户
    success_acc_count = Column(Integer, default=0)  # 累计成功总用户
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class TradingStatAggPay(MysqlBase):
    __tablename__ = 'stat_agg_pay'
    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    is_first = Column(Integer)  # 是否首入 0/总计 1/首入 2/非首入
    pay_source = Column(String)  # 支付产品 网上存款/800001  兑换商城/800002  钜丰/800008
    channel = Column(String)  # 支付通道 第三方 微信 支付宝
    order_count = Column(Integer, default=0)  # 累计总订单
    success_order_count = Column(Integer, default=0)  # 累计成功订单
    acc_count = Column(Integer, default=0)  # 累计总用户
    success_acc_count = Column(Integer, default=0)  # 累计成功总用户
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间


class UserStatViewTime(MysqlBase):
    __tablename__ = 'stat_view_time'
    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    plat = Column(Integer, default=1)  # 平台 1/金业
    agent_type = Column(Integer)  # 设备来源： 1/PC 2/WAP 3/公众号 4/IOS 5/Android
    channel = Column(String)  # 渠道
    website = Column(String)  # 页面
    total_view_time = Column(Integer, default=0)  # 总浏览时长
    pv_count = Column(Integer, default=0)  # pv
    uv_count = Column(Integer, default=0)  # uv
    stat_date = Column(Integer)  # 添加日期
    stat_hour = Column(Integer)  # 添加小时
    stat_time = Column(Integer)  # 统计时间
    update_time = Column(Integer, default=Func.get_timestamp(), onupdate=Func.get_timestamp())  # 更新时间
    add_time = Column(Integer, default=Func.get_timestamp())  # 添加时间

