# -- coding: UTF-8


class PayStatusEnum:
    """ 支付状态 """
    
    # 待支付
    pending = 1
    # 支付成功
    success = 2
    # 支付失败
    fail = 3


class DepositTypeEnum:
    """ 出入金枚举类型 """
    
    # 入金
    incoming = 1
    # 出金
    withdraw = 2


class IsFirstEnum:
    """ 是否首入金类型 """
    # 默认值
    default = 0
    # 首入
    yes = 1
    # 非首入
    no = 2


class TargetTypeEnum:
    # 访问
    v = 1
    # 客户信息获取
    nl = 2
    # 模拟开户
    d = 3
    # 真实开户
    n = 4
    # 首次入金
    a = 5
