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


class TargetEnum:
    # 访问
    V = 1
    # 客户信息获取
    NL = 2
    # 模拟开户
    D = 3
    # 真实开户
    N = 4
    # 首次入金
    A = 5
