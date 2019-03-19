# -- coding: UTF-8

# Clickhouse
CK_NULL_DATETIME = '1970-01-01 00:00:00'
CK_NULL_DATE = '1970-01-01'
CK_NULL_ACT_DATETIME = '1970-01-01 06:28:16'

# Topic Name
TOPIC_L1_DEPOSIT = 'deposit'
TOPIC_L1_AGGPAY = 'agg_pay'
TOPIC_L2_NEED_STATUS = 'need_status'
TOPIC_L2_OTHER_STATUS = 'other_status'

# 支付渠道
PAY_SOURCE_POLY = '4'  # 聚合支付
PAY_SOURCE_ANT = '6'  # 蚂蚁钱包
PAY_SOURCE_QUICK = '7'  # 快速转账
PAY_SOURCE_EUPAY = '8'  # EuPay
PAY_SOURCE_MANUAL = '3'  # 银行汇款


# 渠道-编号对应配置
PAY_SOURCE_FLAG_CONFIG = {
        '[\'800001\', \'800004\']': 4,   # 聚合支付
        '[\'800032\']': 6,             # 蚂蚁钱包
        '[\'800128\']': 8,             # 银联转账
        '[\'800256\']': 3,             # 手工转账/银行电汇
        '[\'800064\']': 7,             # EuPay充值
    }
