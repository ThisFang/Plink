# -*- coding:utf-8 -*-

from conf import base
from conf import database
from conf import boot


def get_conf(conf_name, section):
    """
    获取配置
    :param conf_name: 配置文件
    :param section: 配置块
    :return:
    """
    conf_module = globals()[conf_name]
    section_value = getattr(conf_module, section)
    return section_value
