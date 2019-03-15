# -*- coding:utf-8 -*-


import os
import sys
import logbook
from logbook import Logger, TimedRotatingFileHandler
from conf import get_conf

logger_conf = get_conf('base', 'LOGGER')


# 日志存放路径
# LOG_DIR = os.path.join("Log")
LOG_DIR = logger_conf.get('path')


if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


def log_type(record, handler):
    log = logger_conf.get('format').format(
        date=record.time,  # 日志时间
        level=record.level_name,  # 日志等级
        filename=os.path.split(record.filename)[-1],  # 文件名
        func_name=record.func_name,  # 函数名
        lineno=record.lineno,  # 行号
        module=record.module,  # 行号
        msg=record.message  # 日志内容
    )
    return log


def logger(file_name='default'):
    # 日志打印到文件
    log_file = TimedRotatingFileHandler(
        os.path.join(LOG_DIR, '%s.log' % file_name),
        date_format='%Y-%m-%d',
        bubble=True,
        rollover_format='{basename}_{timestamp}{ext}',
        encoding='utf-8',
    )
    log_file.formatter = log_type

    logbook.set_datetime_format('local')
    run_log = Logger('script_log')
    run_log.handlers = []
    run_log.handlers.append(log_file)
    return run_log
