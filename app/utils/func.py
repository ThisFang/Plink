# -- coding: UTF-8

import time
import datetime
import json
import calendar
import uuid
import sys

if sys.version_info[0] == 2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse


class Func:
    @staticmethod
    def get_timestamp(arg_timestamp=0):
        """
        当前时间戳
        :param arg_timestamp: 时间戳
        :return: int
        """
        if not arg_timestamp:
            arg_timestamp = time.time()
        return int(arg_timestamp)

    @staticmethod
    def get_milliseconds(arg_timestamp=0):
        """
        毫秒时间戳
        :param arg_timestamp: 时间戳
        :return: int
        """
        if not arg_timestamp:
            arg_timestamp = time.time()
        return int(round(arg_timestamp * 1000))

    @staticmethod
    def timedelta(start_time, end_time, time_format='%Y-%m-%d', return_stamp=False):
        """
        时间相差
        传入时间支持到天
        :param start_time:起始时间
        :param end_time:结束时间
        :param time_format:传入和输出的时间格式 默认%Y-%m-%d
        :param return_stamp:是否返回时间戳 默认否
        :return:
        """
        start = datetime.datetime.strptime(start_time, time_format)
        end = datetime.datetime.strptime(end_time, time_format)

        delta = end - start
        days = delta.days

        for_range = range(days + 1 if days else 24)
        date_array = []
        for x in for_range:
            date_obj = start + (datetime.timedelta(days=x) if days else datetime.timedelta(hours=x))
            if return_stamp:
                x_time = int(date_obj.timestamp())
            else:
                x_time = date_obj.strftime(time_format if days else time_format + ' %H')

            date_array.append(x_time)

        return date_array

    @staticmethod
    def half_month_date(date=datetime.datetime.now().strftime('%Y-%m-%d')):
        """
        获取半月起始和结束日期
        :param date:
        :return:
        """
        date = datetime.datetime.strptime(date, '%Y-%m-%d')

        if date.day <= 15:
            start_day = 1
            end_day = 15
        else:
            start_day = 16
            _, end_day = calendar.monthrange(date.year, date.month)

        start_date = date.strftime('%Y-%m-{}'.format(start_day))
        end_date = date.strftime('%Y-%m-{}'.format(end_day))
        return start_date, end_date

    @staticmethod
    def get_date(x_time=0, time_format='%Y-%m-%d'):
        """
        获取格式化时间
        :param x_time:
        :param time_format:
        :return:
        """
        if not x_time:
            x_time = Func.get_timestamp()
        return time.strftime(time_format, time.localtime(x_time))

    @staticmethod
    def get_after_midnight_timestamp(after_day=1):
        """
        获取after_day天后0点时间戳
        :param after_day:
        :return:
        """
        cur_time = time.time()
        expire_time = 86400 * after_day - (cur_time % 86400 + 60 * 60 * 8)
        return cur_time + expire_time

    @staticmethod
    def is_json(value):
        """
        是否json判断
        :param value:
        :return:
        """
        try:
            json.loads(value)
        except Exception:
            return False
        return True

    @staticmethod
    def create_instance(module_name, class_name, *args, **kwargs):
        """
        动态实例化类
        :param module_name:
        :param class_name:
        :param args:
        :param kwargs:
        :return:
        """
        module_meta = __import__(module_name, globals(), locals(), [class_name])
        class_meta = getattr(module_meta, class_name)
        obj = class_meta(*args, **kwargs)
        return obj

    @staticmethod
    def url_parse(url):
        """
        解析url
        :param url:
        :return: ParseResult类
        """
        parse_res = urlparse(url)
        return parse_res

    @staticmethod
    def str2hump(text):
        arr = filter(None, text.lower().split('_'))
        res = ''
        for i in arr:
            res = res + i[0].upper() + i[1:]
        return res

    @staticmethod
    def get_nearly_half_hour(req_time):
        """
        获取临近的半小时时间戳
        :param req_time:时间戳
        :return:
        """
        req_time_array = time.localtime(req_time)
        min_def = req_time_array.tm_min % 30
        sec_def = req_time_array.tm_sec

        half_hour_time = req_time - min_def * 60 - sec_def
        return half_hour_time

