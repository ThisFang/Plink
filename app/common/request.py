# -- coding: UTF-8
import requests
from conf import get_conf
import json
from app.utils import logger

log_prefix = 'curl'


class RequestBase(object):
    def __init__(self, http_method, url, **kwargs):
        self.__retry_times = 1
        self.__retry_max = 3
        self.res = None
        self.reset_retry()

        self._http_method = http_method
        self._url = url
        self._kwargs = kwargs

    def get_response(self):
        res = requests.request(
            self._http_method,
            self._url,
            **self._kwargs
        )
        self.res = res
        return res

    def curl(self):
        """发送"""
        self.get_response()
        # 如果失败则重试__retry_max次
        if not self.is_success():
            if self.inc_retry():
                return self.curl()
            else:
                return self.fail()
        else:
            return self.success()

    def success(self):
        """成功处理"""
        raise NotImplementedError

    def fail(self):
        """失败处理"""
        raise NotImplementedError

    def is_success(self):
        """是否成功"""
        raise NotImplementedError

    def reset_retry(self):
        """重置重试次数"""
        self.__retry_times = 1

    def get_retry(self):
        """重置重试次数"""
        return self.__retry_times

    def inc_retry(self):
        """自增重试次数"""
        if self.__retry_times < self.__retry_max:
            self.__retry_times += 1
            return True
        else:
            return False

    def get_text_res(self):
        """获取结果文本"""
        return self.res.text

    def get_dict_res(self):
        """获取结果字典"""
        return json.loads(self.res.text)


class CurlToGateway(RequestBase):
    def __init__(self, uri, **kwargs):
        self.res = None
        self.url = self.__combine_url(uri)
        super(CurlToGateway, self).__init__('POST', self.url, **kwargs)

    @staticmethod
    def __combine_url(uri):
        host = get_conf('base', 'MPLUS_URL').get('gateway_domain')
        url = '{}{}'.format(host, uri)
        return url

    def is_success(self):
        """是否需要重试"""
        self.res = self.get_dict_res()
        if type(self.res).__name__ != 'dict':
            return False

        if self.res.get('success'):
            return True
        else:
            return False

    def fail(self):
        """失败回调"""
        logger(log_prefix).notice('[GATEWAY_CURL] [FAIL] {} {} {}'.format(self._url, self._kwargs, self.res))
        return self.res

    def success(self):
        """成功回调"""
        # logger(log_prefix).notice('[GATEWAY_CURL] [SUCCESS] {} {} {}'.format(self._url, self._kwargs, self.res))
        return self.res


class CurlToAnalysis(RequestBase):
    def __init__(self, sys, uri='', method='POST', **kwargs):
        self.res = None
        self.url = self.__combine_url(sys, uri)
        self.kwargs = kwargs
        super(CurlToAnalysis, self).__init__(method, self.url, **kwargs)

    @staticmethod
    def __combine_url(sys, uri):
        host = get_conf('base', 'MPLUS_URL').get(sys)
        url = '{}{}'.format(host, uri)
        return url

    def is_success(self):
        """是否需要重试"""
        self.res = self.get_dict_res()
        if type(self.res).__name__ != 'dict':
            return False

        if self.res.get('res'):
            return True
        else:
            return False

    def fail(self):
        """失败回调"""
        logger(log_prefix).notice('[CURL] [FAIL] {} {} {}'.format(self._url, self._kwargs, self.res))
        self.reset_retry()
        return self.res

    def success(self):
        """成功回调"""
        logger(log_prefix).notice('[CURL] [SUCCESS] {} {} {}'.format(self._url, self._kwargs, self.res))
        self.reset_retry()
        return self.res
