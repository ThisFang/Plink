# -- coding: UTF-8
# import requests
import json

try:
    import urllib2 as urlRequest
except Exception:
    import urllib.request as urlRequest


class Invoke:
    def __init__(self, http_method, url, data=None, **kwargs):
        self._http_method = http_method
        self._url = url
        self._data = data
        self._params = kwargs
        self._headers = self.build_headers()

    def call_api(self):
        """ api 接口调用 """
        payload = self._data

        req = urlRequest.Request(self._url, payload, self._headers)
        req.get_method = lambda: self._http_method
        try:
            f = urlRequest.urlopen(req)
            _response = f.read()
            f.close()
        except Exception as e:
            return False

        # if self._http_method in ['POST', 'PUT']:
        #
        #     _response = requests.request(self._http_method, self._url, data=payload, headers=self._headers)
        # else:
        #     _response = requests.request(self._http_method, self._url, headers=self._headers)
        #
        # res = self.result_parse(_response.text)

        res = self.result_parse(_response)
        if not res:
            self.failed_handle()  # TODO
        return res

    def build_headers(self):
        headers = dict()
        if self._http_method not in ['GET']:
            headers['Content-Type'] = self.content_type
        headers['Cache-Control'] = self.cache_control
        return headers

    def is_exists(self, key, target_source):
        """ 判断是否存在key """
        if not isinstance(target_source, dict):
            return False
        
        if key in target_source:
            return True
        else:
            return False

    def get_value(self, key):
        """ 取key的值 """
        source = self._params
        if self.is_exists(key, source):
            return source[key]
        else:
            return None

    @property
    def content_type(self):
        return self.get_value('content_type') or 'application/json'

    @property
    def cache_control(self):
        return self.get_value('cache_control') or 'no-cache'

    def result_parse(self, result_text):
        """ 请求结果解析 """
        try:
            value = json.loads(result_text)
        except BaseException as err:
            return {}
        else:
            return value

    def failed_handle(self):
        """ 调用api失败结果处理 """
        pass
