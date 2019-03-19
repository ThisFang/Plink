# -- coding: UTF-8


import time
from operator import itemgetter
from app.stream.store.cache import RedisStore


class ViewTimeAnalysis:
    def __init__(self):
        self.view_time_ck_detail = None
        self.redis_conn = RedisStore().get_connection()

    def analysis(self, plat, agent_type, channel, visit_id, website, req_time):
        """
        分析浏览时间
        :param plat:
        :param agent_type:
        :param channel:
        :param visit_id:
        :param website:
        :param req_time:
        :return:
        """
        redis_name = self.__redis_name(plat, agent_type, visit_id, req_time)
        self.plat = plat
        self.agent_type = agent_type
        self.visit_id = visit_id
        self.channel = channel
        website = website.encode('utf-8')
        self.__get_view_time_detail(redis_name, website, req_time)

        self.__set_redis_expire_time(redis_name)
        return self.view_time_ck_detail

    def __get_view_time_detail(self, redis_name, website, req_time):
        """
        获取浏览时间
        :param redis_name:
        :param website:
        :param req_time:
        :return:
        """
        view_redis_key_list = self.redis_conn.hkeys(redis_name)
        if not view_redis_key_list:
            self.redis_conn.hset(
                redis_name,
                ViewTimeAnalysis.__combine_redis_hash_key(website, req_time),
                ViewTimeAnalysis.__combine_redis_hash_value(req_time, req_time)
            )
            return
        # 拆分并排序hash
        view_redis_key_list = [self.__split_redis_hash_key(key) for key in view_redis_key_list]
        # view_redis_key_list = [(key[1], int(key[0])) for key in view_redis_key_list]
        view_redis_key_list = sorted(view_redis_key_list, key=itemgetter(0))

        # 获取最新访问记录
        last_view = view_redis_key_list[-1]

        # 如果本次访问不是最新访问(乱序了)
        if req_time < last_view[0]:
            last_view = self.__get_nearly_same_website_view(view_redis_key_list, website, req_time)
            if not last_view:
                self.redis_conn.hset(
                    redis_name,
                    self.__combine_redis_hash_key(website, req_time),
                    self.__combine_redis_hash_value(req_time, req_time)
                )
                return

            self.__update_before_view_redis(
                redis_name, last_view[1], last_view[0], req_time
            )
            return

        # 新页面访问
        if website != last_view[1]:
            self.redis_conn.hset(
                redis_name,
                ViewTimeAnalysis.__combine_redis_hash_key(website, req_time),
                ViewTimeAnalysis.__combine_redis_hash_value(req_time, req_time)
            )

        self.__update_before_view_redis(
            redis_name, last_view[1], last_view[0], req_time
        )

    def __update_before_view_redis(self, redis_name, website, sign, req_time):
        """
        更新redis里的最近一次访问
        :param redis_name:
        :param website:
        :param sign:
        :param req_time:
        :return:
        """
        view_key = ViewTimeAnalysis.__combine_redis_hash_key(website, sign)
        view_value = self.redis_conn.hget(redis_name, view_key)
        view_value = eval(view_value)

        # 在起始结束时间中间不处理
        delta_start_time = view_value.get('st') - req_time
        delta_end_time = req_time - view_value.get('et')

        if delta_start_time > 0:
            delta_time = delta_start_time
            view_value['st'] = req_time
        elif delta_end_time * delta_start_time >= 0:
            # delta_time = None
            return
        else:
            delta_time = delta_end_time
            view_value['et'] = req_time

        delta_time = abs(delta_time)

        self.redis_conn.hset(
            redis_name,
            ViewTimeAnalysis.__combine_redis_hash_key(website, sign),
            view_value
        )
        self.view_time_ck_detail = self.__combine_view_time_ck(
            website.decode('utf-8'),
            sign,
            delta_time
        )

    def __set_redis_expire_time(self, redis_name):
        """
        操作完后如果是新创建的则设置过期时间
        :param redis_name:
        :return:
        """
        if self.redis_conn.ttl(redis_name) < 0:
            self.redis_conn.expire(redis_name, 3600)

    @staticmethod
    def __get_nearly_same_website_view(view_redis_key_list, website, req_time):
        same_website_view_redis_list = list(filter(lambda x: x[1] == website, view_redis_key_list))

        if not same_website_view_redis_list:
            return None

        # 排序获取最相近的一次访问
        near_time = float('inf')
        nearly_same_website_view = (0, '')
        for same_website_view in same_website_view_redis_list:
            if req_time <= same_website_view[0]:
                if same_website_view[0] - req_time < near_time:
                    same_website_view[0] - req_time
                    nearly_same_website_view = same_website_view
                break

            nearly_same_website_view = same_website_view
            near_time = req_time - same_website_view[0]

        return nearly_same_website_view

    @staticmethod
    def __redis_name(plat, agent_type, visit_id, req_time):
        """
        获取访问时长redis的名字
        :param plat:
        :param agent_type:
        :param visit_id:
        :param req_time:
        :return:
        """
        half_hour_time = ViewTimeAnalysis.__get_nearly_half_hour(req_time)
        time_start = time.strftime('%Y%m%d_%H_%M', time.localtime(half_hour_time))
        redis_name = 'mplus:view_time:{}_{}_{}:{}'.format(plat, agent_type, visit_id, time_start)
        return redis_name

    @staticmethod
    def __get_nearly_half_hour(req_time):
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

    @staticmethod
    def __combine_redis_hash_key(website, sign):
        """
        组合redis hash的键值对
        :param website:
        :param sign:
        :return:
        """
        key = '{}_{}'.format(sign, website)
        return key

    @staticmethod
    def __split_redis_hash_key(key):
        try:
            sign, website = key.split('_', 1)
        except Exception:
            sign, website = bytes.decode(key).split('_', 1)

        return int(sign), website

    @staticmethod
    def __combine_redis_hash_value(start_time, end_time):
        """
        组合redis hash的键值对
        :param start_time:
        :param end_time:
        :return:
        """
        value = {
            'st': start_time,
            'et': end_time
        }
        return value

    def __combine_view_time_ck(self, website, sign, view_time):
        """
        组装插入到ck的数据
        :param website:
        :param sign:
        :param view_time:
        :return:
        """
        primary_dict = {
            'plat': self.plat,
            'agent_type': self.agent_type,
            'channel': self.channel or '(not set)',
            'visit_id': self.visit_id,
            'website': website,
            'sign': sign,
            'view_time': view_time
        }
        return primary_dict
