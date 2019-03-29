# -- coding: UTF-8
from app.utils import Func, logger
from app.stream.store.cache import RedisStore


class UvCalc:
    def __init__(self, redis_conf='base'):
        try:
            self.__connection = RedisStore(redis_conf).get_connection()
        except Exception as e:
            logger().warning(e)
            self.__connection = None

    def traffic_uv(self, plat, agent_type, date, website, visit_id_list):
        """访问流量uv计算"""
        redis_name = RedisStore.traffic_uv_redis_name(plat, agent_type, date, website)
        return self.__uv_redis_calc(redis_name, visit_id_list)

    def click_uv(self, plat, agent_type, date, click_category, click_action, visit_id_list):
        """点击事件uv计算"""
        redis_name = RedisStore.click_uv_redis_name(plat, agent_type, date, click_category, click_action)
        return self.__uv_redis_calc(redis_name, visit_id_list)

    def push_app_uv(self, plat, agent_type, date, detail_id, visit_id_list):
        """极光推送app点击uv计算"""
        redis_name = RedisStore.push_app_uv_redis_name(plat, agent_type, date, detail_id)
        return self.__uv_redis_calc(redis_name, visit_id_list)

    def __uv_redis_calc(self, redis_name, visit_id_list):
        """uv的redis计算,基于HyperLogLog实现"""
        if self.__connection is None:
            return visit_id_list

        new_visit_id_list = []
        for visit_id in visit_id_list:
            if self.__connection.pfadd(redis_name, visit_id):
                new_visit_id_list.append(visit_id)

        redis_ttl = self.__connection.ttl(redis_name)
        if redis_ttl == -1:
            expire_time = int(Func.get_after_midnight_timestamp(2)) - Func.get_timestamp()
            self.__connection.expire(redis_name, expire_time)

        return new_visit_id_list
