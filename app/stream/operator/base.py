# -- coding: UTF-8
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector
from app.common import SuperBase
from app.common.base import ALLOW_TOPIC_DICT, ALLOW_TOPIC
import json
from app.utils import logger, LogName
from app.common.request import CurlToGateway


class OperatorBase(SuperBase):
    def __init__(self, boot_conf):
        SuperBase.__init__(self, boot_conf)

    def get_stream(self, stream):
        """
        流逻辑，子类必须实现
        """
        raise NotImplementedError

    def main(self, job_name, env_parallelism, env, source, sink):
        """
        主运行函数，分析入口，*不可缺失
        """
        # 1：设置并发度等运行参数（TaskManager * TaskSlots = Max Parallelism）
        env.set_parallelism(env_parallelism)

        # env对象可以完成流式计算的功能。包括
        # 2：接入逻辑流
        stream = env.create_python_source(source)

        stream = self.get_stream(stream)

        if not sink:
            stream.output()
        else:
            sink.write_by_stream(stream)

        # 3：启动任务

        execute_name = 'Py-Job \'{}\''.format(job_name) if job_name else 'Py-Module \'{}\''.format(self.__module__)

        env.execute(execute_name)


class NormalInitFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        """
        获取redis源头中的参数,normal初始化解析
        """
        method, ip, args, gateway_uri, receive_time = value
        args = json.loads(args)
        try:
            data = args.get('data')
        except Exception as e:
            logger(LogName.TEST).info(args)
            return

        if type(data).__name__ != 'list':
            logger(LogName.ORIGIN).warning('cannot get data {}'.format(args))
            return
        topic = args.get('topic')
        if topic not in ALLOW_TOPIC:
            logger(LogName.ORIGIN).warning('error topic {}'.format(args))
            return

        # 过长做切片处理
        piece_len = 10
        for piece in range(0, len(data), piece_len):
            data_piece = data[piece:piece+piece_len]
            collector.collect((topic, ip, json.dumps(data_piece)))


class CountlyInitFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        """
        获取redis源头中的参数,countly初始化解析
        """
        method, ip, args, gateway_uri, receive_time = value
        args = json.loads(args)

        try:
            events = json.loads(args.get('events'))
        except Exception:
            return

        for event in events:
            segmentation = event.get('segmentation')
            topic = segmentation.get('topic')
            if topic not in ALLOW_TOPIC:
                continue
            data = segmentation.get('data')
            if not data:
                continue
            try:
                data = json.loads(data)
            except Exception:
                continue
            collector.collect((topic, ip, json.dumps(data)))


class CountlyInitToGatewayFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        """
        获取redis源头中的参数,countly初始化解析
        """
        method, ip, args, gateway_uri, receive_time = value
        args = json.loads(args)

        try:
            events = json.loads(args.get('events'))
        except Exception:
            return

        for event in events:
            segmentation = event.get('segmentation')
            topic = segmentation.get('topic')
            if topic not in ALLOW_TOPIC:
                continue
            data = segmentation.get('data')
            if not data:
                continue
            uri = str(ALLOW_TOPIC_DICT.get(topic))
            # 尊享版相关需求收集发送给base
            if topic in ['app_open', 'vip_open', 'app_vip_open']:
                if topic == 'app_vip_open':
                    topic = 'vip_open'
                uri = '/base'
                try:
                    data = json.loads(data)
                except Exception:
                    continue
                send_data = {
                    'event_file': topic,
                    'sign': segmentation.get('sign'),
                    'datalist': data
                }
                CurlToGateway(uri, json=send_data).curl()
                continue
            collector.collect((uri, topic, ip, data))


class TopicSelector(OutputSelector):
    def select(self, value):
        topic, ip, data = value
        return [str(topic)]
