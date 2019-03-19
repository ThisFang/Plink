# -- coding: UTF-8 
from app.common import SuperBase
from app.common.base import ALLOW_TOPIC_DICT, ALLOW_TOPIC
import json
from app.utils import logger
from app.common.request import CurlToGateway
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction, FilterFunction, MapFunction
from org.apache.flink.streaming.api.collector.selector import OutputSelector


log_prefix = 'origin'


class OperatorBase(SuperBase):
    def __init__(self, boot_conf):
        super(OperatorBase, self).__init__(boot_conf)

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
            sink.write_by_stream(stream).output()

        # 3：启动任务

        execute_name = 'Py-Job \'{}\''.format(job_name) if job_name else 'Py-Module \'{}\''.format(self.__module__)

        env.execute(execute_name)


class NormalInitFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        """
        获取redis源头中的参数,normal初始化解析
        """
        try:
            value_dict = json.loads(value)
        except Exception:
            return

        method = value_dict.get('Method')
        if method != 'POST':
            logger(log_prefix).warning('error method {}'.format(value))
            return
        ip = value_dict.get('Headers').get('host')
        args = value_dict.get('Args')

        if type(args).__name__ != 'dict':
            try:
                args = json.loads(args)
            except Exception:
                return
        data = args.get('data')

        if type(data).__name__ != 'list':
            logger(log_prefix).warning('cannot get data {}'.format(value))
            return
        topic = args.get('topic')
        if not topic:
            logger(log_prefix).warning('cannot get topic {}'.format(value))
            return
        if topic not in ALLOW_TOPIC:
            logger(log_prefix).warning('error topic {}'.format(value))
            return

        logger(log_prefix).info(len(str(args)))

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
        try:
            value_dict = json.loads(value)
        except Exception:
            return

        method = value_dict.get('Method')
        if method != 'POST':
            return
        ip = value_dict.get('Headers').get('host')
        args = value_dict.get('Args')

        events = args.get('events')
        if not events:
            return
        try:
            events = json.loads(events)
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
                data = json.loads(data.decode('utf-8'))
            except Exception:
                continue
            logger(log_prefix).info(len(json.dumps(args)))
            collector.collect((topic, ip, json.dumps(data)))


class CountlyInitToGatewayFlatMap(FlatMapFunction):
    def flatMap(self, value, collector):
        """
        获取redis源头中的参数,countly初始化解析
        """
        try:
            value_dict = json.loads(value)
        except Exception:
            return

        method = value_dict.get('Method')
        if method != 'POST':
            return
        ip = value_dict.get('Headers').get('host')
        args = value_dict.get('Args')

        events = args.get('events')
        if not events:
            return
        try:
            events = json.loads(events)
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
                data = json.loads(data.decode('utf-8'))
            except Exception:
                continue
            uri = str(ALLOW_TOPIC_DICT.get(topic))

            # 尊享版相关需求收集发送给base
            if topic in ['app_open', 'vip_open', 'app_vip_open']:
                if topic == 'app_vip_open':
                    topic = 'vip_open'
                uri = '/base'
                send_data = {
                    'event_file': topic,
                    'sign': segmentation.get('sign'),
                    'datalist': data
                }
                CurlToGateway(uri, json=send_data).curl()
                continue
            collector.collect((uri, topic, ip, json.dumps(data)))


class TopicSelector(OutputSelector):
    def select(self, value):
        topic, ip, data = value
        return [str(topic)]
