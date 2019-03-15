# -- coding: UTF-8 
from app.common import SuperBase


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
            sink.write_by_stream(stream)

        # 3：启动任务

        execute_name = 'Py-Job \'{}\''.format(job_name) if job_name else 'Py-Module \'{}\''.format(self.__module__)

        env.execute(execute_name)
