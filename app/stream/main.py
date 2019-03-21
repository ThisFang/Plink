# -*- coding:utf-8 -*-

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import time
from org.apache.flink.streaming.api.functions.source import SourceFunction
import json
from app.utils import logger
from app.utils import Func
from app.common import SuperBase
from conf import get_conf


class Generator(SourceFunction, SuperBase):
    """数据源"""
    def __init__(self, boot_conf, data_source):
        super(Generator, self).__init__(boot_conf)
        self._running = True
        self.__data_source = data_source

    def run(self, ctx):
        # 还原上次挂起
        data_source = self.__data_source
        data_source.get_handler()

        # 自动数秒
        wait_seconds = get_conf('base', 'BASE').get('source_wait_second', 10)
        if not self.boot_conf.get('source_topic'):
            raise ValueError('ERROR SOURCE TOPIC')
        try:
            while self._running:
                position = data_source.mount(ctx)
                if position:
                    data_source.set_position(position)
                    # break
                else:
                    time.sleep(wait_seconds)
        except Exception as err:
            logger().error('Generator raise error: {}, job exited'.format(err))
        finally:
            logger().info('Job finished.\r\n')

    def cancel(self):
        self._running = False


class Entry:
    def run(self, flink, args):
        boot_conf = eval(args.boot)
        boot_name = boot_conf.get('name')
        env_parallelism = boot_conf.get('parallelism', 1)

        env = flink.get_execution_environment()

        data_operator = Entry.get_operator_instance(boot_conf)
        data_sink = Entry.get_sink_instance(boot_conf)
        data_source = Entry.get_source_instance(boot_conf)
        data_generator = Generator(boot_conf, data_source)

        data_operator.main(
            boot_name,
            env_parallelism,
            env,
            data_generator,
            data_sink
        )

    @staticmethod
    def get_source_instance(boot_conf):
        """获取source实例"""
        source_driver = boot_conf.get('source_driver')
        module_name = 'app.stream.source.{}'.format(boot_conf.get('source_driver'))
        class_name = Func.str2hump(source_driver.split('.')[-1])
        instance = Func.create_instance(module_name, class_name, boot_conf=boot_conf)
        return instance

    @staticmethod
    def get_operator_instance(boot_conf):
        """获取operator实例"""
        operator = boot_conf.get('module')
        module_name = 'app.stream.operator.{}'.format(operator)
        class_name = Func.str2hump(operator.split('.')[-1])
        instance = Func.create_instance(module_name, class_name, boot_conf=boot_conf)
        return instance

    @staticmethod
    def get_sink_instance(boot_conf):
        """获取sink实例"""
        sink_driver = boot_conf.get('sink_driver')
        module_name = 'app.stream.sink.{}'.format(sink_driver)
        class_name = Func.str2hump(sink_driver.split('.')[-1])
        instance = Func.create_instance(module_name, class_name, boot_conf=boot_conf)
        return instance


def main(flink):
    parser = argparse.ArgumentParser(description='[Fang]PyFlink-Framework')
    parser.add_argument('--boot', metavar='IN', help='get boot conf')
    args = parser.parse_args()
    Entry().run(flink, args)
