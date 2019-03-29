# -- coding: UTF-8 
from app.common import SuperBase


class SourceBase(SuperBase):
    def __init__(self, boot_conf):
        SuperBase.__init__(self, boot_conf)

    def get_handler(self):
        """获取链接实例或处理器实例"""
        raise NotImplementedError

    def set_position(self, position):
        """用于标记最后信息的读取位置"""
        raise NotImplementedError

    def get_position(self):
        """用于获取最后信息的读取位置"""
        raise NotImplementedError

    def restore_hung_up(self, ctx):
        """恢复上次中断时异常挂起的作业"""
        raise NotImplementedError

    def mount(self, ctx):
        """
        将缓冲区的内容装载至指定位置
        外围项目启动通过当前环境的解释器去运行脚本程式，否则会出现找不到现有项目模块的问题
        """
        raise NotImplementedError
