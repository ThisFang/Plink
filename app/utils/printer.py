# -*- coding:utf-8 -*-


class Printer:
    @staticmethod
    def empty(msg):
        print('{}'.format(msg))

    @staticmethod
    def critical(msg):
        print('[CRITICAL]{}'.format(msg))

    @staticmethod
    def error(msg):
        print('[ERROR]{}'.format(msg))

    @staticmethod
    def info(msg):
        print('[INFO]{}'.format(msg))

    @staticmethod
    def debug(msg):
        print('[DEBUG]{}'.format(msg))

    @staticmethod
    def warning(msg):
        print('[WARNING]{}'.format(msg))
