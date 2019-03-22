# -*- coding:utf-8 -*-

import os
import sys
import json
from app.utils import Printer
from conf import get_conf

FLINK_ROOT = '/usr/local/opt/flink-1.6.0'
VENV_PACKAGE_ROOT = 'venv/lib/python3.6/site-packages'
REQUIRE_FILE_ADDRESS = 'requirements.txt'
PROJECT_ROOT = os.getcwd()
APP_ROOT = '{}/app'.format(PROJECT_ROOT)
CONF_ROOT = '{}/conf'.format(PROJECT_ROOT)


def load_packages():
    """
    加载环境包
    :return: 包列表
    """
    packages = [
        APP_ROOT,
        CONF_ROOT
    ]
    require_file = open(REQUIRE_FILE_ADDRESS, 'r')
    ignore_package_list = ['configparser', 'enum34']
    special_package_list = ['enum']
    for line in require_file.readlines():
        package, version = line.strip().split('==')
        if package in ignore_package_list:
            continue
        package = package.replace('-', '_').lower()
        package = '{}/{}/{}'.format(PROJECT_ROOT, VENV_PACKAGE_ROOT, package)
        packages.append(package)

    for special_package in special_package_list:
        package = '{}/{}/{}'.format(PROJECT_ROOT, VENV_PACKAGE_ROOT, special_package)
        packages.append(package)


    Printer.info('+--------------------------------------------------------+')
    for package in packages:
        Printer.info('LOADING PACKAGE: {}'.format(package))
    Printer.info('+--------------------------------------------------------+')
    Printer.empty('')
    return packages


def stream_boot_join(main_file, packages, boot_conf):
    """
    flink job 启动命令
    :param main_file: 项目主入口文件
    :param packages: 需要加载的包
    :param boot_conf: 引导配置
    :return:
    """
    params = '--boot \'{}\''.format(json.dumps(boot_conf))
    cmd = '{}/bin/pyflink-stream.sh {} {} - {}'.format(FLINK_ROOT, main_file, packages, params)
    # 借助nohup后台运行
    cmd = 'nohup {} &'.format(cmd)
    os.system(cmd)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if not os.path.exists(sys.argv[1]):
            exit('error pro dir --%s--' % sys.argv[1])
        file = sys.argv[1]
    else:
        file = 'app/stream/main.py'

    Printer.empty('#############################################################')
    Printer.info('RUNNING \'[FANG]PYFLINK-FRAMEWORK\' BY FANG')
    Printer.empty('#############################################################')

    packages = load_packages()
    package_str = ' '.join(packages)
    boot_conf_list = get_conf('boot', 'BOOT')
    Printer.info('+-------------------------------------------------------------------------------+')
    for boot_conf in boot_conf_list:
        Printer.info('BOOT \'{name}\' USE MODULE \'{module}\' ALREADY SUBMIT'.format(**boot_conf))
        Printer.info('+-------------------------------------------------------------------------------+')
        stream_boot_join(file, package_str, boot_conf)
