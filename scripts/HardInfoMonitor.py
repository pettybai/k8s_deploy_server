# _*_ coding:utf-8_*_
# Author:   Ace Huang
# Time: 2020/7/7 18:05
# File: hard_info.py

import logging
# from typing import Optional, Awaitable

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
import psutil


class HardInfoUtils(object):

    @staticmethod
    def get_hard_info():
        return {
            'CPU': HardInfoUtils.get_cpu_usage(),
            'MEMORY': HardInfoUtils.get_memory_usage(),
            'DISK': HardInfoUtils.get_disk_usage()
        }

    @staticmethod
    def get_cpu_usage():
        """
        获取CPU使用情况
        :return:
        """
        return {
            'total': psutil.cpu_count(),
            'usage': psutil.cpu_percent() / 100.0,
            'detail': psutil.cpu_times_percent(percpu=True),
            'loadavg': psutil.getloadavg()
        }

    @staticmethod
    def get_memory_usage():
        """
        获取Memory使用情况
        :return:
        """
        _memory_obj = psutil.virtual_memory()
        _swap_obj = psutil.swap_memory()
        _memory_info = {
            'total': getattr(_memory_obj, 'total'),
            'used': getattr(_memory_obj, 'used'),
            'free': getattr(_memory_obj, 'free'),
            'available': getattr(_memory_obj, 'available'),
            'percent': getattr(_memory_obj, 'percent') / 100.0,
            'swap_total': getattr(_swap_obj, 'total'),
            'swap_used': getattr(_swap_obj, 'used'),
            'swap_free': getattr(_swap_obj, 'free'),
            'swap_percent': getattr(_swap_obj, 'percent') / 100.0
        }
        return _memory_info

    @staticmethod
    def get_disk_usage():
        """
        获取Disk使用情况
        :return:
        """

        def _convert(_obj):
            return {
                'total': getattr(_obj, 'total'),
                'used': getattr(_obj, 'used'),
                'free': getattr(_obj, 'free'),
                'percent': getattr(_obj, 'percent') / 100.0
            }

        _partitions = psutil.disk_partitions()
        _disk_info = {
            _.mountpoint: _convert(psutil.disk_usage(_.mountpoint))
            for _ in _partitions
        }
        return _disk_info


class HardInfoHandler(tornado.web.RequestHandler):

    # def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
    #     pass

    def get(self):
        _hard_info = HardInfoUtils.get_hard_info()
        self.write(_hard_info)


define("port", default='8000', help='Port number to use for connection')

tornado.options.parse_command_line()


def start_app():
    app = tornado.web.Application(handlers=[
        (r"/", HardInfoHandler)
    ])

    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)
    port = options.port
    http_server.listen(port)
    logging.info("application started on port {}".format(port))
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    start_app()
