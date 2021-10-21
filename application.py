# -*- coding: utf-8 -*-
import logging
import os
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from utils import Config, get_mysql_monitor_config, get_mysql_cluster_info
from service.mysql_monitor_service import MysqlMonitorService

define("port", default='8000', help='Port number to use for connection')

tornado.options.parse_command_line()

# 配置初始化
_config_path = os.path.join(os.path.dirname(__file__), 'config')
_config_obj = Config()
os.path.isdir(_config_path) and [_config_obj.append_config(os.path.join(_config_path, _config_name))
                                 for _config_name in os.listdir(_config_path)]

# DB初始化
_db_service = MysqlMonitorService(**get_mysql_monitor_config(_config_obj)) \
    if _config_obj.get_conf(_section='ops', _key='monitor', default=False) else None

_monitor_list = _config_obj.get_conf(_section='ops', _key='monitor_list', default=None).split(',') \
    if _config_obj.get_conf(_section='ops', _key='monitor_list', default=None) else None


def start_app():
    from handlers import handler

    app = tornado.web.Application(handlers=[
        (r"/get_deployments", handler.ListDeploymentHandler),
        # not use for now
        (r"/update_deployments", handler.UpdateDeploymentHandler),
        (r"/set_new_image", handler.SetImageHandler),
        (r"/statistics", handler.StatisticsHandler,
         dict(monitor_list=_monitor_list,
              monitor_port=_config_obj.get_conf(_section='ops', _key='monitor_port', default=8000))),
        (r"/ping", handler.PingHandler),
        (r"/", handler.PingHandler),
        (r"/k8s_manage", handler.K8sManageHandler),
        (r"/db_monitor", handler.MysqlMonitorHandler, dict(db=_db_service)),
        (r"/db_cluster_monitor", handler.MysqlClusterMonitorHandler,
         dict(
             db_cluster=get_mysql_cluster_info(_config_obj)
             if _config_obj.get_conf(_section='ops', _key='mysql_cluster') else None))
    ])

    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)
    port = options.port
    http_server.listen(port)
    logging.info("application started on port {}".format(port))
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    start_app()
