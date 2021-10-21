# -*- coding: utf-8 -*-

from handlers.base import BaseHandler
import logging
from service.k8s_service import K8sService
from service.node_service import NodeService


class PingHandler(BaseHandler):
    def get(self):
        self.write("Hello,i'm living")

    def post(self):
        self.write("Hello,i'm living")


class UpdateDeploymentHandler(BaseHandler):

    async def post(self):
        config = getattr(self, 'params').get("config")
        name = getattr(self, 'params').get("deployment_name")
        namespace = getattr(self, 'params').get("namespace")
        new_image = getattr(self, 'params').get("new_image")
        if not all([config, name, namespace, new_image]):
            self.write({"success": False, "data": "", "msg": "incomplete arguments"})
        data = K8sService(kubeconfig=config).patch_namespaced_deployment_image(
            name=name, namespace=namespace, new_image=new_image)
        logging.info("result: {}".format(data))
        self.write({"success": True, "data": data})


class SetImageHandler(BaseHandler):

    async def post(self):
        """更新指定命名空间的指定deployments"""

        config = getattr(self, 'params').get('config')
        namespace = getattr(self, 'params').get('namespace')
        new_image = getattr(self, 'params').get('new_image')
        if not all([config, namespace, new_image]):
            self.write({"success": False, "data": "", "msg": "incomplete arguments"})
        data = K8sService(kubeconfig=config).set_new_version_by_image_name(namespace=namespace, new_image=new_image)
        self.write({"success": True, "data": data})


class ListDeploymentHandler(BaseHandler):

    async def post(self):
        config = getattr(self, 'params').get("config")
        namespace = getattr(self, 'params').get("namespace", None)
        client = K8sService(kubeconfig=config)
        if namespace:
            data = client.list_namespaced_deployment(namespace)
        else:
            data = client.list_deployment_for_all_namespaces()
        logging.info("result: {}".format(data))
        self.write({"success": True, "data": data})


class StatisticsHandler(BaseHandler):

    async def post(self):
        if hasattr(self, 'monitor_list') and getattr(self, 'monitor_list'):
            self.write(
                {
                    'success': True,
                    'data': NodeService(getattr(self, 'params').get('config')).get_node_hard_usage(
                        monitor_list=getattr(self, 'monitor_list'),
                        monitor_port=getattr(self, 'monitor_port') if hasattr(self, 'monitor_port') else 8000
                    )
                })
        else:
            self.write({"success": True, "data": NodeService(getattr(self, 'params').get('config')).get_node_info()})


class K8sManageHandler(BaseHandler):

    async def post(self):
        config = getattr(self, 'params').get("config")
        name = getattr(self, 'params').get("function_name")
        params = getattr(self, 'params').get("function_params")
        logging.info("function: {} params: {}".format(name, params))

        if not all([config, name]):
            self.write({"success": False, "data": "", "msg": "incomplete arguments"})

        func = getattr(K8sService(kubeconfig=config), name)
        data = func(**params)
        logging.info("result: {}".format(data))
        self.write({"success": True, "data": data})


class MysqlMonitorHandler(BaseHandler):

    async def post(self):
        """
        处理逻辑：db在BaseHandler的初始化中就注入、需调用方法、调用参数
        Handler作为一个分发器
        :return:
        """
        _func = getattr(getattr(self, 'db'), getattr(self, 'params').get('function_name'))
        self.write(_func(**getattr(self, 'params').get('function_params')))


class MysqlClusterMonitorHandler(BaseHandler):
    """
    完成MysqlCluster、Mysql主从的存活监控及主从同步监控
    """

    async def get(self):
        if getattr(self, 'db_cluster'):
            from service.mysql_monitor_service import MysqlMonitorService
            _db_cluster = getattr(self, 'db_cluster')
            _db_info = {
                'success': [],
                'error': []
            }
            for _ in _db_cluster['mysql_node_list']:
                try:
                    _db_cli = MysqlMonitorService(
                        host=_[0],
                        port=int(_[1]),
                        user_name=_db_cluster['mysql_cluster_user'],
                        password=_db_cluster['mysql_cluster_password'],
                        db_name='mysql'
                    )
                    _syn_info = _db_cli.execute_sql('show slave status;')
                    if _syn_info['data'] and _syn_info['data'][0]['Slave_IO_Running'] \
                            and _syn_info['data'][0]['Slave_SQL_Running'] \
                            and _syn_info['data'][0]['Last_Errno'] == 0 \
                            and _syn_info['data'][0]['Last_SQL_Errno'] == 0:
                        _db_info['success'].append(f'{_[0]}:{_[1]}')

                    logging.info(_syn_info)
                except Exception as e:
                    logging.error(f'{_[0]}:{_[1]}DB connect error!!!')
                    _db_info['error'].append(f'{_[0]}{_[1]}@{e}')
                else:
                    del _db_cli
            self.write(_db_info)
        else:
            self.write({'success': [], 'error': []})
