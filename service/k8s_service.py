# -*- coding: utf-8 -*-

import logging

from utils import get_client
from kubernetes.stream import stream
from kubernetes.client import ExtensionsV1beta1IngressBackend, ExtensionsV1beta1HTTPIngressPath
from utils import K8S_CLUSTER_V1API_VERSION
from utils import K8S_CLUSTER_LOWEST_VERSION
from utils import K8S_SOURCE_TYPE_DEPLOY
from utils import k8s_object_dict
from utils import K8S_OBJ_NODE


class K8sService(object):
    """
    目前采用的API版本均为CoreV1Api，1.17版本deployment的调整暂未支持（已支持）
    """

    def __init__(self, kubeconfig):
        self.client = get_client(kubeconfig)

    def get_cluster_version(self):
        """
        获取集群版本信息，临时方案根据node列表上的第一个node的kubelet版本
        :return:
        """
        api = self.client.CoreV1Api()
        node_list = api.list_node()
        return {'version': node_list.items[0].status.node_info.kubelet_version if node_list.items else None}

    def get_api_obj(self, source_type: str = 'DEPLOYMENT'):
        """
        根据集群版本、资源类型获取k8s api对象
        (针对不同版本的集群版本创建使用的api不同)
        :param source_type:
        :return:
        """
        _version_info = self.get_cluster_version()
        _version = int(_version_info['version'].split('.')[1]) \
            if _version_info['version'] else K8S_CLUSTER_LOWEST_VERSION
        return self.client.AppsV1Api() \
            if _version >= K8S_CLUSTER_V1API_VERSION and source_type == K8S_SOURCE_TYPE_DEPLOY \
            else self.client.ExtensionsV1beta1Api()

    def list_node(self):
        """
        获取集群node信息，含nodeIP、node中image列表、node客户端版本信息等、node节点硬件信息、node上的label
        :return:
        """
        _api = self.client.CoreV1Api()
        node_list = _api.list_node()
        return [
            {
                'name': i.metadata.name,
                'image_list': [_image.names[1] if len(_image.names) == 2 else _image.names[0] for _image in
                               i.status.images],
                'node_info': i.status.node_info.to_dict(),
                'capacity': i.status.capacity,
                'labels': i.metadata.labels
            }
            for i in node_list.items]

    def label_node(self, node: str, label_dict: dict):
        """
        为指定node打标签，label_dict中若存在则覆盖，否则新增（删除label即将该标签设置为None即可）
        :param node:
        :param label_dict:
        :return:
        """
        _api = self.client.CoreV1Api()
        return {'data': k8s_object_dict(_api.patch_node(name=node, body={'metadata': {'labels': label_dict}}))}

    def list_config_map(self, namespace: str = 'default'):
        """
        获取一个命名空间下所有cm信息
        :param namespace:
        :return:
        """
        _api = self.client.CoreV1Api()
        return {'data': k8s_object_dict(_api.list_namespaced_config_map(namespace=namespace))}

    def patch_config_map(self, name: str, cm_key: str, cm_value: object, namespace: str = 'default'):
        """
        修改config_map，若cm_value为空则为删除
        :param name:
        :param cm_key:
        :param cm_value:
        :param namespace:
        :return:
        """
        _api = self.client.CoreV1Api()
        _cm = _api.read_namespaced_config_map(name=name, namespace=namespace)
        for _k, _v in _cm.data.items():
            if _k == cm_key:
                _cm.data[cm_key] = cm_value
                break
        else:
            _cm.data[cm_key] = cm_value
        return k8s_object_dict(_api.patch_namespaced_config_map(name=name, namespace=namespace, body=_cm))

    def update_config_map(self, name: str, config_dict: dict, namespace: str = 'default'):
        """
        修改指定命名空间下指定cm中的值
        删除则将value值设置为None
        :param name:
        :param config_dict:
        :param namespace:
        :return:
        """
        _api = self.client.CoreV1Api()
        return {
            'data': k8s_object_dict(
                _api.patch_namespaced_config_map(
                    name=name,
                    namespace=namespace,
                    body={'data': config_dict})
            )
        }

    def list_namespace(self):
        """
        获取一个集群下的所有命名空间
        :return:
        """
        api = self.client.CoreV1Api()
        ns_obj_list = api.list_namespace()
        return [i.metadata.name for i in ns_obj_list.items]

    def list_pod_for_all_namespaces(self):
        """
        获取一个集群所有命名空间下的pod实例
        :return:
        """
        api = self.client.CoreV1Api()
        pods = api.list_pod_for_all_namespaces()
        return [
            {
                "name": i.metadata.name,
                "namespace": i.metadata.namespace,
                "node_name": i.spec.node_name,
                "host_ip": i.status.host_ip,
                "pod_ip": i.status.pod_ip,
                "ready": i.status.container_statuses[0].ready if i.status.container_statuses else True,
                # 状态
                "phase": i.status.phase,
                "restart_count": i.status.container_statuses[0].restart_count if i.status.container_statuses else 0,
                "start_time": i.status.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "image": i.status.container_statuses[0].image if i.status.container_statuses else None,
            }
            for i in pods.items
        ]

    def list_namespaced_pod(self, namespace='default'):
        """
        根据命名空间获取该命名空间下的pod实例
        :param namespace:
        :return:
        """
        api = self.client.CoreV1Api()
        pods = api.list_namespaced_pod(namespace=namespace)
        return [{
            "name": i.metadata.name,
            "namespace": i.metadata.namespace,
            "node_name": i.spec.node_name,
            "host_ip": i.status.host_ip,
            "pod_ip": i.status.pod_ip,
            "start_time": i.status.start_time.strftime("%Y-%m-%d %H:%M:%S") if i.status.start_time else None,
            "phase": i.status.phase,
            "ready": i.status.container_statuses[0].ready if i.status.container_statuses else True,
            "restart_count": i.status.container_statuses[0].restart_count if i.status.container_statuses else 0,
            "image": i.status.container_statuses[0].image if i.status.container_statuses else None,

        } for i in pods.items]

    def read_namespaced_pod(self, name, namespace='default'):
        """
        获取pod信息
        :param name:
        :param namespace:
        :return:
        """
        api = self.client.CoreV1Api()
        _res = api.read_namespaced_pod(name=name, namespace=namespace)
        return k8s_object_dict(_res) if _res else None

    def read_namespaced_pod_log(self, name, namespace='default', tail_lines=None, since_seconds=None):
        """
        返回指定pod的最近n秒日志
        :param name:
        :param namespace:
        :param tail_lines:
        :param since_seconds: 最近多少秒
        :return: str 日志
        """
        api = self.client.CoreV1Api()
        # 这里限制最大10MB日志
        _params = {
            'name': name,
            'namespace': namespace,
            'since_seconds' if since_seconds else 'tail_lines': since_seconds if since_seconds else tail_lines,
            'limit_bytes': 10485760,
            'pretty': True
        }
        return api.read_namespaced_pod_log(**_params)

    def delete_namespaced_pod(self, name, namespace='default'):
        """
        重启pod
        :param name: pod名称
        :param namespace: 命名空间
        :return: bool 操作结果
        """
        api = self.client.CoreV1Api()
        api.delete_namespaced_pod(name=name, namespace=namespace)
        return True

    def list_daemon_set_for_all_namespaces(self):
        """
        获取一个集群所有命名空间下的daemonset
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        deployments = api.list_daemon_set_for_all_namespaces(pretty=True)
        return [{
            "name": i.metadata.name,
            "namespace": i.metadata.namespace,
            "image": i.spec.template.spec.containers[0].image,
            "desired": i.status.desired_number_scheduled,
            "current": i.status.current_number_scheduled,  # i.status.number_available
            "template": k8s_object_dict(i.spec),
        } for i in deployments.items]

    def list_namespaced_daemon_set(self, namespace='default'):
        """
        获取一个集群指定命名空间下的daemonset
        :param namespace:
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        deployments = api.list_namespaced_daemon_set(namespace=namespace, pretty=True)
        return [
            {
                "name": i.metadata.name,
                "namespace": i.metadata.namespace,
                "image": i.spec.template.spec.containers[0].image,
                "desired": i.status.desired_number_scheduled,
                "current": i.status.current_number_scheduled,  # i.status.number_available
                "template": k8s_object_dict(i.spec),
            }
            for i in deployments.items
        ]

    def read_namespaced_daemon_set(self, name, namespace='default'):
        """
        获取一个集群下指定命名空间下指定名称的daemonset的信息
        :param name:
        :param namespace:
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        _ds = api.read_namespaced_daemon_set(name=name, namespace=namespace)
        return k8s_object_dict(_ds)

    def list_ingress_for_all_namespaces(self):
        """
        获取一个集群下所有命名空间下的ingress
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        ingress_obj = api.list_ingress_for_all_namespaces()
        return [{
            "name": i.metadata.name,
            "namespace": i.metadata.namespace,
            "annotations": i.metadata.annotations,
            "spec": k8s_object_dict(i.spec),
        } for i in ingress_obj.items]

    def list_namespaced_ingress(self, namespace='default'):
        """
        获取指定命名空间下的ingress
        :param namespace:
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        ingress_obj = api.list_namespaced_ingress(namespace=namespace)
        return [{
            "name": i.metadata.name,
            "namespace": i.metadata.namespace,
            "annotations": i.metadata.annotations,
            "spec": k8s_object_dict(i.spec),
        } for i in ingress_obj.items]

    def patch_namespaced_ingress(self, name: str, location: str, service: dict, host: str = None,
                                 namespace: str = 'default'):
        """
        修改ingress转发信息，当service为None时为删除
        :param name:
        :param location:
        :param service: {service_name:hcm-core,service_port:8000}
        :param host:
        :param namespace:
        :return:
        """
        _api = self.client.ExtensionsV1beta1Api()
        _ing = _api.read_namespaced_ingress(name=name, namespace=namespace)
        for _ in _ing.spec.rules:
            if _.host == host:
                for _i, _l in enumerate(_.http.paths):
                    if _l.path == location:
                        if service:
                            _l.backend = ExtensionsV1beta1IngressBackend(**service)
                        else:
                            del _.http.paths[_i]
                        break
                else:
                    service and _.http.paths.append(ExtensionsV1beta1HTTPIngressPath(
                        backend=ExtensionsV1beta1IngressBackend(**service),
                        path=location
                    ))
                _re = _api.patch_namespaced_ingress(name=name, namespace=namespace, body=_ing)
                return k8s_object_dict(_re)
        raise Exception

    def read_namespaced_ingress(self, name, namespace='default'):
        """
        获取指定命名空间下指定ingress信息
        :param name:
        :param namespace:
        :return:
        """
        api = self.client.ExtensionsV1beta1Api()
        i = api.read_namespaced_ingress(name=name, namespace=namespace)
        return {
            "name": i.metadata.name,
            "namespace": i.metadata.namespace,
            "annotations": i.metadata.annotations,
            "spec": k8s_object_dict(i.spec),
        }

    def set_new_version_by_deploy_list(self, deploy_dict: dict, namespace: str = 'default'):
        """
        批量更新deploy镜像
        :param deploy_dict: {deploy_name:image}
        :param namespace:
        :return:
        """
        _api = self.get_api_obj()

        def update_image(_deploy, _image):
            _deploy_obj = _api.read_namespaced_deployment(name=_deploy, namespace=namespace)
            _deploy_obj.spec.template.spec.containers[0].image = _image
            _api.patch_namespaced_deployment(name=_deploy, namespace=namespace, body=_deploy_obj)

        [update_image(_deploy=_, _image=x) for _, x in deploy_dict.items()]
        return {'success': True}

    def set_new_version_by_deploy(self, new_image: str, deploy_name: str, namespace: str = 'default'):
        """
        设置指定命名空间下指定deploy的镜像版本
        :param new_image:
        :param deploy_name:
        :param namespace:
        :return:
        """
        logging.info("set_new_version_by_deploy")
        _api = self.get_api_obj()
        logging.info("get_api_obj")
        _deploy_obj = _api.read_namespaced_deployment(name=deploy_name, namespace=namespace)
        logging.info("read_namespaced_deployment")
        _deploy_obj.spec.template.spec.containers[0].image = new_image
        _api_response = _api.patch_namespaced_deployment(
            name=deploy_name,
            namespace=namespace,
            body=_deploy_obj)
        logging.info("Deployment updated. status='{}'".format(str(_api_response.status)))
        return {'success': True}

    def set_attr_by_deploy(self, attr, attr_value, deploy_name, namespace='default'):
        """
        修改指定命名空间下指定deploy的属性，直接覆盖原属性值，若不存在则新增属性
        :param attr: "spec.template.spec.containers[0].image"
        :param attr_value:
        :param deploy_name:
        :param namespace:
        :return:
        """
        _api = self.get_api_obj()
        deploy_obj = _api.read_namespaced_deployment(name=deploy_name, namespace=namespace)
        _attr_list = attr.split('.')
        _tmp_obj = deploy_obj
        for _attr_item in _attr_list[:-1]:
            if '[' in _attr_item and ']' in _attr_item:
                # 暂时支持一层列表解析
                _tmp_list = _attr_item.split('[')
                _tmp_obj = getattr(_tmp_obj, _tmp_list[0])[int(_tmp_list[1][:-1])]
            else:
                _tmp_obj = getattr(_tmp_obj, _attr_item)
        setattr(_tmp_obj, _attr_list[-1], attr_value)
        _res = _api.patch_namespaced_deployment(name=deploy_name, namespace=namespace, body=deploy_obj)
        logging.info(_res)
        return {'success': True}

    def set_new_version_by_image_name(self, namespace, new_image):
        """
        根据相同的镜像名称 更新对应deployment的镜像版本号
        :param namespace:
        :param new_image:
        :return:
        """
        name, new_version = new_image.split(":")[0], new_image.split(":")[1]
        logging.info("set_new_version_by_image_name")
        _api = self.get_api_obj()
        logging.info("set_new_version_by_image_name_get_api_obj")
        deployments = _api.list_namespaced_deployment(namespace)
        # logging.info(f"set_new_version_by_image_name_list_namespaced_deployment{len(deployments)}")
        for i in deployments.items:
            deployment_name = i.metadata.name
            logging.info(f"deployment_name:{deployment_name}")
            names = i.spec.template.spec.containers[0].image.split(":")
            if len(names) < 2:
                logging.error(f"wrong format:{deployment_name}:{names}")
                continue
            image_name, image_version = names[0], names[1]
            if image_name != name:
                continue
            # Update container image
            i.spec.template.spec.containers[0].image = new_image
            # Update the deployment
            logging.info(f"set_new_version_by_image_name_patch_namespaced_deployment{deployment_name}:{new_image}")
            api_response = _api.patch_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=i)
            logging.info(f"set_new_image:{i}")
            logging.info("Deployment updated. status='{}'".format(str(api_response.status)))

    def list_namespaced_deployment(self, namespace='default'):
        _api = self.get_api_obj()
        deployments = _api.list_namespaced_deployment(namespace=namespace, pretty=True)
        logging.info(f"list_namespaced_deployment:{deployments}")
        return [
            {
                "name": i.metadata.name,
                "namespace": i.metadata.namespace,
                "image": i.spec.template.spec.containers[0].image,
                # 期望的副本数
                "replicas": i.spec.replicas,
                # 期望的副本数
                "desired": i.spec.replicas,
                # 当前的副本数
                "current": i.status.available_replicas,
                # 以下注释不返回
                # "strategy": i.spec.strategy.to_dict(),
                "template": k8s_object_dict(i.spec),
                # "container0": json.dumps(i.spec.template.spec.containers[0].to_dict())
            }
            for i in deployments.items
        ]

    def list_deployment_for_all_namespaces(self):
        _api = self.get_api_obj()
        deployments = _api.list_deployment_for_all_namespaces(pretty=True)
        return [
            {
                "name": i.metadata.name,
                "namespace": i.metadata.namespace,
                "image": i.spec.template.spec.containers[0].image,
                # 期望的副本数
                "replicas": i.spec.replicas,
                # 期望的副本数
                "desired": i.spec.replicas,
                # 当前的副本数
                "current": i.status.available_replicas,
                # 以下注释不返回
                # "strategy": i.spec.strategy.to_dict(),
                # "template": i.spec.template.spec.to_dict(),
                # "container0": json.dumps(i.spec.template.spec.containers[0].to_dict())
            }
            for i in deployments.items
        ]

    def read_namespaced_deployment(self, name, namespace='default'):
        """
        获取指定namespace下的deployment信息
        :param name:
        :param namespace:
        :return:
        """
        _api = self.get_api_obj()
        return k8s_object_dict(_api.read_namespaced_deployment(name=name, namespace=namespace))

    def patch_namespaced_deployment_scale(self, name, namespace, new_replicas):
        """
        partially update scale of the specified Deployment
        :param name:
        :param namespace:
        :param new_replicas:
        :return:
        """
        _api = self.get_api_obj()
        obj = _api.read_namespaced_deployment_scale(name, namespace)
        logging.info("origin deployment replicas:{}".format(obj.spec.replicas))
        obj.spec.replicas = new_replicas
        new_obj = _api.patch_namespaced_deployment_scale(name=name, namespace=namespace, body=obj)
        logging.info("new deployment scale:{}".format(new_obj.spec.replicas))
        return True

    def patch_namespaced_deployment_image(self, name, namespace, new_image):
        """
        partially update the specified Deployment (Recommend!!!)
        :param name:
        :param namespace:
        :param new_image:
        :return:
        """
        return self.set_new_version_by_deploy(new_image=new_image, deploy_name=name, namespace=namespace)

    def replace_namespaced_deployment(self, name, namespace, new_image):
        """
        replace the specified Deployment (Not Recommend!!!)
        :param name:
        :param namespace:
        :param new_image:
        :return:
        """
        _api = self.get_api_obj()
        obj = _api.read_namespaced_deployment(name=name, namespace=namespace)
        logging.info("old image:{}".format(obj.spec.template.spec.containers[0].image))
        obj.spec.template.spec.containers[0].image = new_image
        new_obj = _api.replace_namespaced_deployment(name=name, namespace=namespace, body=obj)
        logging.info("new image:{}".format(new_obj.spec.template.spec.containers[0].image))
        return True

    def exec_command_on_pod(self, command_line, name, namespace='default'):
        """
        在指定pod内执行指定命令
        :param command_line:list
        :param name:
        :param namespace:
        :return:
        """
        api = self.client.CoreV1Api()
        _inst = api.read_namespaced_pod(name=name, namespace=namespace)
        if _inst:
            sh_command = ['/bin/bash']
            _res = stream(api.connect_get_namespaced_pod_exec,
                          name=name,
                          namespace=namespace,
                          command=sh_command,
                          stderr=True, stdin=True,
                          stdout=True, tty=False, _preload_content=False)
            _response_list = []
            while _res.is_open():
                _res.update(timeout=1)
                if _res.peek_stdout():
                    print("STDOUT: %s" % _res.read_stdout())
                if _res.peek_stderr():
                    print("STDERR: %s" % _res.read_stderr())
                if command_line:
                    c = command_line.pop(0)
                    print("Running command... %s\n" % c)
                    _res.write_stdin(c + "\n")
                    _response_list.append({'command': c, 'response': _res.read_stdout()})
                else:
                    break
            return _response_list
        return {'Error': 'Pod not exits!!!'}

    def get_source_status(self, source_type: str = K8S_OBJ_NODE) -> list:
        """
        获取集群node或者pod的CPU、Memory使用情况
        返回结果为list
        :param source_type:
        :return:
        """

        def detail_info(_obj):
            _usage = _obj['usage'] if source_type == K8S_OBJ_NODE else _obj['containers'][0]['usage']
            return {
                'name': _obj['metadata']['name'],
                'ns': _obj['metadata'].setdefault('namespace', 'Node'),
                'CPU': _usage['cpu'],
                'Memory': _usage['memory']
            }

        _api = self.client.CustomObjectsApi()
        _info = _api.list_cluster_custom_object('metrics.k8s.io', 'v1beta1', source_type)
        return [detail_info(_) for _ in _info['items'] if _info and _info.get('items')]
