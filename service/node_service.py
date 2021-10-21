# -*- coding: utf-8 -*-

import logging
import requests
from utils import get_client, get_timestamp


class NodeService(object):
    def __init__(self, kubeconfig):
        self.client = get_client(kubeconfig=kubeconfig)
        self.api = self.client.CoreV1Api()

    def get_node_hard_usage(self, monitor_list: list = None, monitor_port: int = 8000):
        """
        获取所有节点硬件资源使用信息
        monitor会和list_node合并拿到所有节点
        :param monitor_list: Mysql、NFS节点
        :param monitor_port: 默认所有节点都在宿主机的8000端口启动服务
        :return:
        """
        _node_list = self.api.list_node()
        _node_list = [[_.address for _ in x.status.addresses if _.type == 'InternalIP'][0] for x in _node_list.items]
        _node_list = list(set(_node_list + monitor_list))
        _usage_list = []
        for _ in _node_list:
            try:
                _url = f'http://{_}:{monitor_port}/'
                _response = requests.request(method='GET', url=_url, timeout=5)
                _data = _response.json()
                _node_usage = {
                    "node_ip": _,
                    "cpu": {
                        "percentage": _data['CPU']['usage']
                    },
                    "memory": {
                        "total": _data['MEMORY']['total'],
                        "used": _data['MEMORY']['used'],
                        "percentage": _data['MEMORY']['percent']
                    },
                    "disk": {
                        "name": '+'.join(_data['DISK'].keys()),
                        "total": sum([_data['DISK'][x]['total'] for x in _data['DISK'].keys()]),
                        "used": sum([_data['DISK'][x]['used'] for x in _data['DISK'].keys()]),
                        "percentage": sum([_data['DISK'][x]['used'] for x in _data['DISK'].keys()]) / sum(
                            [_data['DISK'][x]['total'] for x in _data['DISK'].keys()]),
                        "disk_list": [dict({'name': x, 'percentage': _data['DISK'][x]['percent']}, **_data['DISK'][x])
                                      for x in _data['DISK']]
                    },
                    "pods": []
                }
                _usage_list.append(_node_usage)
            except Exception as e:
                # ADD alter
                logging.error(e)
                continue
        return _usage_list

    def get_node_info(self):
        """
        通过cAdvisor API获取节点监控信息
        """
        node_list = []
        ret = self.api.list_node()

        for node in ret.items:
            address = node.status.addresses
            node_ip = [item for item in address if item.type == 'InternalIP'][0].address
            # Get machine infos
            machine_url = "http://%s:4194/api/v1.2/containers/" % node_ip
            try:
                response = requests.get(machine_url, timeout=10)
                if response.status_code != 200:
                    raise Exception
                node_info = response.json()
            except Exception as e:
                logging.error("fetch url:{} failed {}".format(machine_url, str(e)))
                node_info = None

            if not node_info:
                continue

            node_memory_total = node_info["spec"]["memory"]["limit"]

            node_state = node_info["stats"][1]
            node_memory_usage = node_state["memory"]["working_set"]
            node_memory_percentage = round(float(node_memory_usage) / node_memory_total, 6)

            node_time_diff = get_timestamp(node_info["stats"][1]["timestamp"]) - get_timestamp(node_info["stats"][0]["timestamp"])
            # Gets the length of the interval in nanoseconds
            node_time_diff = node_time_diff * 1000000000
            node_cpu_percentage = round(float(
                node_info["stats"][1]["cpu"]["usage"]["total"] - node_info["stats"][0]["cpu"]["usage"]["total"]) / node_time_diff,
                                6)

            node_disk_list = []
            node_device_usage = 0
            node_device_capacity = 0
            node_device = []
            for i in node_state["filesystem"]:
                if not i["device"].startswith("/"):
                    continue
                node_device_usage += i["usage"]
                node_device_capacity += i["capacity"]
                node_device.append(i["device"])
                node_disk_list.append({
                    "name": i["device"],
                    "total": i["capacity"],
                    "used": i["usage"],
                    "percentage": round(float(i["usage"]) / i["capacity"], 6)
                })
            node_device = "+".join(node_device)
            node_disk_percentage = round(float(node_device_usage) / node_device_capacity, 6)

            node_data = {
                "node_ip": node_ip,
                "cpu": {
                    "percentage": node_cpu_percentage
                },
                "memory": {
                    "total": node_memory_total,
                    "used":  node_memory_usage,
                    "percentage": node_memory_percentage
                },
                "disk": {
                    "name": node_device,
                    "total": node_device_capacity,
                    "used": node_device_usage,
                    "percentage": node_disk_percentage,
                    "disk_list": node_disk_list
                },
                "pods": []
            }
            logging.info("ip:{} cpu:{} memory:{} disk:{}".format(node_ip, node_cpu_percentage, node_memory_percentage, node_disk_percentage))

            node_list.append(node_data)

        return node_list
