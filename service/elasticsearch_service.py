# _*_ coding:utf-8_*_
# Author:   Ace Huang
# Time: 2020/5/21 22:28
# File: elasticsearch_service.py

from elasticsearch import Elasticsearch


class ESService(object):
    """
    Es基本操作
    """

    def __init__(self, hosts: list, user_name: str, password: str):
        """
        初始化ES服务
        :param hosts:['IP1:Port1','IP2:Port2'...]
        :param user_name:
        :param password:
        """
        self.es = Elasticsearch(hosts=hosts, http_auth=(user_name, password))

    def query_index_by_name(self, name: str):
        """
        根据给定关键字查询索引名称
        :param name:
        :return:
        """
        _index_list = self.es.cat.indices(format='json')
        return [_index['index'] for _index in _index_list if _index['index'].startswith(name)]

    def delete_index_by_name(self, name: str):
        """
        根据给定名称删除索引
        :param name: 精确匹配
        :return:
        """
        return self.es.indices.delete(name)
