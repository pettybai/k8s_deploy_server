# _*_ coding:utf-8_*_
# Author:   Ace Huang
# Time: 2020/5/22 08:56
# File: es_index_clear.py

from elasticsearch import Elasticsearch
import datetime
import logging


class ESService(object):
    """
    Es基本操作
    """

    def __init__(self, hosts, user_name, password):
        """
        初始化ES服务
        :param hosts:['IP1:Port1','IP2:Port2'...]
        :param user_name:
        :param password:
        """
        self.es = Elasticsearch(hosts=hosts, http_auth=(user_name, password))

    def query_index_by_name(self, name):
        """
        根据给定关键字查询索引名称
        :param name:
        :return:
        """
        _index_list = self.es.cat.indices(format='json')
        return [_index['index'] for _index in _index_list if _index['index'].startswith(name)]

    def delete_index_by_name(self, name):
        """
        根据给定名称删除索引
        :param name: 精确匹配
        :return:
        """
        return self.es.indices.delete(name)


if __name__ == '__main__':
    es_service = ESService(hosts=[''], user_name='elastic', password='hcmcloud_2018')
    _list = es_service.query_index_by_name('logstash-')
    _del_day = (datetime.datetime.now() - datetime.timedelta(days=-7)).strftime('%Y.%m.%d')
    _del_list = [_i for _i in _list if _i < 'logstash-{}'.format(_del_day)]
    _result = [es_service.delete_index_by_name(_item) for _item in _del_list]
    logging.info(_result)
