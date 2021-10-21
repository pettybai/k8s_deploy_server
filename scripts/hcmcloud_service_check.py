# _*_ coding:utf-8_*_
# Author:   Ace Huang
# Time: 2020/9/11 11:31
# File: hcmcloud_service_check.py

import requests
import os
import pymysql
import logging
import datetime
import redis

# #########配置信息######################

# request配置信息
TIMEOUT = 2
PING_METHOD = 'GET'

# 节点列表
NODE_LIST = ['127.0.0.1']

SOURCE_PORT = 8000

# 数据库信息
DB_INFO = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'hcm_core'
}

# redis信息，若非单独安装或者客户redis服务则注释下面的配置host、port、db、oauth
REDIS_INFO = {}

ES_INFO = {
    'host': '127.0.0.1',
    'post': 9200,
    'user': 'elastic',
    'password': 'hcmcloud_2018'
}

# NFS目录，挂载到脚本所属节点的目录
NFS_PATH = '/Users/huangxinyuan/Desktop/opssql'

# K8s相关配置
K8S_NS = 'default'
SERVICE_FRONT = 'hcm-cloud'
SERVICE_BACKEND = 'hcm-core'
SERVICE_OFFICE = 'hcm-office'
SERVICE_ELASTICSEARCH = 'elasticsearch'


# #########配置信息######################

class K8SCommandUtils(object):
    """
    k8s命令执行结果解析工具，负责解析k8s结果，返回结果数组
    """

    @staticmethod
    def parse_result(output):
        """
         将获取的结果整理（第一行为结果名称，后面的每行都与之对应）
        :param output:
        :return:
        """
        _name_list = output[0][:-1].split(' ')
        _name_list = [_ for _ in _name_list if _]
        _re_list = []
        for _i in output[1:]:
            _i = _i[:-1].split(' ')
            _c = 0
            _item = {}
            for _j in _i:
                if _j:
                    _item[_name_list[_c]] = _j
                    _c += 1
            _re_list.append(_item)
        return _re_list


class LogRecordUtils(object):
    """
    日志记录工具，生成检测结果文件
    """

    def __init__(self):
        """
        初始化检测结果文件
        """
        self.log_file = '{}/{}.log'.format(NFS_PATH, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        with open(self.log_file, 'a+') as log_file:
            log_file.write('------------CHECK BEGIN-{}-----------\n'.format(
                datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
            log_file.close()

    def write_log(self, service, log_content):
        """
        将检测结果输入到日志中
        :param service:
        :param log_content:
        :return:
        """
        with open(self.log_file, 'a+') as log_file:
            log_file.write('----------begin-{}----------\n'.format(service))
            log_file.write('--------------{}------------\n'.format(datetime.datetime.now()))
            log_file.write(log_content)
            log_file.write('\n----------end-{}----------\n\n\n'.format(service))
            log_file.close()


class PrivateEnvCheck(object):
    """
    私有化环境环境检测
        检测内容：
            1、所有节点存活及资源使用情况（应用节点、数据库服务、文件服务器）
            2、所有应用存活情况（前端、后端、office、es、redis、mysql、nfs）
            3、服务整体存活情况
    """

    def __init__(self):
        """
        配置信息初始化
        """
        self.log_writer = LogRecordUtils()

    def check(self):
        """
        整体检查入口
        :return:
        """
        self.node_alive()
        self.front_service()
        self.backend_service()
        self.office_service()
        self.es_service()
        self.db_service()
        self.cache_service()
        self.nfs_service()
        self.log_writer.write_log(service='END', log_content='END')

    def node_alive(self):
        """
        节点存活检测
        先获取节点的8000端口的资源使用情况来判断节点存活，
        若资源使用情况获取失败则再通过ssh端口判断节点存活，否则判定节点为宕机
        :return:
        """
        _node_status = {}
        for _ in NODE_LIST:
            try:
                _response = requests.request(
                    method=PING_METHOD,
                    url='http://{}:{}'.format(_, SOURCE_PORT),
                    timeout=TIMEOUT)
                _node_status[_] = _response.content if getattr(_response, 'status_code') == 200 else False
            except Exception as e:
                logging.error(e)
                _node_status[_] = False
        try:
            self.log_writer.write_log(service='NodeStatus', log_content=_node_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service='NodeStatus', log_content='Write Result Error!!!')
        return _node_status

    @staticmethod
    def get_service_info(service_name):
        """
        获取k8s中service信息
        :param service_name:
        :return:
        """
        _output = os.popen(cmd='kubectl get svc -n {}'.format(K8S_NS))
        _svc_list = K8SCommandUtils.parse_result(_output)
        for _ in service_name:
            if _['NAME'] == service_name:
                return _
        return None

    def check_service_ping(self, service_name):
        """
        检查k8s服务是否正常
        :param service_name:
        :return:
        """
        _front_info = self.get_service_info(service_name=service_name)
        _service_url = 'http://{}:{}/ping'.format(_front_info['CLUSTER-IP'], _front_info['PORT(S)'].split(':')[0])
        _response = requests.request(method='GET', url=_service_url, timeout=2)
        return True if _response.status_code == 200 else False

    def front_service(self):
        """
        前端服务检测
        目前方案只检测前端ping存活
        :return:
        """
        _alive_status = self.check_service_ping(service_name=SERVICE_FRONT)
        try:
            self.log_writer.write_log(service=SERVICE_FRONT, log_content=_alive_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service=SERVICE_FRONT, log_content='Result Write Error!!!')
        return _alive_status

    def backend_service(self):
        """
        后端服务检测
        :return:
        """
        _alive_status = self.check_service_ping(service_name=SERVICE_BACKEND)
        try:
            self.log_writer.write_log(service=SERVICE_BACKEND, log_content=_alive_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service=SERVICE_BACKEND, log_content='Result Write Error!!!')
        return _alive_status

    def office_service(self):
        """
        office服务检测
        :return:
        """
        _alive_status = self.check_service_ping(service_name=SERVICE_OFFICE)
        try:
            self.log_writer.write_log(service=SERVICE_OFFICE, log_content=_alive_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service=SERVICE_OFFICE, log_content='Result Write Error!!!')
        return _alive_status

    def db_service(self):
        """
        Mysql服务检测
        :return:
        """
        try:
            _conn = pymysql.connect(**DB_INFO)
            _conn.cursor().execute('select id from company;')
            _alive_status = 'Mysql is alive!!!'
        except Exception as e:
            logging.error(e)
            _alive_status = e
        try:
            self.log_writer.write_log(service='Mysql', log_content=_alive_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service='Mysql', log_content='Result Write Error!!!')

    def cache_service(self):
        """
        redis服务检测
        :return:
        """
        _cache_info = self.get_service_info('redis')
        _background_cache_info = self.get_service_info('redis-background')
        try:
            _redis_service = redis.Redis(host=_cache_info['CLUSTER-IP'], port=6379, db=0)
            _redis_service.keys('*employee*')
            _redis_service.set(name='alive_test_key_{}'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')),
                               value='0',
                               ex=10)
            self.log_writer.write_log(service='Cache-Redis', log_content='Cache-Redis Normal')
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service='Cache-Redis', log_content=e)

        try:
            _redis_background_service = redis.Redis(host=_background_cache_info['CLUSTER-IP'], port=6379, db=0)
            _redis_service.keys('*celery*')
            _redis_service.set(name='alive_test_key_{}'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')),
                               value='0',
                               ex=10)
            self.log_writer.write_log(service='Cache-Redis-Background', log_content='Cache-Redis-Background Normal')
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service='Cache-Redis-Background', log_content=e)

    def nfs_service(self):
        """
        文件服务检测
        :return:
        """
        _test_file = os.path.join(NFS_PATH,
                                  'alive_test_file_{}.txt'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
        try:
            with open(_test_file, 'a+') as tmp_file:
                for _ in range(10):
                    tmp_file.write('Test file{}'.format(_))

            os.remove(_test_file)
            self.log_writer.write_log(service='NFSService', log_content='NFSService is alive!!!')
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service='NFSService', log_content='NFSService has some problem!!!')

    def es_service(self):
        """
        ES服务检测
        :return:
        """
        _alive_status = self.check_service_ping(service_name=SERVICE_ELASTICSEARCH)
        try:
            self.log_writer.write_log(service=SERVICE_ELASTICSEARCH, log_content=_alive_status)
        except Exception as e:
            logging.error(e)
            self.log_writer.write_log(service=SERVICE_ELASTICSEARCH, log_content='Result Write Error!!!')
        return _alive_status


if __name__ == '__main__':
    _check_obj = PrivateEnvCheck()
    _check_obj.check()
