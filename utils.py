# -*- coding: utf-8 -*-

import time
from datetime import datetime
from datetime import date
import configparser
import urllib
import os
import logging
import tempfile
from kubernetes import client, config
import json
import traceback

K8S_CLUSTER_V1API_VERSION = 16  # 强制使用apps/v1的版本 1.16
K8S_CLUSTER_LOWEST_VERSION = 1.9  # 支持最低k8s版本
K8S_SOURCE_TYPE_DEPLOY = 'DEPLOYMENT'
K8S_SOURCE_TYPE = 'DEPLOYMENT'
K8S_OBJ_NODE = 'nodes'
K8S_OBJ_POD = 'pods'


def get_client(kubeconfig):
    """
    Loads configuration
    :param kubeconfig: file or string
    :returns ApiClient
    """

    if not kubeconfig:
        raise Exception

    if os.path.isfile(kubeconfig):
        # this only work for local file
        config_file = kubeconfig
    else:
        try:
            # 增加对ServiceAccount的支持，传入api-server的地址和指定serviceAccount的token来生成一个kubelet客户端
            _c_info = json.loads(kubeconfig)
            _config = client.Configuration()
            _config.verify_ssl = False
            _config.host = _c_info['api_server']
            _config.api_key['authorization'] = _c_info['token']
            client.configuration.debug = True
            return client
        except Exception as e:
            # logging.error(traceback.format_exc())
            # logging.info(f'Not ServiceAccount:{e.msg}')
            _, config_file = tempfile.mkstemp()
            with open(config_file, 'w') as fd:
                fd.write(kubeconfig)
    config.load_kube_config(config_file=config_file)
    client.configuration.debug = True
    return client


def get_timestamp(time_str):
    """convert date string to timestamp
    eg: 2018-07-17T14:26:13.249492592+08:00 ==> 1531808773.0
    :return seconds
    """
    timestamp = time.mktime(datetime.strptime(time_str[:19], '%Y-%m-%dT%H:%M:%S').timetuple())
    return timestamp


class Config(object):
    def __init__(self):
        """
        系统配置对象
        配置对象的结构为：section.option.value
        :return:
        """
        self._conf_ = configparser.RawConfigParser()

    @staticmethod
    def _get_config_from_map_path_(path: str):
        """
        获取指定路径下的配置文件
        :param path:
        :return:
        """
        _conf = configparser.RawConfigParser()
        for _file_name in os.listdir(path):
            _file = os.path.join(path, _file_name)
            if os.path.isfile(_file):
                with open(_file, 'r') as _f:
                    _file_name_split = _file_name.split(".")
                    if len(_file_name_split) > 1:
                        _section = _file_name_split[0]
                        _conf_name = _file_name_split[1]
                        _conf_value = ''.join([x for x in _f.read().split("\n") if not x.startswith("#")])
                        if _section not in _conf.sections():
                            _conf.add_section(_section)
                        _conf.set(_section, _conf_name, _conf_value)
        return _conf

    def append_config(self, conf: str):
        """
        增加配置信息
        :param conf:
        :return:
        """
        _append = configparser.RawConfigParser()
        if conf.startswith("http://") or conf.startswith("https://"):
            res = urllib.request.urlopen(conf)
            _append.readfp(res)
        elif os.path.isdir(conf):
            _append = self._get_config_from_map_path_(conf)
        else:
            _append.read(conf, "utf-8")

        for sn in _append.sections():
            for attr in _append.options(sn):
                if sn not in self._conf_.sections():
                    self._conf_.add_section(sn)
                self._conf_.set(sn, attr, _append.get(sn, attr))

    def get_conf(self, _section, _key=None, conf_type=bytes, default=None):
        """
        读取配置
        :param _section:
        :param _key:
        :param conf_type:
        :param default:
        :return:
        """
        try:
            if _key is None:
                return self._conf_.items(_section)
            else:
                if bool == conf_type or conf_type == bool:
                    return self._conf_.getboolean(_section, _key)
                elif int == conf_type:
                    return int(self._conf_.get(_section, _key))
                else:
                    return self._conf_.get(_section, _key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def set_conf(self, _section, _key, _value):
        """
        设置配置
        :param _section:
        :param _key:
        :param _value:
        :return:
        """
        if _section not in self._conf_.sections():
            self._conf_.add_section(_section)
        self._conf_.set(_section, _key, _value)

    def has_option(self, _section, _option):
        return _section in self._conf_.sections() and _option in self._conf_.options(_section)

    def get_options(self, section: str):
        """
        获取config所有section
        :return:
        """
        return self._conf_.options(section=section) if self._conf_.has_section(section=section) else None

    def get_sections(self):
        """
        获取一个配置对象下的所有section
        :return:
        """
        return self._conf_.sections()


def k8s_object_dict(_obj):
    """
    转化k8s获取到的资源对象成json格式
    attribute_map为k8s对象专用，存放了资源对象属性名称和别名
    :param _obj:
    :return:
    """

    def parse_value(_value):
        """
        K8s资源对象的基类都是object
        :param _value:
        :return:
        """
        return _value if isinstance(_value, (int, float, str)) or not _value \
            else _value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(_value, datetime) \
            else _value.strftime('%Y-%m-%d') if isinstance(_value, date) \
            else {_i: parse_value(_j) for _i, _j in _value.items()} if isinstance(_value, dict) \
            else [parse_value(_) for _ in _value] if isinstance(_value, list) \
            else k8s_object_dict(_value)

    return {
        _attr_label: parse_value(getattr(_obj, _attr_name))
        for _attr_name, _attr_label in getattr(_obj, 'attribute_map').items()
    } if hasattr(_obj, 'attribute_map') else _obj


def get_mysql_monitor_config(config_obj):
    return {
        'host': config_obj.get_conf(_section='ops', _key='db_host', default='127.0.0.1'),
        'port': int(config_obj.get_conf(_section='ops', _key='db_port', default=3306), ),
        'user_name': config_obj.get_conf(_section='ops', _key='db_username', default='hcm'),
        'password': config_obj.get_conf(_section='ops', _key='db_password', default=''),
        'db_name': config_obj.get_conf(_section='ops', _key='db_name', default='hcm_core')
    }


def get_mysql_cluster_info(config_obj):
    return {
        'mysql_node_list': [_.split(':') for _ in
                            config_obj.get_conf(_section='ops', _key='mysql_node_list').split(',')],
        'mysql_cluster_user': config_obj.get_conf(_section='ops', _key='mysql_cluster_user'),
        'mysql_cluster_password': config_obj.get_conf(_section='ops', _key='mysql_cluster_password')
    }
