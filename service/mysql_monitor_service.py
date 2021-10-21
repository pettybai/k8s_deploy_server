# _*_ coding:utf-8_*_
# Author:   Ace Huang
# Time: 2020/5/21 15:43
# File: mysql_monitor_service.py

import pymysql
import logging
import datetime
import decimal

from pymysql.constants import ER


class MysqlMonitorService(object):
    """
    针对Mysql常用监控
    """

    def __init__(self, host: str, port: int, user_name: str, password: str, db_name: str = None):
        """
        初始化Mysql链接参数
        :param host:
        :param port:
        :param user_name:
        :param password:
        """
        self._conn_info = {
            'host': host,
            'port': port,
            'user': user_name,
            'password': password
        }
        if db_name:
            self._conn_info['database'] = db_name
        self.db_conn = pymysql.connect(**self._conn_info)
        self.cursor = self.db_conn.cursor()

    def reconnect(self):
        """
        重连，依据self._conn_info中参数
        :return:
        """
        self.db_conn = pymysql.connect(**self._conn_info)
        self.cursor = self.db_conn.cursor()

    def get_version(self):
        """
        获取数据库版本信息
        :return:
        """
        return {'data': self.db_conn.get_server_info()}

    def __change_db(self, db_name: str):
        """
        切换数据库
        :param db_name:
        :return:
        """
        self.db_conn.select_db(db=db_name)
        self.cursor = self.db_conn.cursor()

    def __execute_sql(self, sql_str: str, db_name: str = None):
        """
        执行指定的sql语句
        :param sql_str:
        :param db_name:
        :return:
        """

        def parse_value(_value):

            return _value.strftime('%Y-%m-%d %H:%M:%S') \
                if isinstance(_value, datetime.datetime) else _value.strftime('%Y-%m-%d') \
                if isinstance(_value, datetime.date) else float(_value) \
                if isinstance(_value, decimal.Decimal) else _value

        db_name and self.__change_db(db_name=db_name)
        try:
            self.cursor.execute(sql_str)
        except pymysql.err.ProgrammingError as e:
            # 重连，若指定db则不会出现连接失效报错
            logging.error(e)
            self.reconnect()
            self.cursor.execute(sql_str)
        except Exception as e1:
            logging.error(e1)
            if e1.args[0] == ER.NO_DB_ERROR:
                self.reconnect()
                self.cursor.execute(sql_str)
        _result_list = self.cursor.fetchall()
        _field_list = [_des[0] for _des in self.cursor.description] if _result_list else []
        self.cursor.close()
        return {
            'data': [
                {
                    _field: parse_value(_item[_index])
                    for _index, _field in enumerate(_field_list)
                }
                for _item in _result_list]
        }

    def get_status(self):
        """
        获取数据库当前状态
        :return:
        """
        _result_list = self.__execute_sql('show global status;')
        logging.info(_result_list)
        return {'data': _result_list}

    def execute_sql(self, sql_str: str, db_name: str = None):
        """
        指定数据库执行SQL
        todo：sql和db_name的管控
        :param sql_str:
        :param db_name:
        :return:
        """
        return self.__execute_sql(sql_str=sql_str, db_name=db_name)
