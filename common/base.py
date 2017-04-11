#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ConfigParser
import os
import pymongo
# import MySQLdb
import pymysql


class Config(object):
    def __init__(self, config_filename="cgss.conf"):
        file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), config_filename)
        self.cf = ConfigParser.ConfigParser()
        self.cf.read(file_path)

    def get_sections(self):
        return self.cf.sections()

    def get_options(self, section):
        return self.cf.options(section)

    def get_content(self, section):
        result = {}
        for option in self.get_options(section):
            value = self.cf.get(section, option)
            result[option] = int(value) if value.isdigit() else value
        return result


class MongoDb(object):

    def __init__(self, host, port, user=None, password=None):
        self._db_host = host
        self._db_port = int(port)
        self._user = user
        self._password = password
        self.conn = None

    def connect(self):
        self.conn = pymongo.MongoClient(self._db_host, self._db_port)
        return self.conn

    def get_db(self, db_name):
        collection = self.conn.get_database(db_name)
        if self._user and self._password:
            collection.authenticate(self._user, self._password)
        return collection

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

# python 2.7可以用这个
# class My_Mysql(object):
#     def __init__(self, host, port, user, password, db_name):
#         self._db_host = host
#         # self._db_port = port
#         self._user = user
#         self._password = str(password)
#         self._db = db_name
#         self.conn = None
#         # print [self._db_host, self._user, self._password, self._db]
#
#     def connecta(self):
#         self.conn = MySQLdb.connect(self._db_host, self._user, self._password, self._db)
#         self.conn.cursor()
#         return self.conn.cursor()
#
#     def run_sql(self, sql):
#         cursor = self.connecta()
#         cursor.execute(sql)
#
#     def close(self):
#         if self.conn:
#             self.conn.close()
#             self.conn = None


class My_Pymysql(object):
    def __init__(self, host, port, user, password, db_name):
        self._db_host = host
        self._db_port = int(port)
        self._user = user
        self._password = str(password)
        self._db = db_name
        self.conn = None
        self.cursor = None
        # print [self._db_host,self._db_port, self._user, self._password, self._db]

    def connecta(self):
        self.conn = pymysql.connect(host=self._db_host, port=self._db_port, user=self._user, passwd=self._password, db=self._db)
        self.conn.cursor()
        self.cursor = self.conn.cursor()
        return self.conn.cursor()

    def run_sql(self, sql):
        cursor = self.connecta()
        cursor.execute(sql)
        self.conn.commit()

    def run_manysql(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def close(self):
        if self.conn:
            self.cursor = None
            self.conn.close()
            self.conn = None


def result(status, value):
    """
    staatus:
    2000, 什么都ok
    4000, 客户上传的文件格式不正确
    4001， 客户上传的文件列超过5400
    4002， 暂时梅想到
    5000， 服务器错误
    5001， 数据表已经存在
    5002,  sql语句错误
    """
    if status == 2000:
        message = u"True"
    elif status == 4000:
        message = u"客户上传的文件格式不正确"
    elif status == 4001:
        message = u"客户上传的文件列超过5400"
    elif status == 4002:
        message = u"暂时梅想到"
    elif status == 5000:
        message = u"服务器错误"
    elif status == 5001:
        message = u"数据表已经存在"
    elif status == 5002:
        message = u"sql语句错误"
    else:
        message = u"未知错误"
    return {
        "statuscode":status,
        "statusmessage": message,
        "value": value
    }


if __name__ == '__main__':

    ret = Config().get_content("mongo")
    print(ret)
    ret = My_Pymysql(**ret)
    sql = ""
    ret.run_sql(sql)
    ret.close()