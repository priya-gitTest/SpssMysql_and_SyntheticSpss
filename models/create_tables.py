#!/usr/bin/env python
# -*- coding:utf-8 -*-
# import MySQLdb
import json
from common.base import Config, My_Pymysql


class create_tables(object):
    def __init__(self):
        self.conf = Config().get_content("mysql")
        # print self.conf

    def create_sql(self, vartypes, width, valuetypes, formats, varnames, filename):
        # LAST_NAME  CHAR(20),
        sql = """CREATE TABLE `{}` (""".format(filename)
        for i in range(len(varnames)):
            if valuetypes[i] == "FLOAT":
                num = width[i].split(".")
                s = "`{}` {}({},{}) DEFAULT NULL".format(varnames[i], valuetypes[i], num[0], num[1])
            elif valuetypes[i] == "DATETIME":
                s = "`{}` {} DEFAULT NULL".format(varnames[i], valuetypes[i])
            else:
                s = "`{}` {}({}) DEFAULT NULL".format(varnames[i], valuetypes[i], width[i])

            if i < len(varnames) - 1:
                sql = sql + s + ","
            elif i == len(varnames) - 1:
                sql = sql + s

        sql = sql + ") ENGINE=InnoDB DEFAULT CHARSET=UTF8"
        return sql

    def run_sql(self, vartypes, width, valuetypes, formats, varnames, filename):
        sql = self.create_sql(vartypes, width, valuetypes, formats, varnames, filename)
        try:
            ret = My_Pymysql(**self.conf)
            ret.run_sql(sql)
            ret.close()

        except Exception as e:
            print e
            return 5001
        return 2000


class writer_tables(object):
    def __init__(self):
        self.conf = Config().get_content("mysql")
        self.ret = None

    def create_sql(self, tablename, data):
        data = tuple(data)
        sql = "insert INTO `{}` VALUES {}".format(tablename, data)
        return sql

    def conn(self):
        ret = My_Pymysql(**self.conf)
        ret.connecta()
        self.ret = ret

    def run_sql(self, filename, data):
        sql = self.create_sql(filename, data)
        try:
            self.ret.run_manysql(sql)
        except Exception as e:
            print e
            return 5002
        return 2000

    def close(self):
        self.ret.close()
        self.ret = None


class create_infor_tables(object):
    def __init__(self):
        self.conf = Config().get_content("sub_table")
        self.ret = None

    def create_sql(self, tablename):
        sql = """CREATE TABLE `{}` (
        `name` VARCHAR (255) DEFAULT NULL,
        `type` VARCHAR (255) DEFAULT NULL,
        `width` INT (5) DEFAULT NULL,
        `float_width` INT (5) DEFAULT NULL,
        `varlabels` text,
        `valuelabels` text
        ) ENGINE = INNODB DEFAULT CHARSET = utf8;""".format(tablename)
        return sql

    def insert_sql(self, tablename, data):
        data = tuple(data)
        sql = "insert INTO `{}` VALUES {}".format(tablename, data)
        return sql

    def conn(self):
        ret = My_Pymysql(**self.conf)
        ret.connecta()
        self.ret = ret

    def run_sql(self, sql):
        try:
            self.ret.run_manysql(sql)
        except Exception as e:
            print e
            return 5002
        return 2000

    def close(self):
        self.ret.close()
        self.ret = None


if __name__ == '__main__':
    # ret = create_tables()

    # res = writer_tables()
    # res.conn()
    # res.run_sql("222", ['hrYjT71474436254', '2016-09-21 13:37:34'])
    # res.close()
    pass
