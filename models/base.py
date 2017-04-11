#!/usr/bin/env python
# -*- coding:utf-8 -*-
from common.base import Config, My_Pymysql

class base_model():
    def __init__(self, conf_name):
        self.conf = Config().get_content(conf_name)
        self.ret = None

    def create_sql(self, sql):
        sql = sql
        return sql
    def conn(self):
        ret = My_Pymysql(**self.conf)
        ret.connecta()
        self.ret = ret

    def run_sql(self, sql):
        sql = self.create_sql(sql)
        try:
            self.ret.run_manysql(sql)
        except Exception as e:
            print e
            return 5002
        return 2000

    def close(self):
        self.ret.close()
        self.ret = None