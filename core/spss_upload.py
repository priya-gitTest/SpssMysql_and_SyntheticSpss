#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import datetime
import pickle
import savReaderWriter
from common.base import MongoDb, Config
import time, os
from collections import OrderedDict
import pymysql
from models.create_tables import create_tables, writer_tables, create_infor_tables
from common.base import my_datetime

vartypes = []  # ['A20', 'F8.2', 'F8', 'DATETIME20'] spss类型
width = []  # ['20', '8.2', '8', '20'] 宽度
valuetypes = []  # 数据类型
float_width = []


def read_sav(filepath):
    # 得到columns
    with savReaderWriter.SavReader(filepath) as read:
        ret = read.getSavFileInfo()
        """
        # getsavfileinfo infomation :
        # #(self.numVars, self.nCases, self.varNames, self.varTypes,self.formats, self.varLabels, self.valueLabels)
                # for i in ret:
        #     print i
        """
        return read.formats, read.varNames, read.varLabels, read.valueLabels


def writer_data(filepath, filename, valuetypes):
    res = writer_tables()
    res.conn()

    try:
        with savReaderWriter.SavReader(filepath, ioUtf8=True) as read:
            # 如果不用ioutf8， 汉字十六进制\被转义，更麻烦
            print valuetypes
            print read[0]
            my_time = my_datetime()
            for i in read:
                for j in range(len(valuetypes)):
                    # 数据库不认unicode所以要转换下
                    # 将varchar进行json存如数据库
                    if valuetypes[j] == "DATETIME":
                        become_time = my_time.become_str(i[j])
                        i[j] = become_time
                    elif valuetypes[j] == "VARCHAR":
                        i[j] = json.dumps(i[j])
                res.run_sql(filename, i)
    except Exception as e:
        print e
    finally:
        res.close()


def get_spss_data(formats, varnames):
    for i in varnames:
        vartypes.append(formats[i])
        if formats[i].startswith("F"):
            ret = formats[i].split("F")[1]
            width.append(ret)
            ret1 = ret.split(".")
            if ret1[1:]:
                valuetypes.append("FLOAT")
            else:
                valuetypes.append("INT")

        elif formats[i].startswith("A"):
            ret = formats[i].split("A")[1]
            width.append(ret)
            valuetypes.append("VARCHAR")

        if formats[i].startswith("D"):
            ret = formats[i].split("DATETIME")[1]
            width.append(ret)
            valuetypes.append("DATETIME")
    return vartypes, width, valuetypes


def float_data(width):
    for i in width:
        if i.split(".")[1:]:
            float_width.append(i[1])
        else:
            float_width.append(0)
    return float_width

def valuelables_decode(unicode_dict):
    if isinstance(unicode_dict, dict):
        for i in unicode_dict:
            unicode_dict[i] = unicode_dict[i].decode('utf-8')
        return unicode_dict
    elif isinstance(unicode_dict, str):
        return unicode_dict.decode('utf-8')

class create_insert_sub_table(object):
    def __init__(self):
        pass

    def create_table(self, filename):
        # res = create_infor_tables()
        # res.conn()
        # sql = res.create_sql(filename)
        # res.run_sql(sql)
        # res.close()
        try:
            res = create_infor_tables()
            res.conn()
            sql = res.create_sql(filename)
            res.run_sql(sql)
            res.close()
        except Exception as e:
            print e
            return 5001
        return 2000

    def insert_data(self, filename, varnames, valuetypes, width, float_width, varLabels, valueLabels, vartypes):
        res = create_infor_tables()
        res.conn()

        for i in range(len(varnames)):
            data = []
            data.append(varnames[i])
            data.append(valuetypes[i])
            data.append(width[i])
            data.append(float_width[i])
            data.append(json.dumps(varLabels[varnames[i]]))
            if varnames[i] in valueLabels:
                unicode_dict = valuelables_decode(valueLabels[varnames[i]])
                json_unicode_dict = pickle.dumps(unicode_dict)
                # print json_unicode_dict
                data.append(json_unicode_dict)
            else:
                data.append(0)
            data.append(vartypes[i])
            data.append("")
            data.append("")
            sql = res.insert_sql(filename, data)
            res.run_sql(sql)
        res.close()

def cheng_time(dtdt):
    # 将时间类型转换成时间戳
    if isinstance(dtdt, datetime.datetime):
        timestamp = time.mktime(dtdt.timetuple())
        return timestamp

    elif isinstance(dtdt, str):
        a_datetime = datetime.datetime.strptime(dtdt, "%Y-%m-%d, %H:%M:%S, %w")
        timestamp = time.mktime(a_datetime.timetuple())
        return timestamp

    elif isinstance(dtdt, float):
        return dtdt



def main(filename):
    # float_width: [0, 0, 0, 0, 0, 0, 0, 0, 0,....]
    # formats: {'Q2R7': 'F5', 'Q7': 'A400',....]
    # vartypes: ['A20', 'DATETIME40', 'DATETIME40', 'F5'...]
    # width: ['20', '40', '40', '5', '5', '5',]
    # valuetypes: ['VARCHAR', 'DATETIME', 'DATETIME', 'INT',]
    # varLabels: {'Q2R7': '\xa3\xc62.1\xa1\xa1\xc7\xeb\xb8\xf9\xbe\xdd\xc'}
    # valueLabels: {'Q2R7': {1.0: '\xb7\xc7\xb3\xa3\xb2\xbb\xb7\xfb\xba\xcf', 2.0: '\xb1\x'}}


    filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)), "file", filename)
    # 得到文件信息
    formats, varnames, varLabels, valueLabels = read_sav(filepath)

    if len(varnames) > 5400:
        return 4001

    vartypes, width, valuetypes = get_spss_data(formats, varnames)
    float_width = float_data(width)

    # 创建表
    ret = create_tables().run_sql(vartypes, width, valuetypes, formats, varnames, filename)
    ret1 = create_insert_sub_table().create_table(filename)

    # 写入数据
    writer_data(filepath, filename, valuetypes)
    for i in range(len(vartypes)):
        if vartypes[i].startswith("F"):
            if vartypes[i].split(".")[1:]:
                pass
            else:
                vartypes[i] = vartypes[i] + ".0"
    create_insert_sub_table().insert_data(filename, varnames, valuetypes, width, float_width, varLabels, valueLabels, vartypes)
    print vartypes

    if ret==2000 and ret1 == 2000:
        return ret
    else:
        return 5000


if __name__ == '__main__':
    filename = "1111.sav"
    main(filename)

