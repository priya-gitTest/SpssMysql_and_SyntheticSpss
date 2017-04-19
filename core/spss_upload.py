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
from common.log import my_log
from common.base import my_datetime
from models.create_tables import create_data_table, writer_data_table
from models.create_tables import create_information_tables, writer_information_tables

vartypes = []  # ['A20', 'F8.2', 'F8', 'DATETIME20'] spss类型
width = []  # ['20', '8.2', '8', '20'] 宽度
valuetypes = []  # 数据类型
float_width = []


# 获取文件详细信息
def read_sav(filepath):
    with savReaderWriter.SavReader(filepath) as read:
        ret = read.getSavFileInfo()
        """
        # getsavfileinfo infomation :
        # (self.numVars, self.nCases, self.varNames, self.varTypes,self.formats, self.varLabels, self.valueLabels)
        """
        return read.formats, read.varNames, read.varLabels, read.valueLabels


# 写入数据到数据库
def writer_data(filepath, filename, valuetypes):
    res = writer_data_table()
    with savReaderWriter.SavReader(filepath, ioUtf8=True) as read:
        # 如果不用ioutf8， 汉字十六进制\被转义，更麻烦
        my_time = my_datetime()
        for i in read:
            for j in range(len(valuetypes)):
                # 数据库不认unicode所以要转换下
                # 将varchar进行json存如数据库
                if valuetypes[j] == "DATETIME":
                    become_time = my_time.become_str(i[j])
                    i[j] = become_time
                elif valuetypes[j] == "DATE":
                    become_time = my_time.become_str(i[j])
                    i[j] = become_time
                elif valuetypes[j] == "VARCHAR":
                    i[j] = json.dumps(i[j])
            res.insert_sql(filename, i)
    res.close()


# 获取spss需要的一些数据
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

        elif formats[i].startswith("DATE"):
            if formats[i].split("DATE")[1].startswith("TIME"):
                ret = formats[i].split("DATETIME")[1]
                width.append(ret)
                valuetypes.append("DATETIME")
            else:
                ret = formats[i].split("DATE")[1]
                width.append(ret)
                valuetypes.append("DATE")
    return vartypes, width, valuetypes


# 进行切割,判断是不是有浮点位的,如果没有填充0,生成文件要用
def float_data(width):
    for i in width:
        if i.split(".")[1:]:
            float_width.append(i[1])
        else:
            float_width.append(0)
    return float_width


# 判断是字典还是字符串, 进行decode
def valuelables_decode(unicode_dict):
    if isinstance(unicode_dict, dict):
        for i in unicode_dict:
            unicode_dict[i] = unicode_dict[i].decode('utf-8')
        return unicode_dict
    elif isinstance(unicode_dict, str):
        return unicode_dict.decode('utf-8')


# 插入信息表的数据
def insert_sub_table(filename, varnames, valuetypes, width, float_width, varLabels, valueLabels, vartypes):
    res = writer_information_tables()

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
        # sql = res.insert_sql(filename, data)
        res.insert_sql(filename, data)
    res.close()


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
    #不允许超过1024列MySQL
    if len(varnames) > 1024:
        return 4001

    vartypes, width, valuetypes = get_spss_data(formats, varnames)
    float_width = float_data(width)

    # 创建表
    create_data_table(vartypes, width, valuetypes, formats, varnames, filename)
    create_information_tables(filename)

    # 写入数据
    writer_data(filepath, filename, valuetypes)
    for i in range(len(vartypes)):
        if vartypes[i].startswith("F"):
            if vartypes[i].split(".")[1:]:
                pass
            else:
                vartypes[i] = vartypes[i] + ".0"
    insert_sub_table(filename, varnames, valuetypes, width, float_width, varLabels, valueLabels, vartypes)

    # if ret==2000 and ret1 == 2000:
    #     return ret
    # else:
    #     return 5000


if __name__ == '__main__':
    filename = "1111.sav"
    main(filename)
