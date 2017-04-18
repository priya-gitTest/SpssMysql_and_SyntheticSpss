#!/usr/bin/env python
# -*- coding:utf-8 -*-


# 合成spss文件
import json
import pickle
from savReaderWriter import SavWriter
from models.download_file import download_data

varNames = []
varTypes = {}
varLabels = {}
valueLabels = {}
formats = {}
my_columns_types = []

data = []


def spss_download():
    pass

query_information = download_data('information', 'ttt.sav')
query_data = download_data('data', 'ttt.sav')


for i in query_information:
    name = unicode(i["name"])
    my_columns_types.append(i["type"])
    varNames.append(name)

    if i["formats"].startswith("F") or i["formats"].startswith("D"):
        varTypes[name] = 0
    elif i["formats"].startswith("A"):
        varTypes[name] = int(i["formats"].split("A")[1])
    else:
        varTypes[name] = 0

    varLabels[name] = json.loads(i["varlabels"])
    if i["valuelabels"]:
        res2 = i["valuelabels"]
        if res2 == "0":
            valueLabels[name] = {}
        else:
            res3 = pickle.loads(res2)
            valueLabels[name] = res3

    else:
        valueLabels[name] = {}

    formats[name] = i["formats"]


savFileName = '/opt/someFile.sav'
with SavWriter(savFileName=savFileName, varNames=varNames, varTypes=varTypes,
               formats=formats, varLabels=varLabels, valueLabels=valueLabels,
               ioUtf8=True, columnWidths={}) as writer:
    for row_data in query_data:
        sub_li = []
        for i in range(len(my_columns_types)):
            sub_data = row_data[varNames[i]]
            if my_columns_types[i] == "VARCHAR":
                sub_li.append(json.loads(sub_data))
            elif my_columns_types[i] == "DATETIME":
                sub_li.append(writer.spssDateTime(b'%s' % sub_data, '%Y-%m-%d %H:%M:%S'))
            elif my_columns_types[i] == "DATE":
                sub_li.append(writer.spssDateTime(b'%s' % sub_data, '%Y-%m-%d'))
            else:
                sub_li.append(sub_data)
        data.append(sub_li)

    writer.writerows(data)

