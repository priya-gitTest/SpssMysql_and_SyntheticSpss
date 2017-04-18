#!/usr/bin/env python
# -*- coding:utf-8 -*-


"""
import datetime

savFileName = '/opt/someFile.sav'
varNames = [u'ID', u'StartTime', u'EndTime', u'VerNo', u'Q1', u'Q2', u'Q4']
varTypes = {u'Q1': 0, u'Q2': 400, u'Q4': 400, u'StartTime': 0, u'VerNo': 0, u'EndTime': 0, u'ID': 20}
varLabels = {u'Q1': u'\u5546\u8d85\u914d\u9001\u6536\u8d39\u6807\u51c6\u6b63\u786e\u7684\u662f', u'Q2': u'\u5546\u8d85\u4e0a\u7ebf\u6807\u51c6', u'Q4': u'\u672c\u6b21\u57f9\u8bad\u6536\u83b7\u548c\u610f\u89c1', u'StartTime': u'\u5f00\u59cb\u65f6\u95f4', u'VerNo': u'\u7248\u672c', u'EndTime': u'\u7ed3\u675f\u65f6\u95f4', u'ID': u'\u7528\u6237'}
valueLabels = {'Q1': {1.0: u'\u4e13\u9001\u6536\u8d39', 2.0: u'\u5feb\u9001\u6536\u8d39'}, u'Q2': {}, u'Q4': {}, 'StartTime': {}, 'VerNo': {}, 'EndTime': {}, 'ID': {}}
formats = {u'Q1': u'F5.0', u'VerNo': u'F5.0', u'EndTime': 'DATETIME40', u'StartTime': 'DATETIME40'}
data = [[u'lKWmel1491380676', 13710788676.0, 13710788696.0, 1L, 1, u'\u725b\u820c', u'\u6e56\u516c\u56ed\u80e1\u5a77']]
# 时间模块这样是错误的data = [[u'lKWmel1491380676', datetime.datetime(2016, 9, 21, 13, 42, 8), datetime.datetime(2016, 9, 21, 13, 42, 8), 1L, 1, u'\u725b\u820c', u'\u6e56\u516c\u56ed\u80e1\u5a77']]
#
# with SavWriter(savFileName, varNames, varTypes, varLabels=varLabels, columnWidths={}, ioUtf8=True) as writer:
#     writer.writerows(data)
with SavWriter(savFileName=savFileName, varNames=varNames, varTypes=varTypes,
               varLabels=varLabels, valueLabels=valueLabels, ioUtf8=True, formats=formats,
               columnWidths={}) as writer:

    writer.writerows(data)
"""