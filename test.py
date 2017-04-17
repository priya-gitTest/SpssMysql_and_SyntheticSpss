#!/usr/bin/env python
# -*- coding:utf-8 -*-



def a(d):
    if isinstance(d, dict):
        for i in d:
            d[i] = d[i].decode('utf-8')


if __name__ == '__main__':
    d = {1.0: '\xe7\x94\xb7', 2.0: '\xe5\xa5\xb3'}
    a(d)
