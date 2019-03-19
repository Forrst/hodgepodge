#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-03-08 下午4:38
'''
def bubleSort(xlist):
    for i in xrange(len(xlist)):
        for j in xrange(i+1,len(xlist)):
            if xlist[i] < xlist[j]:
                t = xlist[i]
                xlist[i] = xlist[j]
                xlist[j] = t
    return xlist

def selectSort(xlist):
    t = xlist[0]
    for i in xlist:
        if i>t:
            t = i
    