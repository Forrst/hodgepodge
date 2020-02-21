#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-03-08 下午4:38
'''
def bubbleSort(xlist):
    '''
    冒泡排序法，降序
    :param xlist:
    :return:
    '''
    for i in range(len(xlist)):
        for j in range(i+1,len(xlist)):
            print(f"{xlist[i]}\t{xlist[j]}")
            if xlist[i] < xlist[j]:
                t = xlist[i]
                xlist[i] = xlist[j]
                xlist[j] = t
    return xlist

def selectSort(xlist):
    '''
    选择排序
    先扫描一次找出最小的索引，然后交换首位和最小值的位置
    :param xlist:
    :return:
    '''
    t = xlist[0]
    for i in xlist:
        if i>t:
            t = i
    