#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-12-12 下午3:06
'''
a = [2,4,3,8,5,6,9,1]
for i in xrange(len(a)):
    for j in xrange(i+1,len(a)):
        # print a[i],a[j]
        if a[i]<a[j]:
            a[i],a[j] = a[j],a[i]
        print a
print a
