#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-06-24 上午10:38
'''

tmp = "/root/jzhou/gpg/tmp"
string = "for test"
def encrypt(data):
    f = open(data,"w+")
    f.write(data)
    f.close()

    "gpg --recipient jia.zhou --output data.out --encrypt %s"%(tmp)