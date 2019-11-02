#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-16 下午1:04
'''
import requests
import datetime

#aiw 抢购
usrname = "13051514442"
password = "weijiayou504+"
amount = 999
rate = 54


token = ""

status_code = 0
while status_code != 200:
    url = "http://114.55.208.50/appApi2.html?action=UserLoginNew&loginName={}&password={}".format(usrname,password)
    data = {
        "content-type": "application/json",
        "Host": "114.55.208.50",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "User-Agent": "okhttp/3.12.1",
        "If-Modified-Since": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")

    }
    r = requests.get(url,data)
    status_code = r.status_code
    print r.json()['msg'],r.json()
    token = r.json()['token']

status_code = 0
flag = True
while flag:
    url = "http://114.55.208.50/appApi2.html?action=getExchangeInfo&token={}".format(token)
    data = {
        "content-type": "application/json",
        "Host": "114.55.208.50",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "User-Agent": "okhttp/3.12.1",
        "If-Modified-Since": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")
    }
    r = requests.get(url,data)
    status_code = r.status_code
    if status_code != 200:
        continue
    if r.json()['rate'] != rate:
        flag = True
    elif r.json()['rate'] == rate:
        flag = False
    print r.json()

status_code = 0
while status_code != 200:
    url = "http://114.55.208.50/appApi2.html?action=exchange&token={}&amount={}".format(token,amount)
    data = {
        "content-type": "application/json",
        "Host": "114.55.208.50",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "User-Agent": "okhttp/3.12.1",
        "If-Modified-Since": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")

    }
    r = requests.get(url,data)
    status_code = r.status_code
    print r.json()['msg'],r.json()