#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-08 下午3:11
'''
import json
import urllib2

import requests
from poster.streaminghttp import register_openers

token = 'eyJ1X2lkIjoiIiwidV90b2tlbiI6IiJ9'
#第一步登录
url = "https://m.ddyjapp.com/api-service-app/user/login"
data = {
    "Host": "m.ddyjapp.com",
    "Content-Type": "application/json",
    "Connection": "keep-alive",
    "app_token": "{}".format(token),
    "Accept": "*/*",
    "User-Agent": "DDYJ/3.0.5 (iPhone; iOS 13.1.2; Scale/2.00)",
    "Accept-Language": "zh-Hans-CN;q=1, ja-JP;q=0.9, en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br"
}
jsons = {"lastAddress": "北京市朝阳区太阳宫中路靠近冠城大厦", "lastShortAddress": "朝阳区太阳宫中路14", "userName": "18565877872",
         "imei": "50D0CB08-04F2-4447-96A9-D2D324D6991B", "password": "zj18565877872", "lon": "116.446938",
         "terminal": "ios", "lat": "39.970966", "lastAcArea": "110105", "regIdAlias": "1114a89792cc8ef01ff",
         "loginType": "1"}
r = requests.post(url,headers=data,data=json.dumps(jsons))
print r.text

#
POST https://m.ddyjapp.com/api-service-app/user/home-users HTTP/1.1
Host: m.ddyjapp.com
Content-Type: application/json
Connection: keep-alive
app_token: eyJ1X2lkIjoiMjM0MjcwMzU3NzIzODE1OTM2IiwidV90b2tlbiI6IjdGNDFCMzQ2RUFENDI3MDlBN0QxMzkzQzEzNjM0NjZFIn0=
Accept: */*
User-Agent: DDYJ/3.0.5 (iPhone; iOS 13.1.2; Scale/2.00)
Accept-Language: zh-Hans-CN;q=1, ja-JP;q=0.9, en-US;q=0.8
Content-Length: 57
Accept-Encoding: gzip, deflate, br

{"lat":"39.971001","categoryType":"1","lon":"116.446953"}

#第二部我要接单
app_token = "eyJ1X2lkIjoiMjM0MjcwMzU3NzIzODE1OTM2IiwidV90b2tlbiI6IjdGNDFCMzQ2RUFENDI3MDlBN0QxMzkzQzEzNjM0NjZFIn0="
url = "https://m.ddyjapp.com/api-service-app/needs/accept"
data = {
    "Host": "m.ddyjapp.com",
    "Content-Type": "application/json",
    "Connection": "keep-alive",
    "app_token": "{}".format(app_token),
    "Accept": "*/*",
    "User-Agent": "DDYJ/3.0.5 (iPhone; iOS 13.1.2; Scale/2.00)",
    "Accept-Language": "zh-Hans-CN;q=1, ja-JP;q=0.9, en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br"
}
jsons = {"needsId":"","lat":39.971001000000001,"lon":116.446956}
r = requests.post(url,headers=data,data=json.dumps(jsons))
print r.text

{"needsId":"","lat":39.971001000000001,"lon":116.446956}
