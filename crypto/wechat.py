#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-10 下午6:13
'''
import requests
appid,secret = "wx4610aa974308e24b","f4bd6e636b65e0fa4045483725606ebe"
access_token_url = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={}&secret={}".format(appid,secret)
r = requests.get(access_token_url)
access_token = r.json()['access_token']