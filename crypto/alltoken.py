#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-08-01 下午5:47
'''
'''
GET https://3m.cybex.io/realtime_pager_mongo?page=0&limit=20 HTTP/1.1
Host: 3m.cybex.io
Connection: keep-alive
Accept: application/json, text/plain, */*
Origin: https://cybex.live
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36
DNT: 1
Referer: https://cybex.live/block/14931769
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7
'''

import requests

token = ""

status_code = 0
url = "https://3m.cybex.io/realtime_pager_mongo?page=0&limit=20"
data = {
    "Host": "3m.cybex.io",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://cybex.live",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    "DNT": "1",
    "Referer": "https://cybex.live/block/14931769",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7"
}
r = requests.get(url,data)
status_code = r.status_code
print r.json()['msg'],r.json()
token = r.json()['token']







'''
***
import re
def num_from_block_id(block_id):
    'block id is a str like '0ΘΘcbdc8ef185d54ef44cccddc99b538Θ5195d94'
    assert len(block_id) == 40
    def num_from_hex32(s):
        assert len(s) == 8
        return int("". join(re.findall(r".{2}", s) [::-1]), 16)
    _sum = 0
    for i in range(8,40,8):
        s = block_id[i:i+8]
        _sum += num_from_hex32(s)
    _sum &=((1 << 32)-1)
    return _sum
if __name__ == "__main__":
    ret = num_from_block_id("00e3dbc2b053ef6dd48c270ac7d274f4d829c4d1")
    print ret,ret%100

***
'''
