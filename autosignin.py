#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-05-20 下午5:02
'''
import urllib2
import urllib
import gzip
import cookielib



def getOpener(head):
    # deal with the Cookies
    cj = cookielib.CookieJar()
    pro = urllib2.HTTPCookieProcessor(cj)
    opener = urllib2.build_opener(pro)
    header = []
    for key, value in head.items():
        elem = (key, value)
        header.append(elem)
    opener.addheaders = header
    return opener

def ungzip(data):
    try:        # 尝试解压
        print('正在解压.....')
        data = gzip.decompress(data)
        print('解压完毕!')
    except:
        print('未经压缩, 无需解压')
    return data


header = {
    'Connection': 'Keep-Alive',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate',
    'X-Requested-With': 'XMLHttpRequest',
    'Host': 'www.17sucai.com',
}


url = 'http://www.17sucai.com/auth'
opener = getOpener(header)

id = 'xxxxxxx'
password = 'xxxxxxx'
postDict = {
    'email': id,
    'password': password,
}

postData = urllib.parse.urlencode(postDict).encode()
op = opener.open(url, postData)
data = op.read()
data = ungzip(data)

print(data)


url = 'http://www.17sucai.com/member/signin' #签到的地址

op = opener.open(url)

data = op.read()
data = ungzip(data)

print(data)


class Header():
    def __init__(self,url,method,data,protocal):
        self.url = url
        self.method = method
        self.data = data
        self.protocal = protocal

def get_header(header_str):
    data = {}
    method = ""
    url = ""
    protocal = ""
    for i,j in enumerate(header_str.split("\n")):
        if i==0:
            print "line:",j
            req = j.split(" ")
            method = req[0].strip()
            url = req[1].strip()
            protocal = req[2].strip()
        else:
            print "line",j
            t = j.strip().split(":")
            key = t[0].strip()
            value = t[1].strip()
            data[key] = value
    return Header(url,method,data,protocal)

def split_to_dict(text):
    headers = text.strip().split("\n\n")
    headers_ = []
    for header in headers:
        if len(header.strip())==0:
            continue
        print "header: ",header
        headers_.append(get_header(header.strip()))
    return headers_


def sendHttpRequest(header):
    opener = getOpener(header.data)
    url = "http://"+header.data['Host']+header.url
    # id = 'xxxxxxx'
    # password = 'xxxxxxx'
    # postDict = {
    #     'email': id,
    #     'password': password,
    # }
    # postData = urllib.parse.urlencode(postDict).encode()
    # op = opener.open(url, postData)
    op = opener.open(url)
    data = op.read()
    data = ungzip(data)
    print(data)


url = 'http://www.17sucai.com/member/signin' #签到的地址

op = opener.open(url)

data = op.read()
data = ungzip(data)

print(data)

