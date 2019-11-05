#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-31 上午8:39
'''
import datetime
import subprocess
import time
import requests
import urllib2
import logging
import re
import MySQLdb
from bs4 import BeautifulSoup as soup

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("hkdistirbution.py")


now = datetime.datetime.now()
now_day = str(now.day)
now_month = str(now.month)
url = "https://www.hkexnews.hk/ncms/script/eds/homecat2_c.json?_={}".format(int(time.time()*1000))
data = {
    "Host": "www.hkexnews.hk",
    "Connection": "keep-alive",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "DNT": "1",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": "https://www.hkexnews.hk/index_c.htm",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
    "Cookie": "sclang=zh-CN;TS016e7565=015e7ee60365f95f96555bd742b02978c327bb6e57cae34056b44ee6163a784ccb73cb0f415fd16971a392d740200b87c3b6644737; WT_FPC=id=46.17.45.134-1288001088.30772922:lv=1572482259684:ss=1572482231190"
}
r = requests.get(url,data)
bills = r.json()
logger.info(u"最新的配发结果公告如下:\n{}".format(r.text))
billList = bills['newsInfo']

requestPara = []
codeSet = set()
for bill in billList:
    relDay = bill['relD']
    relMonth = bill['relM']
    webPath = bill['webPath']
    webPathList = webPath.split("/")
    year = webPathList[-3]
    host = "/".join(webPath.split("/")[:-1])+"/"
    date = "{}-{}-{}".format(year,relMonth,relDay)
    stockCode = bill['stock'][0]['sc']
    if int(relDay) == int(now_day) and int(relMonth) == int(now_month):
        webXml = urllib2.urlopen(webPath).read()
        webSoup = soup(webXml,'lxml')
        href = webSoup.find("a",text=re.compile(u"概要|摘要"))['href']
        realUrl = host+href
        requestPara.append([realUrl,stockCode,date])
        codeSet.add(stockCode)
    else:
        continue

con = MySQLdb.connect(host="192.168.5.106",user="root",passwd="zunjiazichan123",db="app_data",charset='utf8')
cursor = con.cursor()
sql = "select code from hk_distribution where update_time>'{}'".format(datetime.datetime(year=now.year,month=now.month,day=now.day))
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.commit()
con.close()
codeExist = set()
for i in result:
    codeExist.add(i[0])

for paras in requestPara:
    try:
        url = paras[0]
        code = paras[1]
        date = paras[2]
        if code in codeExist:
            continue
        logger.info("url:{} code:{} date:{}".format(url,code,date))
        cmd = "java -cp /root/zhoujia/hkdistribution/seek-jar-with-dependencies.jar com.vip.seek.creeper.extraction.HKStockDistribution -c /apps/svr/seek/seek_creeper/seek_creeper_001_001/conf {} {} {} true".format(url,code,date)
        popen=subprocess.Popen(cmd.split(" "), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs,errs=popen.communicate()
        logger.info("标准输出：{}".format(outs))
        logger.error("标准错误：{}".format(errs))
    except Exception,e:
        logger.error(e,exc_info=True)
        continue