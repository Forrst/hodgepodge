#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-09-19 下午5:34
'''
import os
import re
os.chdir('../')
from db.mysql.SqlUtil import Mysql
import json
import pandas as pd
import datetime
db5_106 = Mysql("mysql5.106")
news = db5_106.execute("select news_time,junior_channel,rowkey,title,other_info,weight from news where info_type = 1 and news_time>='2018-08-01 00:00:00' and news_time< '2018-10-01 00:00:00' and weight>=1 order by news_time desc","app_data")

def getENWords(title):
    '''
    查找标题中的英文单词
    :param title:
    :return:
    '''
    enWords = []
    words = title.split(" ")
    for word in words:
        if word.isalpha():
            enWords.append(word)
    return enWords

def isCNTitle(title, other_info):
    '''
    通过标题和other_info判断是否为中文标题
    :param title:
    :param other_info:
    :return:
    '''

    otherinfo = json.loads(other_info)
    if "seekflag" in other_info:
        if otherinfo['seekflag'] == "seek":
            return True
        elif otherinfo['seekflag'] == "enseek":
            return False
        else:
            return True
    else:
        enwords = getENWords(title)
        cnwords = re.findall(u"[\u4e00-\u9fa5]", title)
        total = len(enwords) + len(cnwords)
        cnRate = len(cnwords) * 1.0 / total
        enRate = len(enwords) * 1.0 / total
        if cnRate >= enRate:
            return True
        else:
            return False

key_news = []
for i in news:
    news_time = i[0]
    junior_channel = i[1]
    rowkey = i[2]
    title = i[3]
    if not isCNTitle(title,i[4]):
        continue
    other_info_json = json.loads(i[4])
    weight = i[5]
    key_words = other_info_json['key_extract'] if 'key_extract' in other_info_json else ""
    navigation = other_info_json['navigation'] if 'navigation' in other_info_json else ""
    weekday = news_time.weekday()+1
    hour = news_time.hour
    key_news.append([weekday,hour,news_time,junior_channel,rowkey,title,key_words,navigation,weight])

title_df = pd.DataFrame(key_news,columns=['weekday','hour','news_time','junior_channel','rowkey','title','key_words','navigation','weight'])
title_df.to_csv("/home/eos/data/other/title_classfication1012.csv",encoding="utf8")