#!/usr/bin/python
#-*- coding:utf-8 -*-
import json
import MySQLdb
import pandas as pd
#from apr import *
from numpy import *

con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select rowkey,orig_tags,other_info from news where info_type= 1 and weight>0 and is_deleted = 0 and news_time>='2018-03-07'"
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.close()

'''
处理标签列表,变成集合
'''
def tagExtract(dictTag):
    ret = set()
    for tag in dictTag.keys():
        tagTmp = tag
        if dictTag[tag]<0.1:
            continue
        if ";" in tag:
            tagTmp = getJunior(tag)
        if tagTmp != "":
            ret.add(tagTmp)
    return ret

def getJunior(tag):
    ret = ""
    for t in tag.split(";"):
        if t[:6]== 'JUNIOR':
            ret = t
            break
    return ret   


if __name__=="__main__":
    rawData = []
    for rowkey,tag,other_info in result:
        try:
            if "seekflag" in json.loads(other_info).keys() and json.loads(other_info)["seekflag"] == "enseek":
                continue
            dictTag = json.loads(tag)
            info = tagExtract(dictTag)
            if len(info)<1:
                continue
            rawData.append([rowkey,info])
        except Exception,e:
            print "error "+rowkey,tag
    Data = []
    for i in rawData:
        Data.append(list(i[1]))

tagDict = {}
for i in Data:
    for j in i:
        if j in tagDict:
            tagDict[j] = tagDict[j]+1
        else:
            tagDict[j] = 1
df = pd.DataFrame.from_dict(tagDict,orient='index')
df = df.sort_values(0,ascending=False)
df.to_csv("tag.csv",encoding='utf-8')

removeSet = set()
for i in tagDict.keys():
    if tagDict[i] <6:
        removeSet.add(i)

Data_filtered = []
for i in Data:
    row = i
    for j in i:
        if j in removeSet:
            row.remove(j)
    Data_filtered.append(row)

tagOver2 = []
for i in Data_filtered:
    if len(i)>=2:
        tagOver2.append(i)

#len(tagOver2) = 269398

   
minSupport = 0.00025
conf = 0.3
#L,supportData = apriori(tagOver2,minSupport=minSupport)
#brl=generateRules(L, supportData,conf)
#print 'brl:',brl
16.24





'''
SQL
'''
industry = "select exchange,code,name from industry"
concept = "select exchange,code,name from concept"
industry_hk = "select exchange,code,name from industry_hk"
concept_hk = "select exchange,code,name from concept_hk"
stock_info = "select exchange,code,name from stock_info"
stock_info_hk = "select exchange,code,name from stock_info_hk"

tag_code_name = "select code,name from tag_code_name"
macro = "select exchange,code,name from macro"

'''
SQL_US
''' 
stock_info_us = "select exchange,code,chname from stock_info_us"
industry_us = "select exchange,code,chname from industry_us"

Name = {}

def insertName(Name):
    getCodeName(execute(industry),Name)
    getCodeName(execute(concept),Name)
    getCodeName(execute(industry_hk),Name)
    getCodeName(execute(concept_hk),Name)
    getCodeName(execute(stock_info),Name)
    getCodeName(execute(stock_info_hk),Name)
    getCodeName(executeUs(industry_us),Name)
    getCodeName(executeUs(stock_info_us),Name)
    getCodeName(execute(tag_code_name),Name)
    getCodeName(execute(macro),Name)
    
def getCodeName(sqlRet,Name):
    if len(sqlRet[0])==3:
        for i in sqlRet:
            Name[i[0]+"_"+i[1]] = i[2]
        return Name
    elif len(sqlRet[0])==2:
        for i in sqlRet:
            Name[i[0]] = i[1]
        return Name

def execute(sql):
    con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return result

def executeUs(sql):
    con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='us_data',charset='utf8')
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return result

insertName(Name)


'''
Support = {}
for i in supportData.keys():
    if supportData[i]>=minSupport:
        Support[i] = supportData[i]
'''

def getName(codes):
    name = []
    for i in codes:
        try:
            name.append(Name[i])
        except Exception,e:
            continue
    return "&".join(name)
    

def getCodes(frozensets):
    ret = []
    for i in frozensets:
        for j in i.split(";"):
            ret.append(j)
    ret.sort()
    return ret

ret = []
for i in brl:
    confidence = i[2]
    a = getCodes(i[0])
    b = getCodes(i[1])
    code = "&".join(a)+"->"+"&".join(b)
    name_a = getName(a)
    name_b = getName(b)
    name = name_a+"->"+name_b
    support = supportData[i[0]]
    level = max(len(i[0]),len(i[1]))+1
    ret.append([code,name,confidence,support,level])

for i in supportData.keys():
    if supportData[i]>=minSupport and len(i) ==1:
        support = supportData[i]
        a = getCodes(i)
        code = "&".join(a)
        name = getName(a)
        confidence = 1
        level = 1
        ret.append([code,name,confidence,support,level])

uniqueRet = []
for i in ret:
    if i in uniqueRet:
        continue
    uniqueRet.append(i)
    con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='classification',charset='utf8')
    cursor = con.cursor()
    sql = "insert into recommend_tags(code,name,confidence,support,level) values('%s','%s','%f','%f','%d')"%(i[0],i[1],i[2],i[3],i[4])
    cursor.execute(sql)
    cursor.close()
    con.commit()
    con.close()

sql = "select title,other_info,rowkey from news where junior_channel like '%港股%' and info_type = 1 and news_time >'2018-03-01' order by news_time desc"
con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
cursor.execute(sql)
news = cursor.fetchall()
cursor.close()
con.close()

hknews = []
counter = 0
web = set()
for i in news:
    title = i[0]
    other_info = json.loads(i[1])
    rowkey = i[2]
    try:
        website = other_info['website']
    except Exception,e:
        website = "unknown_website"
    navigation = ""

    if 'navigation' in other_info:
        flag = "yes"
        words = other_info['navigation'].split("  ")
        if words[-1] == u"正文":
            navigation = words[-2]
        else:
            navigation = words[-1]
    else:
        flag = "no"
        navigation = i[1]
    counter+=1
    web.add(website+"\t"+other_info['navigation'])
    '''if counter<1000:
        print flag,rowkey,website,navigation,title'''
    hknews.append([title,flag,navigation])    
        

