#!/usr/bin/python
#-*- coding:utf-8 -*-

import MySQLdb
import json

lines = open("hkclassificationrules.txt","r+").read().split("\n")
lines.remove("")
inserts = []
for j,i in enumerate(lines):
    columns = i.split("\t")
    inserts.append([columns[0].decode("utf8"),columns[1].decode("utf8"),columns[2].decode("utf8"),columns[3].decode("utf8")])
        


con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "insert into news_columns(website,other_regex,column_name,column_code,parent_channel,classification,type,source_url) values(%s,%s,%s,%s,'港股','港股',1,'')"
cursor.executemany(sql,inserts)
cursor.close()
con.commit()
con.close()

a = set()
a.add(768)
a.add(6502)
a.add(50942)
a.add(1364)
a.add(251521)
import MySQLdb
con = MySQLdb.connect(host='192.168.5.105',user='root',passwd='zunjiazichan123',db='report',charset='utf8')
cursor = con.cursor()
sql = "insert into user_cf_all (from_user_id,to_user_id,distance) values (6502,768,0.0001)"
cursor.execute(sql)
sql = "insert into user_cf_all (from_user_id,to_user_id,distance) values (768,6502,0.0001)"
cursor.execute(sql)
con.commit()
con.close()
counter = 0
for to_user_id in d:
    print counter
    counter+=1
    sql = "select from_user_id from user_cf_all where to_user_id = %s order by distance asc limit 500"%to_user_id
    cursor.execute(sql)
    ret = cursor.fetchall()
    b = set()
    for i in ret:
        b.add(i[0])
    if len(set(ret) & a) != 0:
        print to_user_id,set(ret) & a
cursor.close()
con.close()


con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select create_time,rowkey,junior_channel,column_code,title,other_info from news where junior_channel like '%港股%' and create_time>='2018-07-25 15:30:00' and junior_channel not in ('港股公告','港股热点','港股研报')"
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.commit()
con.close()

ret = []
for i in result:
    other_info = json.loads(i[-1])
    website = ""
    navigation = ""
    classification = i[3]
    if 'website' in other_info:
        website = other_info['website']
    if 'navigation' in other_info:
        navigation = other_info['navigation']
    if i[3] is None:
        classification = "None"
    ret.append([str(i[0]),i[1],i[2],classification,i[4],website,navigation])

f = open("/home/eos/下载/港股分类/zhuanlan.csv",'a+')
for i in ret:
    try:
        line = "\t".join(i).encode("utf8")+"\n"
        f.write(line)
    except Exception,e:
        print e
        print i
f.close()

import happybase
import MySQLdb

connection1 = happybase.Connection("192.168.2.231")
connection1.open()
table1 = connection1.table("news")
con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select * from news where junior_channel like '%港股%' and create_time>='2018-07-27 15:25:00' and junior_channel not in ('港股公告','港股热点','港股研报') and `column_code` = ''"
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.commit()
con.close()

def printData(data):
    url = "info:url"
    junior_channel = "info:junior_channel"
    website = "info:website"
    parentUrl = "info:parentUrl"
    navigation = "info:navigation"
    if url in data:
        print "url: ",data[url]
    if junior_channel in data:
        print "junior_channel: ",data[junior_channel]
    if website in data:
        print "website: ",data[website]
    if parentUrl in data:
        print "parentUrl",data[parentUrl]
    if navigation in data:
        print "navigation: ",data[navigation]
    print "----------------------------------"

print "Total error :"+str(len(result))
for i in result:
    rowkey = i[6]
    data = table1.row(rowkey.decode("hex"))
    print "rowkey: ",rowkey
    printData(data)



"select column_code,count(column_code) from news where junior_channel like '%港股%' and create_time>='2018-07-25 15:30:00' and junior_channel not in ('港股公告','港股热点','港股研报') group by column_code"

import happybase
#线下数据库http://192.168.2.231/pma/index.php
#  192.168.2.231 root/zunjiazichan123

con = happybase.Connection("192.168.2.232")
con.open()
table = con.table("news")
data = table.row("d0df7ffffe9ade954b59f830053f0000".decode("hex"))

import MySQLdb
con = MySQLdb.connect(host='',user='',passwd='',db='report',charset='utf8')
cursor = con.cursor()
sql = "update user_ab_test set user_type = 'B' where user_id in %s"%b
sql = sql.replace("[","(").replace("]",")")
cursor.execute(sql)
ret = cursor.fetchall()
cursor.close()
con.commit()
con.close()

import happybase
con = happybase.Connection("192.168.5.156")
con.open()
table = con.table("news")

r= table.row('a2897ffffe96b96cb6cc9b7f615e0000'.decode("hex"))
for i in r:
    print i,r[i]