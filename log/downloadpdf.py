#!/usr/bin/python
#-*- coding:utf-8 -*-

import MySQLdb
import urllib
import time
from bs4 import BeautifulSoup as soup

con = MySQLdb.connect(host='192.168.5.105',user='root',passwd='zunjiazichan123',db='report',charset='utf8')
cursor = con.cursor()
sql = "select * from user_register_change_idfa where id = 4202280"
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.close()
def replaceNone(x):
    if x is None:
        return 'null'
    else:
        return x
map(replaceNone,a)

"insert into news(a,b,c,d) values (%s)"%(",".join(["%s"]*len(a)))


con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = '''
SELECT * 
FROM  `news` 
WHERE  `other_info` LIKE  '{"show_tags":["配发结果"],"category":"配发结果"}'
AND  `info_type` =7
order by update_time desc
'''
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.close()

urls = []
for i in result:
    try:
        src = i[13]
        html = None
        html = urllib.urlopen(i[13]).read()
        while html is None:
            time.sleep(1)
        href = soup(html,'lxml').findAll("a")[1]['href']
        url = src.split("/")[:-1]
        url.append(href[2:])
        urls.append("/".join(url))
    except Exception,e:
        print e
        print src
        continue
        
        
con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "update news_columns set other_regex = %s where id = %s"
cursor.executemany(sql,inserts)
cursor.close()
con.commit()
con.close()





