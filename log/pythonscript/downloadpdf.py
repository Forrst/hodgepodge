#!/usr/bin/python
#-*- coding:utf-8 -*-

import MySQLdb

con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select * from news where info_type= 7 and other_info like '{'show_tags':['配发结果'],'category':'配发结果'}' order by update_time desc limit 10"
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.close()
