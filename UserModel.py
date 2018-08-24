#!/usr/bin/python
# -*- coding:utf-8 -*- 
"""
作者:eos
创建时间:2018-08-06 上午11:46
"""


import happybase
import hashlib
from db.mysql import SqlUtil

#获取dt=2018-07-31的用户阅读时长
date = "2018-07-01"
getAllUsers = "select a.user_id,name,phone_number,funds_account,sum(read_time) read_times from news_read_time a left join user_account_info b on a.user_id = b.user_id where read_day >= '%s' and funds_account is not null group by user_id having read_times >= 2 order by read_times desc"%date
all_user_read_time = SqlUtil.select(getAllUsers)

f = open("/home/eos/临时文件/user_info.csv","a+")
for i in all_user_read_time:
    ids = i[0].encode("utf8") if i[0] is not None else "None"
    name = i[1].encode("utf8") if i[1] is not None else "None"
    phone = i[2].encode("utf8") if i[2] is not None else "None"
    account = i[3].encode("utf8") if i[3] is not None else "None"
    readTime = str(i[4])
    line = "\t".join([ids,name,phone,account,readTime])+"\n"
    f.write(line)
    f.flush()
f.close()


user_info = SqlUtil.select("select name,phone_number from user_account_info where user_id = '29462'")

#港股募资额
'''
select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>'2018-01-01' order by IssueEndDate desc

select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>='2017-01-01' and to_char(IssueEndDate,'YYYY-MM-DD')<='2017-07-31' order by IssueEndDate des
'''



#前20的信息
# head20 = [i[0] for i in result[0:20]]
# sql_user_info = "select * from user_account_info where user_id = '16380'"
# user_info = getDataFrom5_105(sql_user_info)
#
# #获取具体点击的数据
# sql = "select distinct user_id,news_type,news_id,news_channel,start_time,stop_time,read_time,platform,channel,dt from news_read_time where dt = '2018-07-31' and user_id = '16380' order by start_time desc"
# result = getDataFrom5_105(sql)

#获取用户标签
user_id = "16380"
md5_user_id = hashlib.md5(user_id).hexdigest()[:16]

con = happybase.Connection("192.168.2.232")
con.open()
table = con.table("news")
ret = table.row("")
for i in ret:
    print i
con.close()