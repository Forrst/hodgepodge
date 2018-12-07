import MySQLdb
import happybase
import hashlib
def getDataFrom5_105(sql):
    con = MySQLdb.connect(host='192.168.5.105',user='root',passwd='zunjiazichan123',db='report',charset='utf8')
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.commit()
    con.close()
    return result

#获取dt=2018-07-31的用户阅读时长
def getUserReadTimeDesc(date):
    sql = "select user_id,sum(read_time) read_times from news_read_time where read_day >= '%s' group by user_id order by read_times desc"%date
    return getDataFrom5_105(sql)

getUserReadTimeDesc

sql = "select * from news_read_time where news_type=19 and news_channel = '(null)' order by read_time desc"
result = getDataFrom5_105(sql)


user_info = getDataFrom5_105("select name,phone_number from user_account_info where user_id = '29462'")

#港股募资额
select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>'2018-01-01' order by IssueEndDate desc

select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>='2017-01-01' and to_char(IssueEndDate,'YYYY-MM-DD')<='2017-07-31' order by IssueEndDate des

#前20的信息
head20 = [i[0] for i in result[0:20]]
sql_user_info = "select * from user_account_info where user_id = '16380'"
user_info = getDataFrom5_105(sql_user_info)

#获取具体点击的数据
sql = "select distinct user_id,news_type,news_id,news_channel,start_time,stop_time,read_time,platform,channel,dt from news_read_time where dt = '2018-07-31' and user_id = '16380' order by start_time desc"
result = getDataFrom5_105(sql)

#获取用户标签
user_id = "16380"
md5_user_id = hashlib.md5(user_id).hexdigest()[:16]

import happybase
con = happybase.Connection("192.168.2.232")
con.open()
table = con.table("deep_read")
ret = table.row("c04364f803ca5285259d1799ee85960b".decode("hex"))
for i in ret:
    print i
con.close()
