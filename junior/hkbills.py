#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-08-07 下午3:28
'''
import requests
import logging
import os
os.chdir("/home/eos/git/hodgepodge")
from db.mysql.SqlUtil import Mysql
db = Mysql("mysql5.105")

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("hkbills.py")

# process_date = str(datetime.datetime.now().date())
process_date = '2020-06-08'
sql1 = "SELECT a.`short_name`,b.`account_id` FROM (select account_id from cash_flow where process_date = '{}') b INNER JOIN account_profile a ON a.`account_id`=b.`account_id` GROUP BY b.`account_id`".format(process_date)

sql2 = "SELECT a.`short_name`,b.`account_id` FROM (select account_id from product_flow where process_date = '{}') b INNER JOIN account_profile a ON a.`account_id`=b.`account_id` GROUP BY b.`account_id`".format(process_date)

sql3 = "SELECT a.`short_name`,b.`account_id` FROM (select account_id from account_trade where process_date = '{}') b INNER JOIN account_profile a ON a.`account_id`=b.`account_id` GROUP BY b.`account_id`".format(process_date)

sql4 = '''
SELECT 
 f.short_name, 
 e.account_id 
 FROM 
 ( 
  SELECT 
   account_id 
  FROM 
   ( 
    SELECT 
     * 
    FROM 
     corp_action 
    WHERE 
     posted_date = '{}'
   ) a 
  INNER JOIN ( 
   SELECT 
    * 
   FROM 
    corp_action_detail 
  ) b ON a.event_id = b.event_id 
  AND a.product_id = b.product_id 
  LEFT JOIN product c ON a.market_id = c.list_market_id 
  AND a.product_id = c.product_id 
 ) e 
 INNER JOIN account_profile f ON e.account_id = f.account_id
'''.format(process_date)


accountIdList = set()
accounts1 = db.execute(sql1,"jcbms")
for i in accounts1:
    accountIdList.add(i[1])
accounts2 = db.execute(sql2,"jcbms")
for i in accounts2:
    accountIdList.add(i[1])
accounts3 = db.execute(sql3,"jcbms")
for i in accounts3:
    accountIdList.add(i[1])
accounts4 = db.execute(sql4,"jcbms")
for i in accounts4:
    accountIdList.add(i[1])




#补发product_ipo_app未录入的
accounts = db.execute("select account_id from hkbill_email where bill_date = '{}' and status = 'success'".format(process_date),"report")
email_account = set([i[0]for i in accounts])

account_ = accountIdList-email_account
logger.info("Total {} email failure accounts:".format(len(account_)))

for accountid in account_:
    try:
        r = requests.get("http://192.168.5.54:8089/pdf.html?accountId={}&day={}".format(accountid,process_date))
        if r.status_code == 200:
            logger.info("send hkbil email for {} success for bill_date {}".format(accountid,process_date))
        else:
            logger.info("send hkbil email for {} failure for bill_date {}".format(accountid,process_date))
    except Exception as e:
        logger.error("error accountid: {} for bill_date {}".format(accountid,process_date))
        logger.error(e,exc_info = True)

'''
sql = "delete from hk_bills where bill_date = '20190806' and code in ({})".format(",".join(['%s']*len(account)))
con = MySQLdb.connect(host="192.168.5.106",user="root",passwd="zunjiazichan123",db="app_data",charset='utf8')
cursor = con.cursor()
cursor.execute(sql,account)
result = cursor.fetchall()
cursor.close()
con.commit()
con.close()

#查看漏发邮件的
sql = "select account_id from hkbill_email where bill_date = '2019-08-07'"
accounts = db.execute(sql,"report")
account = [i[0]for i in accounts]
'''

'''
补发某人一段时间的结单
'''
def sendforperiod():
    import datetime
    accountid = '60119608'
    start = datetime.datetime(year=2020,month=4,day=8)
    while True:
        process_date = str(start)[:10]
        if process_date == '2020-05-16':
            break
        try:
            r = requests.get(f"http://192.168.5.54:8089/pdfonly.html?accountId={accountid}&day={process_date}&sendEmail=true")
            if r.status_code == 200:
                logger.info(f"{accountid} success {process_date}")
            else:
                logger.info(f"{accountid} failure {process_date}")
            start += datetime.timedelta(days=1)
        except Exception as e:
            start += datetime.timedelta(days=1)
            logger.error(f"error accountid: {accountid} @ {process_date}")
            logger.error(e,exc_info = True)


##############


