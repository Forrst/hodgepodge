#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-08-07 下午3:28
'''
import os
import requests
import MySQLdb
import logging

from dateutil.relativedelta import relativedelta

os.chdir("/home/eos/git/hodgepodge/")
from db.mysql.SqlUtil import Mysql
db = Mysql("mysql5.106")


logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("hkbillsMonth.py")

# process_date = '2019-11-05'
# sql = '''
# SELECT
# account_id
# FROM
# product_ipo_app
# WHERE
# product_id IN (
#     '01987',
#     '02163',
#     '01379',
#     '01870',
#     '01922',
#     '03603',
#     '01921',
#     '01501',
#     '02231',
#     '08645'
# )
# GROUP BY
# account_id
# '''
#
# accounts = db.execute(sql,db='jcbms')
# account_ = [i[0] for i in accounts]
# for i in account_:
#     account = i
#     sql = '''
#     delete from hk_bills where code = '{}' and bill_date = '201911' and bill_type = 'monthly'
#     '''.format(account)
#     db.execute(sql,'app_data')

# logger.info("Total {} email failure accounts:".format(len(account_)))
# index = account_.index("61808878")
# account__ = account_[index+1:]
#
# # head = account_[:index+1]
# # for a in account__:
# #     sqldelete = '''
# # delete from report.hkbill_email where account_id = '{}' and bill_date = '201911'
# # '''.format(a)
# #     db.execute(sqldelete,'jcbms')
# #     sqlset = '''
# #     insert into report.hkbill_email set account_id
# #     '''
#
# for accountid in account__:
#     try:
#         r = requests.get("http://192.168.5.54:8089/createPdfbyMonthForAccount.html?account_id={}&day={}&sendEmail=true".format(accountid,process_date))
#         if r.status_code == 200:
#             logger.info("{} success".format(accountid))
#         else:
#             logger.info("{} failure".format(accountid))
#     except Exception,e:
#         logger.error("error accountid: {}".format(accountid))
#         logger.error(e,exc_info = True)

#导出列表用户历史结单
import datetime
account_str = '''
66027528
66019188
66010708
66011158
66010798
66010838
66011128
66011178
66011218
66027298
66011098
66019928
66027708
66027718
66011658
66010918
66010758
66010188
66016978
66010568
66010958
66011038
66010668
66010788
66011278
66019018
66018958
66018938
66018838
66018658
66018988
66018978
66018628
66017678
66016998
66010108
66009978
66017108
66011298
66017088
66010268
66017138
66011238
66010778
66010968
66011518
66017628
66010128
66010158
66019178
66019008
66012268'''
import datetime
import shutil
(datetime.date.today() - relativedelta(months=+1))
end_date = datetime.datetime(year=2019,month=11,day=1)
start_date = datetime.datetime(year=2017,month=11,day=1)
t = start_date
dates = []
while t<=end_date:
    dates.append(t.strftime('%Y%m'))
    t = t+relativedelta(months=+1)

path_prefix = "/data/estatement/"
accounts = account_str.split("\n")
for account in accounts[1:]:
    for date in dates[::-1]:
        path = date+"/"+account+"_"+date+"_0M.pdf"
        real_path = path_prefix+"monthly/"+path
        if date>'201910':

            real_path = path_prefix+"monthly_new/"+path
        if os.path.exists(real_path):
            print(real_path)
            shutil.copy(real_path,"/data/estatement/3680/")

dirs = os.listdir("/data/estatement/monthly_new/201909")
for account in accounts[1:]:
    filename = account+"_201909_0M.pdf"
    for index in range(len(dirs)):
        dir = dirs[index]
        if filename in dir:
            print(filename)
            shutil.copy("/data/estatement/monthly_new/201909/"+dir,"/data/estatement/3680/")

