#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-01-02 下午5:43
'''
import datetime

import MySQLdb
import pandas as pd
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("---RFM MODEL---")

con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="app_data", charset="utf8")
rfm = pd.read_sql("select account,process_date,type,amount from rfm_base order by process_date desc",con =con)

con_105 = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
account_register = pd.read_sql("select funds_account as account,DATE_FORMAT(min(stat_time), '%Y-%m-%d') as time from user_account_info group by funds_account",con=con_105)
account_register.drop_duplicates(inplace=True)
# rfm_2week = rfm[(rfm.process_date>='2019-12-16')&(rfm.process_date<='2019-12-31')]
# account_2week = rfm_2week.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_2week = account_2week.sort_values("account",ascending=True)
#
# rfm_1month = rfm[(rfm.process_date>='2019-12-01')&(rfm.process_date<='2019-12-31')]
# account_1month = rfm_1month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_1month = account_1month.sort_values("account",ascending=True)
#
# rfm_2month = rfm[(rfm.process_date>='2019-11-01')&(rfm.process_date<='2019-12-31')]
# account_2month = rfm_2month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_2month = account_2month.sort_values("account",ascending=True)
#
# rfm_3month = rfm[(rfm.process_date>='2019-10-01')&(rfm.process_date<='2019-12-31')]
# account_3month = rfm_3month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_3month = account_3month.sort_values("account",ascending=True)
#
# rfm_6month = rfm[(rfm.process_date>='2019-07-01')&(rfm.process_date<='2019-12-31')]
# account_6month = rfm_6month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_6month = rfm_6month.sort_values("account",ascending=True)
#
# account_total = rfm.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
# account_total = account_total.sort_values("account",ascending=True)

account_info = pd.DataFrame(rfm.account.unique(),columns=['account'])
account_r = pd.merge(account_info,account_register,how='inner')

real_now = datetime.datetime.now()


def getRFM(account_row):
    account_row['recency'] = "null"
    account_row['frequency'] = "null"
    account_row['monetary'] = "null"
    try:
        x = account_row.account
        logger.info(f"comput for {x}")
        rtime = rfm[(rfm.account==x) & (rfm.type != 'cash_out') & (rfm.type != 'product_out')]['process_date'].max()
        r = (real_now.date() - datetime.datetime.strptime(rtime,'%Y-%m-%d').date()).days
        account_row['recency'] = r

        ftimes = len(rfm[(rfm.account==x) & (rfm.type != 'cash_out') & (rfm.type != 'product_out')])
        fstat_day = account_r[account_r.account==x].time.values[0]
        fdays = (real_now.date() - datetime.datetime.strptime(fstat_day,'%Y-%m-%d').date()).days
        f = ftimes*1.0/fdays
        account_row['frequency'] = f
        mamount = rfm[(rfm.account==x) & ((rfm.type == 'trade') | (rfm.type == 'ipo'))]
        mamount_trade = mamount[mamount.type=='trade'].amount.sum()
        mamount_ipo = mamount[mamount.type=='ipo'].amount.sum()*0.1
        mamount_total = mamount_trade+mamount_ipo
        m = mamount_total*1.0/fdays
        account_row['monetary'] = m
    except Exception as e:
        logging.error(f"error account {account_row}")
    return account_row

account_rfm = account_r.apply(lambda x:getRFM(x),axis=1)
account_rfm = account_rfm[account_rfm.monetary!='null']
account_rfm.to_csv("account_rfm.csv",encoding="utf-8")
level_dict = {
    'high_high_high':"重要价值客户",
    'high_low_high':'重要发展客户',
    'low_high_high':'重要保持客户',
    'low_low_high':'重要挽留客户',
    'high_high_low':'一般价值客户',
    'high_low_low':'一般发展客户',
    'low_high_low':'一般保持客户',
    'low_low_low':'一般挽留客户'

}
levelabc_dict = {
    'high_high_high':"A级",
    'high_low_high':'A级',
    'low_high_high':'B级',
    'low_low_high':'B级',
    'high_high_low':'B级',
    'high_low_low':'B级',
    'low_high_low':'C级',
    'low_low_low':'C级'

}


def comput_weight(account_row):
    if account_row.recency<=40:
        account_row['recency_level'] = 'high'
    elif account_row.recency>40:
        account_row['recency_level'] = 'low'
    if account_row.frequency>=0.11233885819521179:
        account_row['frequency_level'] = 'high'
    elif account_row.frequency<0.11233885819521179:
        account_row['frequency_level'] = 'low'
    if account_row.monetary>=274.33905949253733:
        account_row['monetary_level'] = 'high'
    elif account_row.monetary<274.33905949253733:
        account_row['monetary_level'] = 'low'
    level = account_row['recency_level']+"_"+account_row['frequency_level']+"_"+account_row['monetary_level']
    account_row['customer_level'] = level_dict[level]
    account_row['customer_abc_level'] = levelabc_dict[level]
    return account_row

account_rfm_level = account_rfm.apply(lambda x:comput_weight(x),axis=1)
account_rfm_level.to_csv("account_rfm_level.csv")