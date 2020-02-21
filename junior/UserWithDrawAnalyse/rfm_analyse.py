#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-01-02 下午5:43
'''
import MySQLdb
import pandas as pd

con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="app_data", charset="utf8")
rfm = pd.read_sql("select account,process_date,type,amount from rfm_base",con =con)

rfm_2week = rfm[(rfm.process_date>='2019-12-16')&(rfm.process_date<='2019-12-31')]
account_2week = rfm_2week.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_2week = account_2week.sort_values("account",ascending=True)

rfm_1month = rfm[(rfm.process_date>='2019-12-01')&(rfm.process_date<='2019-12-31')]
account_1month = rfm_1month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_1month = account_1month.sort_values("account",ascending=True)

rfm_2month = rfm[(rfm.process_date>='2019-11-01')&(rfm.process_date<='2019-12-31')]
account_2month = rfm_2month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_2month = account_2month.sort_values("account",ascending=True)

rfm_3month = rfm[(rfm.process_date>='2019-10-01')&(rfm.process_date<='2019-12-31')]
account_3month = rfm_3month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_3month = account_3month.sort_values("account",ascending=True)

rfm_6month = rfm[(rfm.process_date>='2019-07-01')&(rfm.process_date<='2019-12-31')]
account_6month = rfm_6month.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_6month = rfm_6month.sort_values("account",ascending=True)

account_total = rfm.groupby(["account",'type'],as_index=False).agg({"amount":'sum',"process_date":'count'})
account_total = account_total.sort_values("account",ascending=True)

account_info = pd.DataFrame(rfm.account.unique(),columns=['account'])

