#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-18 下午5:20
'''

import MySQLdb
import os
os.chdir("/home/eos/git/hodgepodge")
import logging
import pandas as pd


logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger(" RFM ")


#获取用户ipo申购的记录
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
ipo_sql = '''
SELECT
	ID,
	CLIENT_ACC_ID,
	AMOUNT
FROM
	product_ipo_app
'''
ipo_info = pd.read_sql(ipo_sql,con)
logger.info(f"oms 2019-08-06以前的ipo申购记录总条数 {len(ipo_info)}")

def str_to_date(x):
    x_list = list(x)
    x_list.insert(4,"-")
    x_list.insert(7,"-")
    return "".join(x_list)
ipo_info['process_date'] = ipo_info.apply(lambda x:str_to_date(x['ID'][:8]),axis=1)

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
ipo_bms_sql = '''
SELECT
	account_id as account,
	amount,
	created_time
FROM
	product_ipo_app
WHERE
	created_time>'2019-08-06 23:59:59'
	and created_time<='2019-12-20 23:59:59'
order by created_time asc
'''
ipo_bms_info = pd.read_sql(ipo_bms_sql,con)
logger.info(f"total ipo info {len(ipo_bms_info)}")
ipo_bms_info['process_date'] = ipo_bms_info.apply(lambda x:str(x['created_time'].date()),axis=1)

ipo_info.columns = ['id', 'account', 'amount', 'process_date']
ipo_df = pd.concat([ipo_info[['account', 'amount', 'process_date']],ipo_bms_info[['account', 'amount', 'process_date']]])

account_ipo = ipo_df.sort_values(by=['account','process_date'],ascending=True)
account_ipo = account_ipo.reset_index(drop=True)
account_ipo['type'] = 'ipo'


#获取用户交易记录
trade_sql = '''
select process_date,account_id,sum(exchange_rate*trade_amount) as day_trade_amount from account_trade where process_date>'2019-07-16' and process_date<='2019-12-20' group by process_date,account_id
'''
trade_info = pd.read_sql(trade_sql,con)

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="oms_ju", charset="utf8")
trade_excution_sql = '''
SELECT
	client_acc_id,
	sum(
		qty * price * (CASE
		WHEN exchange = 1 THEN
			1
		WHEN exchange = 2 THEN
			7.7983
		WHEN (exchange = 3 OR exchange = 4) THEN
			1.1127 end)
	) as day_trade_amount,
	trading_day
FROM
	trade_excution_01
GROUP BY
	client_acc_id,
	trading_day
'''
trade_excution_info = pd.read_sql(trade_excution_sql,con)
trade_excution_info.columns = ['account', 'amount', 'process_date']
trade_excution_info['process_date'] = trade_excution_info.apply(lambda x:str_to_date(str(x['process_date'])),axis=1)

trade_info = trade_info[['account_id',  'day_trade_amount','process_date']]
trade_info.columns = ['account', 'amount', 'process_date']
trade_info_df = pd.concat([trade_excution_info,trade_info])
trade_info_df = trade_info_df.sort_values(by=['account','process_date'],ascending=True)
trade_info_df = trade_info_df.reset_index(drop = True)
trade_info_df['type'] = 'trade'

# account_ipo.to_csv("account_ipo.csv")
# trade_info_df.to_csv("trade_info_df.csv")
rfm_df = pd.concat([account_ipo,trade_info_df])
rfm_df = rfm_df.sort_values(by=['account','process_date','type'],ascending=True)
rfm_df = rfm_df.reset_index(drop = True)

#获取存取款数据
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
cash_flow_sql = '''
select process_date,account_id,amount,purpose from cash_flow where process_date<='2019-12-20' and (remark like "%存款%" or remark like "%提款%") and status = 'Confirmed'
'''
cash_flow_info = pd.read_sql(cash_flow_sql,con)
cash_type_dict = {1:"cash_in",2:"cash_out"}
cash_flow_info['type'] = cash_flow_info.apply(lambda x:cash_type_dict[x['purpose']],axis=1)
cash_flow_info = cash_flow_info[['account_id','amount','process_date','type']]
cash_flow_info.columns = ['account', 'amount', 'process_date','type']
cash_flow_info = cash_flow_info.reset_index(drop = True)

rfm_df = pd.concat([rfm_df,cash_flow_info])
rfm_df['process_date'] = rfm_df[['process_date']].astype('str')
rfm_df = rfm_df.sort_values(by=['account','process_date'],ascending=True)
rfm_df = rfm_df.reset_index(drop = True)
rfm_df['amount'] = rfm_df['amount'].astype('float')

#存数据
from sqlalchemy import create_engine
engine = create_engine("mysql://root:zunjiazichan123@192.168.5.106/app_data",encoding="utf8",echo=False)
rfm_df.to_sql("rfm_base",con=engine,index=False, if_exists='append')


#获取转入转出数据
product_io_sql = '''
select process_date,account_id as account,quantity as amount,product_id,purpose from product_flow where process_date<="2019-12-20" and (remark like "%转入%" or remark like "%转出%")
'''

product_info = pd.read_sql(product_io_sql,con=con)
product_type_dict = {1:"product_in",2:"product_out"}
product_info['type'] = product_info.apply(lambda x:product_type_dict[x['purpose']],axis=1)
product_info['desc'] = product_info['product_id']
product_info = product_info[['account', 'amount', 'process_date','type','desc']]
product_info['amount'] = product_info['amount'].astype("float")
product_info['process_date'] = product_info['process_date'].astype("str")
product_info = product_info.sort_values(by=['account','process_date'],ascending=True)
product_info = product_info.reset_index(drop = True)

engine = create_engine("mysql://root:zunjiazichan123@192.168.2.231/trade_data",encoding="utf8",echo=False)
product_info.to_sql("",con=engine,index=False, if_exists='append')

#分析
# con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="app_data", charset="utf8")
# rfm = pd.read_sql("select account,process_date,type from rfm_base",con =con)
