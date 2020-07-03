#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-06-19 上午9:08
'''
from __future__ import absolute_import
import string


from db.mysql.SqlUtil import Mysql
import pandas as pd
import MySQLdb
db = Mysql("mysql5.105")
con = MySQLdb.connect(host="192.168.5.105",user="root",passwd="zunjiazichan123",db="jcbms",charset='utf8')

#年龄 性别
base_info_sql = '''
select account_id,short_name,birth_day,gender,country from jcbms.account_profile
'''
base_info = pd.read_sql(con=con,sql=base_info_sql)


#持有的美元资产
account_balance_sql = '''
SELECT
	account_id,
	currency,
	equity_balance
FROM
	account_balance
WHERE
	process_date = '20200618'
AND currency = 4
AND equity_balance != 0
ORDER BY
	account_id DESC
'''

balance_info = pd.read_sql(account_balance_sql,con)


#交易次数 交易金额
account_trade_sql = '''
select a.*,TIMESTAMPDIFF(DAY,a.start,a.end) as timedelta from (select account_id,count(*) as times,sum(trade_amount) total_trade_amount,sum(quantity) as total_trade_quantity,min(process_date) as start,max(process_date) as end from account_trade where process_date<='20200618' and (market_id = 2 or market_id = 18) GROUP BY account_id ORDER BY times desc) a
'''

trade_info = pd.read_sql(account_trade_sql,con)

def balance(x):
    print(x.account_id)
    ret = balance_info[balance_info['account_id']==x.account_id]['equity_balance']
    value = 0
    if len(ret.values)!=0:
        value = ret.values[0]
    return value
trade_info['balance'] = trade_info.apply(balance,axis=1)


#港股新股
hk_new_stock_sql = '''
select product_id from product_ipo_app where list_market_id = 1 GROUP BY list_market_id,product_id
'''


hk_new_stock = pd.read_sql(hk_new_stock_sql,con)
stock_set = set(hk_new_stock.product_id)

mysqlcon = MySQLdb.connect(host="192.168.5.105",user="root",passwd="zunjiazichan123",db="jcbms",charset='utf8')
cursor = mysqlcon.cursor()

def has_hk_new_stock(x):
    print(x.account_id)
    trade_hk_new_stock = False
    trade_hk_stock = False
    sql = f'''
    select product_id from account_trade where account_id ='{x.account_id}' and market_id=1 group by account_id,product_id
    '''
    cursor.execute(sql)
    product_tuple = cursor.fetchall()
    if len(product_tuple)>0:
        trade_hk_stock = True
    stocks = set(i[0] for i in product_tuple)
    common = stocks & stock_set
    if len(common)>0:
        trade_hk_new_stock = True
    return [trade_hk_stock,trade_hk_new_stock]
trade_info['trade_stock'] = trade_info.apply(lambda x:has_hk_new_stock(x),axis=1)

trade_info['trade_hk_stock'] = trade_info.apply(lambda x:x.trade_stock[0],axis=1)
trade_info['trade_hk_new_stock'] = trade_info.apply(lambda x:x.trade_stock[1],axis=1)

account_info = pd.merge(trade_info,balance_info,how='left',on='account_id')
account = pd.merge(account_info,base_info,how='left',on='account_id')
cursor.close()
mysqlcon.close()

#最多人交易的股票
account_trade_unique_sql = '''
select a.product_id,count(*) as account_unique from (select account_id,product_id from account_trade where process_date<='20200618' and market_id in (2,16) GROUP BY account_id,product_id) a GROUP BY a.product_id order by account_unique desc
'''

account_trade_unique = pd.read_sql(account_trade_unique_sql,con)


#交易总金额最多的股票
stock_trade_sql = '''
select product_id,sum(net_amount) as total_net_amount from account_trade where process_date<='20200618' and market_id in (2,16) GROUP BY product_id ORDER BY total_net_amount desc
'''

stock_amount = pd.read_sql(stock_trade_sql,con)



#交易笔数最多的股票
stock_times_sql = "select product_id,count(*) as times from account_trade where market_id in (2,16) GROUP BY product_id ORDER BY times desc"
stock_times = pd.read_sql(stock_times_sql,con)

asrank = pd.merge(account_trade_unique,stock_amount,how="left")
rank = pd.merge(asrank,stock_times,how='left')


#19：00：00-05：00：00
orders_sql = '''
select insert_time,insert_date,LPAD(insert_time,9,0) as real_time from orders where market_id = 3 and `status` !=7 order by insert_time asc

'''
cons = MySQLdb.connect(host="192.168.5.105",user="root",passwd="zunjiazichan123",db="oms_history",charset='utf8')

orders_time = pd.read_sql(orders_sql,cons)

real_time = orders_time.real_time.tolist()
real_date = orders_time.insert_date.tolist()
#20200309-20201103夏令时  21:30开盘
#20191104-20200308冬令时  22:30开盘
#20190311-20191103夏令时  21:30开盘
#20181105-20190310冬令时  22:30开盘
#20180312-20181104夏令时  21:30开盘


def time_period(x):
    hour = x[:2]
    minute = x[2:4]
    if minute<'30':
        return hour+"00"
    else:
        return hour+'30'

real_time_period = list(map(time_period,real_time))
orders_time['period'] = real_time_period

def which_period(x):
    x = str(x)
    if '20200309'<=x<='20201103' or '20190311'<=x<='20191103' or '20180312'<=x<='20181104':
        return  True
    else:
        return False


orders_time['summer'] = orders_time.apply(lambda x:which_period(x['insert_date']),axis=1)

def real_time_(x):
    hour = x.period[:2]
    if x.summer == False:
        if hour=='00':
            hour='23'
        else:
            hour = str(int(hour)-1).zfill(2)
    result = hour+x.period[2:]
    return result

orders_time['real_time_'] = orders_time.apply(lambda x:real_time_(x),axis=1)
orders_time.groupby('period',as_index=False).count()

import math
def fl(x):
    print(x.account_id)
    if x.times_permonth==math.inf:
        return 0
    return math.floor(x.times_permonth)
account['times_p'] = account.apply(lambda x:fl(x),axis=1)
account.groupby("times_p",as_index=False).count().to_csv("permonth.csv")