#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-25 下午4:54
'''

from db.mysql.SqlUtil import Mysql
db = Mysql("mysql5.105")
import pandas as pd
from collections import namedtuple
sql = "select distinct product_id from product_ipo_announcement"
new_stocks = db.execute(sql,"jcbms")
new_stocks = set([i[0] for i in new_stocks])

sql = '''
    SELECT
        account_id,
        instrument_id
    FROM
        oms_history.trade
    WHERE
        trade_date > '20190314'
        group by account_id,instrument_id
    UNION
        (
            SELECT
                client_acc_id AS account_id,
                CODE AS instrument_id
            FROM
                oms_ju.trade_excution_01
            WHERE
                trading_day > '20190314'
            group by account_id,instrument_id
        )
    ORDER BY
        account_id asc
'''
account_stock = db.execute(sql)
account_stock_list = []
for i in account_stock:
    account = i[0]
    stock = i[1]
    account_stock_list.append([account,stock])
account_stock_df = pd.DataFrame(account_stock_list,columns=['account','stock'])
account_ = list(account_stock_df.account.unique())
def myiter(d, cols=None):
    if cols is None:
        v = d.values.tolist()
        cols = d.columns.values.tolist()
    else:
        j = [d.columns.get_loc(c) for c in cols]
        v = d.values[:, j].tolist()

    n = namedtuple('MyTuple', cols)

    for line in iter(v):
        yield n(*line)

def account_new_stock_comput(account):
    stock_trade = set(account_stock_df[account_stock_df['account']==account]['stock'])
    total = len(stock_trade)
    new_stock_count = len(stock_trade & new_stocks)
    return [account,total,new_stock_count]

account_stock_count = map(account_new_stock_comput,account_)
account_stock_df = pd.DataFrame(account_stock_count,columns=['account','total','new_stock_count'])
account_stock_df['new_stock_rate'] = account_stock_df['new_stock_count']*1.0/account_stock_df['total']
def getrate(x):
    m = list(account_stock_df[account_stock_df['account']==x]['new_stock_rate'])
    if len(m)==0:
        return ""
    else:
        return m[0]
map(getrate,)