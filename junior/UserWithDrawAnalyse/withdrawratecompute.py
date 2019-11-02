#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-25 上午9:46
'''
import logging
import pandas as pd
from db.mysql.SqlUtil import Mysql
from collections import namedtuple

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("withdraw_train.py")

db = Mysql("mysql5.105")

account_null_sql = '''
    SELECT
        a.account
    FROM
        (
            SELECT
                account,
                count(*) AS times
            FROM
                trade_user_money
            WHERE
                net_assets = withdraw
            AND withdraw = 0
            AND info_date <= '20190831'
            AND info_date >= '20190701'
            GROUP BY
                account
            HAVING
                times = 45
        ) a
    INNER JOIN (
        SELECT
            account,
            count(*) AS times
        FROM
            trade_user_money_backup_bak01
        WHERE
            net_assets = withdraw
        AND withdraw = 0
        AND info_date <= '20190631'
        AND info_date >= '20190601'
        GROUP BY
            account
        HAVING
            times = 20
    ) b ON a.account = b.account
'''
account_null = db.execute(account_null_sql,"miningtrade")
account_null_list = [i[0] for i in account_null]


account_6_sql = '''
SELECT
	account,
	net_assets,
	withdraw,
	info_date
FROM
	trade_user_money_backup_bak01
WHERE
info_date <= '20190631'
AND info_date >= '20190601'
AND account NOT IN ({})
ORDER BY
	info_date ASC
'''.format(str(map(str, account_null_list)).replace("[", "").replace("]", ""))
account_6 = db.execute(account_6_sql,"miningtrade")
account_6_list = []
for i in account_6:
    account = i[0]
    net_assets = i[1]
    withdraw = i[2]
    info_date = i[3]
    account_6_list.append([account,net_assets,withdraw,info_date])


account_78_sql = '''
SELECT
	account,
	net_assets,
	withdraw,
	info_date
FROM
	trade_user_money
WHERE
info_date <= '20190831'
AND info_date >= '20190701'
AND account NOT IN ({})
ORDER BY
	info_date ASC
'''.format(str(map(str, account_null_list)).replace("[", "").replace("]", ""))
account_78 = db.execute(account_78_sql,"miningtrade")
account_78_list = []
for i in account_78:
    account = i[0]
    net_assets = i[1]
    withdraw = i[2]
    info_date = i[3]
    account_78_list.append([account,net_assets,withdraw,info_date])


df6 = pd.DataFrame(account_6_list, columns=['account','net_assets','withdraw','info_date'])
df78 = pd.DataFrame(account_78_list,columns=['account','net_assets','withdraw','info_date'])
dataframe = pd.concat([df6,df78],axis=0,ignore_index=True)
dataframe = dataframe.fillna(method='ffill')

accounts = list(dataframe['account'].unique())


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


def comput_full_withdraw(account):
    account_user_money = dataframe[dataframe['account']==account]
    flag = False
    last = ""
    end = 0
    total = 0
    counter_true = 0
    total = 0
    for index,line in enumerate(myiter(account_user_money)):
        this = line
        if flag == False and (this.net_assets == this.withdraw and this.withdraw !=0) and (last == "" or total>0 or last.net_assets != last.withdraw or last.net_assets == last.withdraw==0):
            total+=1
            flag = True
            end = index+14
            continue
        if flag == True and index<=end and this.net_assets == this.withdraw and this.withdraw==0:
            counter_true+=1
            flag = False
        if flag == True and index==end:
            flag = False
        last = line
    ret = [account,total,counter_true]
    return ret
account_full_withdraw_ret = map(comput_full_withdraw,accounts)
account_full_withdraw_df = pd.DataFrame(account_full_withdraw_ret,columns=["account","total","full_withdraw"])
print account_full_withdraw_df['full_withdraw'].sum()*1.0/account_full_withdraw_df['total'].sum()