#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-19 上午10:30
'''
import logging
import time
import pandas as pd
from db.mysql.SqlUtil import Mysql
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("withdraw_train.py")
db = Mysql("mysql5.105")
withdraw_info = pd.read_csv("withdraw_info", index_col=0)
accounts = list(withdraw_info['account'].unique())

sql = '''
SELECT
	account,
	net_assets,
	withdraw,
	stock_assets,
	info_date
FROM
	trade_user_money
WHERE
    info_date>='20190628'
    and info_date<='20190712'
	and account IN ({})
'''.format(str(map(str, accounts)).replace("[", "").replace("]", ""))
start = time.time()
data = db.execute(sql,"miningtrade")
end = time.time()
logger.info("数据库中总共取出的有效用户数为{}，花费{}秒!".format(len(data),end-start))

#形成dataframe
account_info = []
for i in data:
    account = i[0]
    net_assets = i[1]
    withdraw = i[2]
    stock_assets = i[3]
    info_date = i[4]
    account_info.append([account,net_assets,withdraw,stock_assets,info_date])
dfna = pd.DataFrame(account_info,columns=['account','net_assets','withdraw','stock_assets','info_date'])

#消除空值用上一条记录取代下一条的方式
df = dfna.fillna(method='ffill')
# dateList = sorted(df.info_date.unique())

from collections import namedtuple

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


def getAccountInfo(account):
    #计算用户为以下三种取值的个数

    data = df[df.account==account]
    #账户总资产为余额并且没有出金的个数
    account_positive = 0

    #账户总资产为余额并且部分出金的个数
    account_neutral = 0

    #账户总资产为余额全部出金的个数
    account_negative = 0
    for index,line in enumerate(myiter(data)):
        if index == 0:
            last_line = line
            continue
        if last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets == line.withdraw and line.withdraw==0.0:
            account_negative+=1
        elif last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets == line.withdraw and line.withdraw !=0.0 and line.withdraw < last_line.withdraw:
            account_neutral+=1
        elif last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets != line.withdraw:
            account_positive+=1
        last_line = line
    return [account,account_positive,account_neutral,account_negative]

account_list = df.account.unique().tolist()
start = time.time()
# thread_num = 28
# account_list_thread = {}
# for index,account in enumerate(account_list):
#     if index%28 not in account_list_thread:
#         account_list_thread[index%28] = []
#     else:
#         account_list_thread[index%28].append(account)
account_info = map(getAccountInfo,account_list)
end = time.time()
logger.info("compute for getaccountinfo {} cost time {} seconds!".format(len(account_list),end-start))

account_info = pd.DataFrame(account_info,columns=['account','account_positive','account_neutral','account_negative'])
account_info.to_csv("withdraw_train",encoding="utf-8")
