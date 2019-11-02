#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-09 下午5:13
'''
import pandas as pd
import os
import logging
os.chdir("/home/eos/git/hodgepodge/")
from db.mysql.SqlUtil import Mysql

db = Mysql("mysql5.105")
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("account_analyse.py")


'''
1812/3423 1
791/3423 2
3344 唯一账户
'''

'''
1，首先筛选出出金之后有入金行为的用户这部分用户为忠实用户 可根据周期来判断
2，出金之后无入金行为的用户 全部出金的为流失用户 部分出金的为有可能出金的用户 出金的概率小于3
3，入金之后没有出过金的用户 为危险用户最有可能出金的用户 出金的概率最大
'''

#1
#用户最后一次入金的时间
sql = '''
SELECT
	funds_account,
	max(apply_date) as max_apply_date
FROM
	deposit_detail
where audit_status = '300010'
GROUP BY
	funds_account
ORDER BY
	max_apply_date DESC
'''

account_last_in = db.execute(sql,"miningaccount")
account_last_in_dict = {}
for i in account_last_in:
    account_id = i[0]
    last_in_time = i[1]
    account_last_in_dict[account_id] = last_in_time

#用户最早一次出金的时间
sql = '''
SELECT
	funds_account,
	min(insert_date) AS early_out_time
FROM
	miningaccount.transfer_money_apply
WHERE
	`status` = "3001"
AND (
	deposit_step < 1
	OR deposit_step IS NULL
)
AND transfer_method_id != 6
GROUP BY
	funds_account
ORDER BY
	early_out_time ASC
'''
account_early_out = db.execute(sql,"miningaccount")
account_early_out_dict = {}
for i in account_early_out:
    account_id = i[0]
    early_out_time = i[1]
    account_early_out_dict[account_id] = early_out_time

#用户最早一次入金的时间
sql = '''
SELECT
	funds_account,
	min(apply_date) as min_apply_date
FROM
	deposit_detail
where audit_status = '300010'
GROUP BY
	funds_account
ORDER BY
	min_apply_date DESC
'''


account_early_in = db.execute(sql,"miningaccount")
account_early_in_dict = {}
for i in account_early_in:
    account_id = i[0]
    early_in_time = i[1]
    account_early_in_dict[account_id] = early_in_time

#获取所有入金的用户账号
sql = '''
SELECT
	funds_account
FROM
	deposit_detail
WHERE
	audit_status = '300010'
AND funds_account IS NOT NULL
GROUP BY
	funds_account
'''
all_accounts = db.execute(sql,"miningaccount")

ret = []
for i in all_accounts:
    account_id = i[0]
    first_in = (account_id in account_early_in_dict and account_early_in_dict[account_id] or "")
    early_out = (account_id in account_early_out_dict and account_early_out_dict[account_id] or "")
    last_in = (account_id in account_last_in_dict and account_last_in_dict[account_id] or "")
    ret.append([account_id,first_in,last_in,early_out])
account_info = pd.DataFrame(ret,columns = ['account_id','first_in','last_in','early_out'])
def getClassfication(x):
    '''
    level = A 多次入金中间有出金行为
    level = B 多次入金之后无出金行为
    level = C 多次入金之后开始出金
    level = D 单次入金无出金行为
    level = E 单次入金有出金行为
    '''
    level = 0
    first_in = x['first_in']
    last_in = x['last_in']
    early_out = x['early_out']
    if first_in == last_in and isinstance(x['early_out'],pd.tslib.NaTType):
        level = "D"
    elif first_in == last_in and not isinstance(x['early_out'],pd.tslib.NaTType):
        level = "E"
    elif first_in<last_in and isinstance(x['early_out'],pd.tslib.NaTType):
        level = "B"
    elif first_in<last_in and last_in>early_out:
        level = "A"
    elif first_in<last_in and last_in<early_out:
        level = "C"
    return level
account_info['level'] = account_info.apply(lambda x:getClassfication(x),axis=1)

'''
1，计算用户盈亏
2，查看可取资金
3，股票市值
4，总市值
5，*借贷金额
'''

'''
1 表示香港
2 表示美国
3 表示上海
4 表示深圳
'''

exchange_rate = {
    '1':1,
    '2':7.84,
    '3':1.1,
    '4':1.1
}

def get_profit(account_id):
    sql = '''
    SELECT
        trading_day,
        account_id,
        direction,
        exchange_id,
        instrument_id,
        instrument_name,
        volume,
        price
    FROM
        oms_history.trade
    WHERE
        account_id = '{}'
    UNION
        (
            SELECT
                trading_day,
                client_acc_id AS account_id,
                bs_flag AS direction,
                exchange AS exchange_id,
                CODE AS instrument_id,
                instrument_name,
                qty AS volume,
                price
            FROM
                oms_ju.trade_excution_01
            WHERE
                client_acc_id = '{}'
        )
    ORDER BY
        trading_day DESC
    '''.format(account_id,account_id)
    trade_info = db.execute(sql,"oms_ju")
    trade_list = []
    instrument_id_dict = {}
    for i in trade_info:
        account_id = i[1]
        instrument_id = i[4]
        amount = i[6]*i[7]
        currency = i[3]
        if i[2] == "1":
            if instrument_id not in instrument_id_dict:
                instrument_id_dict[instrument_id] = "b"
            elif instrument_id_dict[instrument_id] == "s":
                instrument_id_dict[instrument_id] = "True"
            amount = -float(amount)*exchange_rate[currency]
        else:
            if instrument_id not in instrument_id_dict:
                instrument_id_dict[instrument_id] = "s"
            elif instrument_id_dict[instrument_id] == "b":
                instrument_id_dict[instrument_id] = "True"
            amount = float(amount)*exchange_rate[currency]
        trade_list.append([account_id,instrument_id,i[2],amount])
    stock_pair_set = set([key for key in instrument_id_dict.keys() if instrument_id_dict[key]=='True'])
    df = pd.DataFrame(trade_list,columns=['account_id','stock_code','bs_flag','amount'])
    dfg = df[df['stock_code'].isin(stock_pair_set)].groupby(['account_id','bs_flag','stock_code'])
    summary = dfg.sum()
    if len(summary) == 0:
        return 0
    else:
        dfgs = dfg.sum().sum()
        return float(dfgs)
# profit_dict = {}
# for i in all_accounts:
#     account_id = i[0]
#     profit = get_profit(account_id)
#     profit_dict[account_id] = profit

account_info['profit'] = account_info.apply(lambda x:get_profit(x['account_id']),axis=1)


#6月份之后出金的用户
sql = '''
SELECT
	funds_account,
	amount,
	insert_date
FROM
	transfer_money_apply
WHERE
	`status` = '3001'
and insert_date <'2019-06-01 00:00:00'
AND funds_account NOT IN (
	SELECT
		funds_account
	FROM
		transfer_money_apply
	WHERE
		COMMENT LIKE '%测试账户%'
	OR COMMENT LIKE '%test%'
GROUP BY funds_account
) 
'''

#资产变为0的用户
sql = '''
SELECT
	account
FROM
	trade_user_money
WHERE
	info_date >= '20190601'
AND net_assets > -10
AND net_assets<10
AND stock_assets = 0
GROUP BY
	account,
	net_assets
ORDER BY
	net_assets ASC
'''
account1 = db.execute(sql,"miningtrade")

sql = '''
SELECT
	account,
	net_assets,
	insert_time
FROM
	trade_user_money
WHERE
	insert_time < '2019-06-01 00:00:00'
AND insert_time>'2019-05-01 00:00:00'
AND stock_assets > 0
AND net_assets>100
GROUP BY
	account,
	net_assets
ORDER BY
	net_assets ASC
'''
account2 = db.execute(sql,"miningtrade")
#6月份以后完全出金的
account_6 = set([i[0] for i in account1])
#5月份有资产的
account_5 = set([i[0] for i in account2])
#6月份以后完全出金的
accounts = account_5 & account_6

#获取用户连续净资产等于可取的天数
account = '91499798'
sql = '''
SELECT
	info_date,
	account,
	net_assets,
	withdraw
FROM
	trade_user_money
WHERE
	account = '{}'
AND info_date >= '20190601'
'''.format(account)

sql = '''
SELECT
	info_date,
	account,
	net_assets,
	withdraw,
	COUNT(*) AS times
FROM
	trade_user_money
WHERE
	account = '88636695'
AND net_assets = withdraw
AND withdraw != 0
AND info_date >= '20190601'
GROUP BY
	account,
	net_assets,
	withdraw
ORDER BY
	info_date ASC
'''

'''
特征1 全部出金的次数
特征2 可取和净资产一样并且不为0的天数
'''



#资产一直为0的用户不计算在内
import time
start = time.time()
all_accounts_sql = '''
SELECT
	a.account,
	net_assets,
	withdraw,
	stock_assets,
	info_date
FROM
	(
		SELECT
			account,
			net_assets,
			withdraw,
			stock_assets,
			info_date
		FROM
			trade_user_money
		WHERE
			info_date >= '20190501'
		AND info_date < '20190701'
	) a
LEFT JOIN (
	SELECT
		account
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
			AND info_date >= '20190501'
			AND info_date < '20190701'
			GROUP BY
				account
			HAVING
				times = 42
		) b
) c ON a.account = c.account
WHERE
	c.account IS NULL
ORDER BY
	account,
	info_date ASC
'''
accounts_all = db.execute(all_accounts_sql,"miningtrade")
end = time.time()
logger.info("total records {} cost time {} seconds!".format(len(accounts_all),end-start))

account_info = []
for i in accounts_all:
    account = i[0]
    net_assets = i[1]
    withdraw = i[2]
    stock_assets = i[3]
    info_date = i[4]
    account_info.append([account,net_assets,withdraw,stock_assets,info_date])
dfna = pd.DataFrame(account_info,columns=['account','net_assets','withdraw','stock_assets','info_date'])
df = dfna.fillna(method='ffill')
dateList = sorted(df.info_date.unique())

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
account_info = map(getAccountInfo,account_list)
end = time.time()
logger.info("compute for getaccountinfo {} cost time {} seconds!".format(len(accounts_all),end-start))

sql = '''

'''