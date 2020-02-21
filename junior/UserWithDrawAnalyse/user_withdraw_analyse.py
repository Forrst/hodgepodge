#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-21 下午3:01
'''
import os
os.chdir("/home/eos/git/hodgepodge")
from db.mysql.SqlUtil import Mysql
import pandas as pd
import numpy as np
import MySQLdb
import datetime
db = Mysql("mysql5.105")
#存取款

sql ='''
SELECT
	funds_account,
	withdraw,
	stat_time_day,
	max_trade_day,
	withdraw_date,
	last_withdraw_money,
	withdraw_num,
	order_num
FROM
	user_golden_info
WHERE
	withdraw_date >= '2019-05-01'
OR withdraw_date = ""
ORDER BY
	withdraw_date DESC
'''
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
accounts = pd.read_sql(sql,con)

def account_handler(x):
    stat_time_day = x['stat_time_day']

    if stat_time_day != "" and stat_time_day is not None:
        stat_time_day = datetime.datetime(year=stat_time_day.year,month=stat_time_day.month,day=stat_time_day.day)
    else:
        stat_time_day = np.nan

    if x['max_trade_day'].strip() != "" and stat_time_day is not np.nan:
        max_trade_day = datetime.datetime.strptime(x['max_trade_day'],'%Y%m%d')
        try:
            x['trade_days_period'] = (max_trade_day - stat_time_day).days
        except Exception as e:
            print(type(stat_time_day))
            print(e)
    elif x['max_trade_day'].strip() == "":
        x['trade_days_period'] = np.nan

    if x['withdraw_date'].strip() != "" and stat_time_day is not np.nan:
        withdraw_date = datetime.datetime.strptime(x['withdraw_date'],'%Y-%m-%d %H:%M:%S')
        withdraw_date = datetime.datetime(year=withdraw_date.year,month=withdraw_date.month,day=withdraw_date.day)
        x['withdraw_days_period'] = (withdraw_date - stat_time_day).days
    elif x['withdraw_date'].strip() == "":
        x['withdraw_days_period'] = np.nan

    if x['withdraw_date'].strip() != "" and x['max_trade_day'].strip() != "":
        withdraw_date = datetime.datetime.strptime(x['withdraw_date'],'%Y-%m-%d %H:%M:%S')
        withdraw_date = datetime.datetime(year=withdraw_date.year,month=withdraw_date.month,day=withdraw_date.day)
        max_trade_day = datetime.datetime.strptime(x['max_trade_day'],'%Y%m%d')
        x['withdraw_trade_days'] = (withdraw_date-max_trade_day).days
    else:
        x['withdraw_trade_days'] = np.nan
    return x

accounts_ = accounts.apply(lambda x:account_handler(x),axis=1)

withdrawdate = []
for i in accounts:
    withdrawdate.append(i[1])

#认购新股
sql = '''
SELECT
	a.*
FROM
	(
		SELECT
			account_id,
			date_format(SPLIT_STR(ipo_id, '_', 3), '%Y-%m-%d') AS process_date,
			product_id,
			quantity,
			allot_qty
		FROM
			product_ipo_app
	) a
INNER JOIN (
	SELECT
		account_id
	FROM
		account_balance
	WHERE
		(
			settle_balance = 0
			AND trade_balance = 0
		)
	AND process_date = '2019-10-18'
) b ON a.account_id = b.account_id
ORDER BY
	a.account_id,
	a.process_date DESC
'''
account_ipo_app = db.execute(sql,db="jcbms")
ipo_app = []
for i in account_ipo_app:
    ipo_app.append([i[0],i[1],i[2],i[3],i[4]])
ipo_app_df = pd.DataFrame(ipo_app,columns=['account_id'])
#交易记录
sql = '''
SELECT
	a.*
FROM
	(
		SELECT
			process_date,
			account_id,
			product_id,
			product_description,
			avg_price,
			quantity,
			buy_sell
		FROM
			account_trade
	) a
INNER JOIN (
	SELECT
		account_id
	FROM
		account_balance
	WHERE
		(
			settle_balance = 0
			AND trade_balance = 0
		)
	AND process_date = '2019-10-18'
) b ON a.account_id = b.account_id
ORDER BY
	a.account_id,
	a.process_date DESC
'''
account_tradin = db.execute(sql,db="jcbms")

#
#用总资产等于总可取的天数
#用户最近6个月的交易记录
#用户打新申购中签记录

#用户总资产等于总可取

accounts = pd.read_csv("/home/eos/data/withdraw/full_withdraw_user.csv")
account = '66010958'

account_info_list = []
for account in accounts:
    account_dayList = []
    cash_flow_sql = '''
SELECT
	account_id,
	balance,
	process_date,
	amount,
	purpose,
	remark
FROM
	cash_flow
WHERE
	account_id = '{}'
AND (
	remark LIKE "%存款%"
	OR remark LIKE "%提款%"
)
ORDER BY
	process_date DESC
'''.format(account)
    cash_flow = db.execute(cash_flow_sql)
    if len(cash_flow)==0:
        continue
    trade_user_money_sql = '''
SELECT
	min(info_date) AS startdate,
	max(info_date) AS enddate,
	account,
	net_assets,
	withdraw,
	count(*) AS times
FROM
	(
		SELECT
			*
		FROM
			trade_user_money
		WHERE
			account = '{}'
		UNION
			(
				SELECT
					*
				FROM
					trade_user_money_backup_bak01
				WHERE
					account = '{}'
				AND info_date > '20190501'
			)
	) a
GROUP BY
	account,
	net_assets,
	withdraw
HAVING
	net_assets = withdraw
	and withdraw != 0
ORDER BY
	enddate DESC'''.format(account,account)
    account_trade_sql = '''
SELECT
process_date,
account_id,
product_id,
product_description,
avg_price,
quantity,
buy_sell
FROM
account_trade
where account_id = '{}' '''.format(account)
    ipo_app_sql = '''
SELECT
	account_id,
	date_format(
		SPLIT_STR (ipo_id, '_', 3),
		'%Y-%m-%d'
	) AS process_date,
	product_id,
	quantity,
	allot_qty
FROM
	product_ipo_app
WHERE
	account_id = '{}'
'''.format(account)
    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="miningtrade", charset="utf8")
    account_info = pd.read_sql(sql = trade_user_money_sql,con= con)
    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
    account_trade = pd.read_sql(sql = account_trade_sql,con= con)
    account_ipo = pd.read_sql(sql = ipo_app_sql,con= con)
    account_info_list.append([account])
