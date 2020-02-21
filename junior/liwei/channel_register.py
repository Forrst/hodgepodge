#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-01-10 下午3:34
'''
import MySQLdb
import pandas as pd
#渠道注册用户
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
qd_sql = '''
SELECT
	a.channel,
	a.user_id,
	b.funds_account,
	c.cash_in_total,
	d.trade_amount_total
FROM
	(
		SELECT
			channel,
			user_id
		FROM
			user_register_change
		WHERE
			stat_time >= '2019-06-01 00:00:00'
		AND stat_time <= '2019-12-31 23:59:59'
	) a
LEFT JOIN user_account_info b ON a.user_id = b.user_id
LEFT JOIN (
	(
		SELECT
			account_id,
			sum(amount) AS cash_in_total
		FROM
			jcbms.cash_flow
		WHERE
			process_date >= '2019-06-01'
		AND process_date <= '2019-12-31'
		AND purpose = 1
		AND remark LIKE "%存款%"
		GROUP BY
			account_id
	)
) c ON b.funds_account = c.account_id
LEFT JOIN (
	SELECT
		account_id,
		sum(trade_amount) AS trade_amount_total
	FROM
		jcbms.account_trade
	WHERE
		process_date >= '2019-06-01'
	AND process_date <= '2019-12-31'
	GROUP BY
		account_id
) d ON c.account_id = d.account_id
'''
qd_df = pd.read_sql(qd_sql,con)

qd_register = qd_df[['channel','user_id']].groupby('channel',as_index=False).count()

qd_cash = qd_df[['channel','cash_in_total']].dropna()
qd_cash = qd_cash.groupby("channel",as_index=False).count()

qd_trade = qd_df[['channel','trade_amount_total']].dropna()
qd_trade = qd_trade.groupby('channel',as_index=False).count()
qd_m1 = pd.merge(qd_register,qd_cash,how='left')
qd_m2 = pd.merge(qd_m1,qd_trade)
qd_m2 = qd_m2.fillna(0)
qd_m2 = qd_m2.sort_values(by=['user_id','cash_in_total','trade_amount_total'],ascending=False)
qd_m2.reset_index(drop=True)