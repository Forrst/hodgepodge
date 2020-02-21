#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-01-07 上午9:50
'''
import MySQLdb
import pandas as pd


con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
account_sql = '''
SELECT
	channel_register,
	funds_account,
	stat_time_day
FROM
	user_golden_info
WHERE
	channel_register LIKE "%XCT%"
AND stat_time_day >= '2019-06-01'
AND stat_time_day <= '2019-12-31'
ORDER BY
	stat_time_day ASC
'''
account_df = pd.read_sql(account_sql,con)

con1 = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
ipo_sql = '''
SELECT
	b.account_id,
	count(*) AS ipo_total_times,
	sum(is_loan) AS ipo_loan_times,
	sum(allot_status) AS ipo_win_times,
	sum(interest) AS ipo_interest_total,
	sum(loan_charge) AS ipo_loan_charge_total,
	sum(ipo_refund_amount) AS ipo_refund_total
FROM
	(
		SELECT
			account_id,
			product_id,
			interest,
			loan_charge,

		IF (allot_qty <> 0, 1, 0) AS allot_status,

	IF (loan_amount <> 0, 1, 0) AS is_loan,
	allot_qty * allot_price * 0.01 AS ipo_refund_amount
FROM
	product_ipo_app
WHERE
	ipo_id IN (
		SELECT
			ipo_id
		FROM
			product_ipo_announcement
		WHERE
			allot_date > '2019-05-31 23:59:59'
		AND start_time <= '2019-12-31 23:59:59'
	)
	) b
GROUP BY
	b.account_id
'''
ipo_df = pd.read_sql(ipo_sql,con1)


trade_sql = '''
SELECT
	a.account_id,
	a.buy_sell_type,
	a.market_type,
	sum(b.nums_of_trades) AS trade_times,
	sum(a.real_commission) AS trade_commission
FROM
	(
		SELECT
			account_id,
			trade_id,

		IF (input_channel = 5, buy_sell, 0) AS buy_sell_type,
		(
			CASE
			WHEN market_id = 1 THEN
				'HK'
			WHEN (
				(market_id = 2) || (market_id = 16)
			) THEN
				'US'
			WHEN (
				(market_id = 4) || (market_id = 8)
			) THEN
				'CN'
			END
		) AS market_type,

	IF (input_channel <> 5, 0, 1) * commission AS real_commission
	FROM
		account_trade
	WHERE
		process_date >= '2019-06-01'
	AND process_date <= '2019-12-31'
	AND (market_id<>4 and market_id<>8)
	) a
INNER JOIN (
	SELECT
		trade_id,
		sum(num_of_trades) AS nums_of_trades
	FROM
		account_trade_detail
	WHERE
		process_date >= '2019-06-01'
	AND process_date <= '2019-12-31'
	GROUP BY
		trade_id
) b ON a.trade_id = b.trade_id
GROUP BY
	a.account_id,
	a.buy_sell_type,
	market_type
HAVING
	account_id IS NOT NULL
ORDER BY
	account_id,
	buy_sell_type
'''
trade_df = pd.read_sql(trade_sql,con1)

account_df = account_df.rename(columns={'funds_account':'account_id'})
market_times = trade_df.groupby(['account_id','market_type'],as_index=False).agg({'trade_times':sum})
market_times_hk = market_times[market_times.market_type=='HK']
market_times_us = market_times[market_times.market_type=='US']
market_times_hk = market_times_hk.rename(columns={'trade_times':'trade_hk_times'})
market_times_us = market_times_us.rename(columns={'trade_times':'trade_us_times'})
market_times_hk = market_times_hk[['account_id','trade_hk_times']]
market_times_us = market_times_us[['account_id','trade_us_times']]

trade_df_t = trade_df.groupby(['account_id', 'buy_sell_type'],as_index=False).agg({'trade_times':sum,'trade_commission':sum})
trade_df_buy = trade_df_t[trade_df_t['buy_sell_type']==1]
trade_df_sell =trade_df_t[trade_df_t['buy_sell_type']==2]
trade_df_normal =trade_df_t[trade_df_t['buy_sell_type']==0]

trade_df_buy = trade_df_buy.rename(columns={'trade_times':'dark_buy_times','trade_commission':'dark_buy_commission'})
trade_df_sell = trade_df_sell.rename(columns={'trade_times':'dark_sell_times','trade_commission':'dark_sell_commission'})
trade_df_normal = trade_df_normal.rename(columns={'trade_times':'normal_trade_times','trade_commission':'normal_commission'})

trade_df_buy1 = trade_df_buy[['account_id','dark_buy_times','dark_buy_commission']]
trade_df_sell1 = trade_df_sell[['account_id','dark_sell_times','dark_sell_commission']]
trade_df_normal1 = trade_df_normal[['account_id','normal_trade_times']]
d1 = pd.merge(account_df,ipo_df,how='left')
d2 = pd.merge(d1,trade_df_buy1,how='left')
d3 = pd.merge(d2,trade_df_sell1,how='left')
d4 = pd.merge(d3,trade_df_normal1,how='left')
d5 = pd.merge(d4,market_times_hk,how='left')
d6 = pd.merge(d5,market_times_us,how='left')


consumer_df_final = d6.fillna(0)
consumer_df_final.reset_index(drop=True,inplace=True)