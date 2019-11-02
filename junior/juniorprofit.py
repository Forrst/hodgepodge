#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-02 下午6:24
'''

import pandas as pd
import MySQLdb

hk_rate = 1
cn_rate = 1.1347
usd_rate = 7.801

sql_non_trade = '''
SELECT
	account_id,
	currency,
	amount,
remark
FROM
	cash_flow
WHERE
	(remark LIKE "%IPO Apply Handling Fee(loan_charge)%"
OR remark LIKE "%IPO Loan Interest%%"
OR remark LIKE "%Monthly Interest Paid%")
AND amount <>0
GROUP BY
	account_id,
	currency
ORDER BY
	amount DESC
'''
sql_trade = '''
SELECT
	account_id,
	charge_code,
	sum(charges) AS charge
FROM
	account_charge
WHERE
	(
		formula_code = 'DEFAULT'
		OR formula_code = 'US_JCQSF'
		OR formula_code = 'JC_QSF'
		OR formula_code = 'US_CLEARING_FEE'
		OR formula_code = 'US_CLEAR_FEE_YX'
		OR formula_code = 'US_CLEARING_FEE'
	)
AND charges <> 0
GROUP BY
	account_id,
	charge_code
ORDER BY
	charge DESC
'''

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
trade_df = pd.read_sql(sql_trade,con=con)
non_trade_df = pd.read_sql(sql_non_trade,con=con)
