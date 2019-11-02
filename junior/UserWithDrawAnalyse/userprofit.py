#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-26 上午9:58
'''
import pandas as pd
from db.mysql.SqlUtil import Mysql
db = Mysql("mysql5.105")
sql = '''
SELECT
	id,
	funds_account,
	purpose,
	currency,
	sum(amount) sum_amount,
	count(*) AS count
FROM
	customer_payment
GROUP BY
	funds_account,
	currency,
	purpose
ORDER BY
	funds_account,
	purpose ASC
'''
account_in_out = db.execute(sql)
account_in_out_list = []
for i in account_in_out:
    account = i[1]
    purpose = i[2]
    currency = i[3]
    amount = i[4]
    times = i[5]
    account_in_out_list.append([account,purpose,currency,amount,times])

dataframe_in_out_currency = pd.DataFrame(account_in_out_list,columns=['account','purpose','currency','amount','times'])


def compute_profit(account):
    #获取第一笔入金的金额
    report_sql = '''
        SELECT
            funds_account,
            stat_time_day,
            sum(amount) AS total_amount,
            purpose,
            currency
        FROM
            (
                SELECT
                    funds_account,
                    stat_time_day,
                    amount,
                    purpose,
                    currency
                FROM
                    customer_payment
                WHERE
                    funds_account = '{}'
                AND purpose = 1
                ORDER BY
                    stat_time ASC
            ) a
        INNER JOIN (
            SELECT
                min(stat_time_day) AS stat_time_day_min
            FROM
                customer_payment
            WHERE
                funds_account = '{}'
        ) b ON a.stat_time_day = b.stat_time_day_min
        GROUP BY
            funds_account,
            stat_time_day,
            purpose,
            currency
    '''.format(account)
    report_data = db.execute(report_sql)

    #获取当前的总资金
    date = report_data[0][1]
    jcbms_sql = '''
        SELECT
            account_id,
            currency,
            market_value + trade_balance + ipo_frozen_before_close AS total
        FROM
            account_balance
        WHERE
            account_id = '{}'
        AND process_date = '{}'        
    '''.format(account,date)
    db.execute(jcbms_sql)
