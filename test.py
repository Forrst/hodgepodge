#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-12-12 下午3:06
'''
import datetime

import MySQLdb

asset_value_net_begin = 0
asset_value_net_end = 0
total_market_value = 0
available = 0
ipo_frozen = 0
accessable = 0
hk_asset_total = 0
hk_market_value = 0
hk_rate = 0

us_asset_total = 0
us_market_value = 0
us_rate = 0

cn_asset_total = 0
cn_market_value = 0
cn_rate = 0
def data_compare(process_date):
    process_day = "{}-{}-{}".format(process_date.year,process_date.month,process_date.day)
    con = MySQLdb.connect(host='192.168.5.105',user='root',passwd='zunjiazichan123',db='report',charset='utf8')
    cursor = con.cursor()
    sql = "select sum(market_value+trade_balance+ipo_frozen_before_close) as asset_market_value,sum(market_value) as total_market_value,sum(trade_balance) as available,sum(ipo_frozen_before_allot) as ipo_frozen,sum(max(settle_balance + min(accrued_interest_credit- a.accrued_interest,0) -margin_amt + min(0,market_value) - frozen_amount) as accessable from account_balance  left join currency_history where process_date = {}".format(process_day)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.commit()
    con.close()
    return result



now  = datetime.datetime.now()
for i in xrange(10):
    process_date = now-datetime.timedelta(days=i+1)




