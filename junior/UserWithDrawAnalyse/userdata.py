#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-29 上午9:43
'''

import MySQLdb
import pandas as pd
import os
os.chdir("/home/eos/git/hodgepodge")
from db.mysql.SqlUtil import Mysql
import numpy as np
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("userdata.py")

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
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
(
	remark LIKE "%存款%"
	OR remark LIKE "%提款%"
)
AND process_date >='2019-02-01'
ORDER BY
	process_date DESC
'''
accounts = pd.read_sql(cash_flow_sql,con)



# def myiter(d, cols=None):
#     if cols is None:
#         v = d.values.tolist()
#         cols = d.columns.values.tolist()
#     else:
#         j = [d.columns.get_loc(c) for c in cols]
#         v = d.values[:, j].tolist()
#
#     n = namedtuple('MyTuple', cols)
#
#     for line in iter(v):
#         yield n(*line)

# db = Mysql("mysql5.105")
# account_full_withdraw = []
# account_date = set()
# #计算出金用户 出金之后的总资产
# for i in tqdm(myiter(withdraw_accounts)):
#     try:
#         net_assets = 0
#         date = i.process_date
#         account = i.account_id
#         date_account = str(date)+"_"+str(account)
#         if date_account in account_date:
#             continue
#         else:
#             account_date.add(date_account)
#         process_date = str(date)
#         if process_date<'2019-07-01':
#             sqlret = db.execute("select net_assets from trade_user_money_backup_bak01 where info_date = '{}' and account = '{}'".format(process_date.replace("-",""),account),db='miningtrade')
#         elif process_date>='2019-07-01' and process_date<'2019-07-15':
#             sqlret = db.execute("select net_assets from trade_user_money where info_date = '{}' and account = '{}'".format(process_date.replace("-",""),account),db='miningtrade')
#         else:
#             net_assets_sql = '''
#                     SELECT
#                     sum(
#                     a.net_assets * b.exchange_rate
#                     ) AS total
#                     FROM
#                     (
#                     SELECT
#                         market_value + trade_balance + ipo_frozen_before_close AS net_assets,
#                         currency,
#                         process_date
#                     FROM
#                         account_balance
#                     WHERE
#                         account_id = '{}'
#                     AND process_date = '{}'
#                     ) a
#                     LEFT JOIN (
#                     SELECT
#                     process_date,
#                     currency,
#                     exchange_rate
#                     FROM
#                     currency_history
#                     WHERE
#                     process_date = '{}'
#                     ) b ON a.process_date = b.process_date
#                     AND a.currency = b.currency
#                 '''.format(account,process_date,process_date)
#             sqlret = db.execute(net_assets_sql,db='jcbms')
#         net_assets = sqlret[0][0]
#         account_full_withdraw.append([account,process_date,net_assets])
#     except Exception,e:
#         print i
#         print e
#         continue
#
# account_full_withdraw_ = map(lambda x:[x[0],x[1],float(x[2])],account_full_withdraw)
# account_balance_df = pd.DataFrame(account_full_withdraw_,columns=['account','process_date','amount'])
# account_real_fwd = account_balance_df[account_balance_df['amount']<50]
# start = datetime.datetime(year=2019,month=6,day=1)
# for i in range(10):
#     end = start+datetime.timedelta(days=15)
#     print start.date(),end.date()
#     # print len(account_real_fwd[account_real_fwd['process_date']>=str(start.date())][account_real_fwd['process_date']<str(end.date())])
#     start = end



# ipo_apply = '''
# SELECT
# 	account_id,
# 	date_format(
# 		SPLIT_STR (ipo_id, '_', 3),
# 		'%Y-%m-%d'
# 	) AS process_date,
# 	product_id,
# 	quantity,
# 	allot_qty
# FROM
# 	product_ipo_app
# WHERE
# 	account_id = '{}'
# ORDER BY
# 	process_date DESC'''.format(account)
# ipo_data = db.execute(ipo_apply,db='jcbms')

#总资产大于10万的用户
total_net_assets_sql = '''
        SELECT
        a.account,
        a.max_net_assets,
        b.phone1,
        short_name,
        birth_day,
        email_address,
        mailing_address
        FROM
        (
            SELECT
            account,
            max(net_assets) AS max_net_assets
        FROM
        trade_user_money
        WHERE
        net_assets > 100000
        GROUP BY
        account
        ) a
        LEFT JOIN jcbms.account_profile b ON a.account = b.account_id
        ORDER BY
        max_net_assets DESC
'''

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="miningtrade", charset="utf8")
major = pd.read_sql(total_net_assets_sql,con)

db = Mysql("mysql5.105")
def getTotalNetAssets(account,process_date):
    '''
    计算用户总资产
    :param account:
    :param process_date:
    :return:
    '''
    net_assets_sql = '''
                    SELECT
                    sum(
                    a.net_assets * b.exchange_rate
                    ) AS total
                    FROM
                    (
                    SELECT
                        market_value + trade_balance + ipo_frozen_before_close AS net_assets,
                        currency,
                        process_date
                    FROM
                        account_balance
                    WHERE
                        account_id = '{}'
                    AND process_date = '{}'
                    ) a
                    LEFT JOIN (
                    SELECT
                    process_date,
                    currency,
                    exchange_rate
                    FROM
                    currency_history
                    WHERE
                    process_date = '{}'
                    ) b ON a.process_date = b.process_date
                    AND a.currency = b.currency
                '''.format(account,process_date,process_date)
    sqlret = db.execute(net_assets_sql,'jcbms')
    return sqlret[0][0]



major['net_assets'] = major['account'].apply(lambda x:getTotalNetAssets(x,"2019-10-31"))
c = set(accounts['account_id']) & set(major['account'])
major['net_assets'] = major['net_assets'].astype(np.float64)
#计算现有资产与最大资产的比值
major['asset_rate'] = major['net_assets']/major['max_net_assets']


major_cash_flow = pd.merge(major, accounts, left_on = "account", right_on= 'account_id' , how='left')
withdraw_deposit_info = accounts.groupby(["account_id","purpose"],as_index=False,sort=True).count()

def getWithdraw(account):
    #获取出金次数
    withdraw = withdraw_deposit_info[withdraw_deposit_info['account_id']==account][withdraw_deposit_info['purpose']==2]['balance']
    if len(withdraw)==0:
        return 0
    else:
        return withdraw.tolist()[0]

def getDeposit(account):
    #获取出金次数
    deposit = withdraw_deposit_info[withdraw_deposit_info['account_id']==account][withdraw_deposit_info['purpose']==1]['balance']
    if len(deposit)==0:
        return 0
    else:
        return deposit.tolist()[0]

#出入金次数
major['withdraw'] = major['account'].apply(lambda x:getWithdraw(x))
major['deposit'] = major['account'].apply(lambda x:getDeposit(x))
major['cash_flow_total'] = major['withdraw']+major['deposit']

#申购新股
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
def getIpo(account):
    ipo_sql ='''
        SELECT
            *
        FROM
            (
                SELECT
                    CLIENT_ACC_ID AS account_id,
                    jcbms.SPLIT_STR (ID, ':', 2) AS stock_id,
        
                IF (ACTUAL_QTY = '0.000000', 0, 1) AS status
                FROM
                    report.product_ipo_app
                WHERE
                    CLIENT_ACC_ID = '{}'
                AND ACTUAL_QTY IS NOT NULL
                AND ACTUAL_QTY <> 'NULL'
            ) a
        UNION
            SELECT
                account_id,
                product_id AS stock_id,
        
            IF (allot_qty < 1, 0, 1) AS status
            FROM
                jcbms.product_ipo_app
            WHERE
                account_id = '{}'
            ORDER BY
                stock_id DESC
    '''.format(account,account)
    ipo_df = pd.read_sql(ipo_sql,con)
    ipo_summary = ipo_df.groupby(['account_id','status'],as_index=False,sort=True).count()
    total = 0
    if len(ipo_summary)>0:
        total = ipo_summary.stock_id.sum()
    win = 0
    if len(ipo_summary[ipo_summary['status'] == 1])>0:
        win = ipo_summary[ipo_summary['status'] == 1].stock_id.values[0]
    return [total,win]
major['ipo_times'] = major['account'].apply(lambda x:getIpo(x))

#获取所有的新股集合
all_ipo_stock_sql = '''
SELECT
jcbms.SPLIT_STR (ID, ':', 2) AS stock_id
FROM
report.product_ipo_app
GROUP BY
stock_id
UNION
SELECT
product_id AS stock_id
FROM
jcbms.product_ipo_app
GROUP BY
stock_id'''
ipo_stock_df = pd.read_sql(all_ipo_stock_sql,con)
ipo_stock_set = set(ipo_stock_df.stock_id)

#交易的股票详情
def get_trade(account):
    logger.info("handling {}".format(account))
    sql = '''
    
        SELECT
            client_acc_id AS account_id,
            exchange,
            CODE,
            bs_flag,
            SUM(price * qty) AS amount
        FROM
            (
                SELECT
                    *
                FROM
                    oms_ju.trade_excution_01
                WHERE
                    client_acc_id = '{}'
            ) a
        GROUP BY
            client_acc_id,
            bs_flag,
            instrument_name
    UNION ALL
        (
            SELECT
                account_id,
                exchange_id,
                instrument_id AS CODE,
                direction AS bs_flag,
                SUM(price * volume) AS amount
            FROM
                oms_history.trade
            WHERE
                account_id = '{}'
            GROUP BY
                account_id,
                direction,
                instrument_id
            ORDER BY
                CODE,
                amount,
                bs_flag DESC
        )
    '''.format(account,account)
    trade_df = pd.read_sql(sql,con)
    trade_amount_summary = trade_df.groupby(['exchange','bs_flag'],as_index=False).sum()
    if len(trade_amount_summary) == 0:
        return [0,0,0,0,0]
    hk_buy = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='1'][trade_amount_summary['bs_flag']=='1']['amount'])
    hk_sell = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='1'][trade_amount_summary['bs_flag']=='2']['amount'])
    us_buy = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='2'][trade_amount_summary['bs_flag']=='1']['amount'])
    us_sell = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='2'][trade_amount_summary['bs_flag']=='2']['amount'])
    sh_buy = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='3'][trade_amount_summary['bs_flag']=='1']['amount'])
    sh_sell = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='3'][trade_amount_summary['bs_flag']=='2']['amount'])
    sz_buy = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='4'][trade_amount_summary['bs_flag']=='1']['amount'])
    sz_sell = getTrueValue(trade_amount_summary[trade_amount_summary['exchange']=='4'][trade_amount_summary['bs_flag']=='2']['amount'])
    hk_total = hk_buy+hk_sell
    us_total = us_buy+us_sell
    cn_total = sh_buy+sh_sell+sz_buy+sz_sell
    total_stock_number = len(set(trade_df['code']))
    ipo_stock_number = len(ipo_stock_set & set(trade_df['code']))
    return [hk_total,us_total,cn_total,total_stock_number,ipo_stock_number]

def getTrueValue(x):
    if len(x)==0:
        value = 0
    else:
        value = x.values[0]
    return value

major['trade_info'] = major['account'].apply(lambda x:get_trade(x))

major['tran_hk_total'] = map(lambda x:x[0],major['trade_info'])
major['tran_us_total'] = map(lambda x:x[1]*7.8375,major['trade_info'])
major['tran_cn_total'] = map(lambda x:x[2]*1.1129,major['trade_info'])
major['tran_stocks_all'] = map(lambda x:x[3],major['trade_info'])
major['tran_stocks_ipo'] = map(lambda x:x[4],major['trade_info'])
major['ipo_apply_times'] = map(lambda x:x[0],major['ipo_times'])
major['ipo_win_times'] = map(lambda x:x[1],major['ipo_times'])
major['tran_total'] = major['tran_hk_total']+major['tran_us_total']+major['tran_hk_total']

major_user = major[[u'account', u'max_net_assets', u'phone1', u'short_name',
                   u'birth_day', u'net_assets', u'asset_rate',
                   u'withdraw', u'deposit', u'cash_flow_total'
                   , u'tran_hk_total', u'tran_us_total', u'tran_cn_total',u'tran_total',
                   u'tran_stocks_all', u'tran_stocks_ipo', u'ipo_apply_times',
                   u'ipo_win_times']]
major_user.to_excel("/home/eos/data/withdraw/major_user.xls",encoding="utf-8")