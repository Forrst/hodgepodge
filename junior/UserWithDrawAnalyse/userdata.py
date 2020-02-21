#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-29 上午9:43
'''
import MySQLdb
import os

import datetime
from pyecharts import options as opts
from pyecharts.charts import Bar
from tqdm import tqdm

os.chdir("/home/eos/git/hodgepodge")
from db.mysql.SqlUtil import Mysql
import numpy as np
import logging
import pandas as pd
import collections


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
AND process_date >='2019-06-02'
AND process_date <="2019-12-24"
ORDER BY
	process_date DESC
'''.format()
accounts = pd.read_sql(cash_flow_sql,con)
acc_unique = accounts.account_id.unique()
cash_out_df = accounts[accounts.purpose==2][['account_id','process_date','amount']]


#查找指定用户的出入金行为
# cash_flow_sql = '''
# SELECT
# 	account_id,
# 	balance,
# 	process_date,
# 	amount,
# 	purpose,
# 	remark
# FROM
# 	cash_flow
# WHERE
# (
# 	remark LIKE "%存款%"
# 	OR remark LIKE "%提款%"
# )
# AND account_id in ({})
# ORDER BY
# 	account_id,process_date asc
# '''.format(str(list(acc_unique)).replace("[","").replace("]",""))
# cash_flow = pd.read_sql(cash_flow_sql,con)





#计算用户的平均交易时间,以及最后一次交易时间间隔
# def compute_trade_freq(accounts):
#      sql = "
#     select account_id,trading_day,count(*) as times from oms_history.orders where account_id in ({}) group by account_id,trading_day order by account_id,trading_day asc".format(str(list(accounts)).replace("[","").replace("]",""))
#     return pd.read_sql(sql,con=con)


def myiter(d, cols=None):
    if cols is None:
        v = d.values.tolist()
        cols = d.columns.values.tolist()
    else:
        j = [d.columns.get_loc(c) for c in cols]
        v = d.values[:, j].tolist()

    n = collections.namedtuple('MyTuple', cols)

    for line in iter(v):
        yield n(*line)

# trade_freq = compute_trade_freq(acc_unique)
# account_unique = []
# account_tperiod = {}
# account_times = {}
# for i in myiter(trade_freq):
#     if i.account_id not in account_unique:
#         account_unique.append(i.account_id)
#         account_times[i.account_id] = [i.times]
#         account_tperiod[i.account_id] = [datetime.datetime.strptime(str(i.trading_day),'%Y%m%d')]
#     else:
#         account_times[i.account_id].append(i.times)
#         account_tperiod[i.account_id].append(datetime.datetime.strptime(str(i.trading_day),'%Y%m%d'))
#
# trade_info = []
# for account in account_unique:
#     account_day = account_tperiod[account]
#     trade_days = [(account_day[i+1]-account_day[i]).days for i in range(len(account_day)-1)]
#     trade_times = account_times[account]
#     last_trade_day = account_day[-1]
#     trade_info.append([account,trade_days,trade_times,last_trade_day])


db = Mysql("mysql5.105")
account_full_withdraw = []
# account_date = set()
#计算出金用户 出金之后的总资产
for i in tqdm(myiter(cash_out_df)):
    try:
        net_assets = 0
        date = i.process_date
        account = i.account_id
        # date_account = str(date)+"_"+str(account)
        # if date_account in account_date:
        #     continue
        # else:
        #     account_date.add(date_account)
        process_date = str(date)
        if process_date<'2019-07-01':
            sqlret = db.execute("select net_assets from trade_user_money_backup_bak01 where info_date = '{}' and account = '{}'".format(process_date.replace("-",""),account),db='miningtrade')
        elif process_date>='2019-07-01' and process_date<'2019-07-15':
            sqlret = db.execute("select net_assets from trade_user_money where info_date = '{}' and account = '{}'".format(process_date.replace("-",""),account),db='miningtrade')
        else:
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
            sqlret = db.execute(net_assets_sql,db='jcbms')
        net_assets = sqlret[0][0]
        account_full_withdraw.append([account,process_date,net_assets])
    except Exception as e:
        print(i)
        print(e)
        continue

account_full_withdraw_ = map(lambda x:[x[0],x[1],float(x[2])],account_full_withdraw)
account_balance_df = pd.DataFrame(account_full_withdraw_,columns=['account','process_date','amount'])
account_real_fwd = account_balance_df[account_balance_df['amount']<50]
start = datetime.datetime(year=2019,month=6,day=1)
for i in range(10):
    end = start+datetime.timedelta(days=15)
    print start.date(),end.date()
    # print len(account_real_fwd[account_real_fwd['process_date']>=str(start.date())][account_real_fwd['process_date']<str(end.date())])
    start = end



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
# total_net_assets_sql = '''
#         SELECT
#         a.account,
#         a.max_net_assets,
#         b.phone1,
#         short_name,
#         birth_day,
#         email_address,
#         mailing_address
#         FROM
#         (
#             SELECT
#             account,
#             max(net_assets) AS max_net_assets
#         FROM
#         trade_user_money
#         WHERE
#         net_assets > 100000
#         GROUP BY
#         account
#         ) a
#         LEFT JOIN jcbms.account_profile b ON a.account = b.account_id
#         ORDER BY
#         max_net_assets DESC
# '''

def getMaxNetAssets(account):
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
            account in ({})        
        GROUP BY
        account
        ) a
        LEFT JOIN jcbms.account_profile b ON a.account = b.account_id
        ORDER BY
        max_net_assets DESC
'''.format(str(list(account)).replace("[","").replace("]",""))
    sqlret = pd.read_sql(total_net_assets_sql,con=con)
    return sqlret


def getTotalNetAssets(account,process_date):
    '''
    计算用户总资产
    :param account:
    :param process_date:
    :return:
    '''
    # net_assets_sql = '''
    #                 SELECT
    #                 sum(
    #                 a.net_assets * b.exchange_rate
    #                 ) AS total
    #                 FROM
    #                 (
    #                 SELECT
    #                     market_value + trade_balance + ipo_frozen_before_close AS net_assets,
    #                     currency,
    #                     process_date
    #                 FROM
    #                     account_balance
    #                 WHERE
    #                     account_id = '{}'
    #                 AND process_date = '{}'
    #                 ) a
    #                 LEFT JOIN (
    #                 SELECT
    #                 process_date,
    #                 currency,
    #                 exchange_rate
    #                 FROM
    #                 currency_history
    #                 WHERE
    #                 process_date = '{}'
    #                 ) b ON a.process_date = b.process_date
    #                 AND a.currency = b.currency
    #             '''.format(account,process_date,process_date)
    net_assets_sql = f'''
                    SELECT
                    sum(
                        (
                            market_value + trade_balance + ipo_frozen_before_close
                        ) * (
                            CASE
                            WHEN currency = 1 THEN
                                1.1656370206
                            WHEN currency = 2 THEN
                                1
                            WHEN 4 THEN
                                7.8415000000
                            END
                        )
                    ) AS total_asset
                FROM
                    account_balance
                WHERE
                    account_id = '{account}'
                AND process_date = '{process_date}'
                GROUP BY
                    account_id
                    '''
    sqlret = db.execute(net_assets_sql,'jcbms')
    return sqlret[0][0]

major = pd.DataFrame(accounts['account_id'].unique())
major.columns = ['account']
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="miningtrade", charset="utf8")
major['net_assets'] = major['account'].apply(lambda x:getTotalNetAssets(x,"2019-12-16"))
major['net_assets'] = major['net_assets'].astype(np.float64)

db = Mysql("mysql5.105")

# c = set(accounts['account_id']) & set(major['account'])
#计算现有资产与最大资产的比值
major['asset_rate'] = major['net_assets']/major['max_net_assets']

account_info = getMaxNetAssets(major['account'])

major_cash_flow = pd.merge(accounts, account_info, left_on = "account_id", right_on= 'account' , how='left')
major_cash_flow = pd.merge(major_cash_flow,major,left_on="account_id",right_on="account",how='left')

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

major = major_cash_flow
major.rename(columns={'account_id':'account',}, inplace = True)
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

major['tran_hk_total'] = list(map(lambda x:x[0],major['trade_info']))
major['tran_us_total'] = list(map(lambda x:x[1]*7.8375,major['trade_info']))
major['tran_cn_total'] = list(map(lambda x:x[2]*1.1129,major['trade_info']))
major['tran_stocks_all'] = list(map(lambda x:x[3],major['trade_info']))
major['tran_stocks_ipo'] = list(map(lambda x:x[4],major['trade_info']))
major['ipo_apply_times'] = list(map(lambda x:x[0],major['ipo_times']))
major['ipo_win_times'] = list(map(lambda x:x[1],major['ipo_times']))
major['tran_total'] = major['tran_hk_total']+major['tran_us_total']+major['tran_hk_total']

major_user = major[[u'account', u'max_net_assets', u'phone1', u'short_name',
                   u'birth_day', u'net_assets', u'asset_rate',
                   u'withdraw', u'deposit', u'cash_flow_total'
                   , u'tran_hk_total', u'tran_us_total', u'tran_cn_total',u'tran_total',
                   u'tran_stocks_all', u'tran_stocks_ipo', u'ipo_apply_times',
                   u'ipo_win_times']]
major_user.to_excel("/home/eos/data/withdraw/major_user.xls",encoding="utf-8")\

user = major_user[['asset_rate','max_net_assets','deposit','cash_flow_total','tran_hk_total','tran_us_total','tran_total','tran_stocks_all','ipo_apply_times','ipo_win_times']]

user['tran_hk_total'] = user['tran_hk_total']/user['max_net_assets']
user['tran_us_total'] = user['tran_us_total']/user['max_net_assets']
user['tran_total'] = user['tran_total']/user['max_net_assets']
user['asset_rate'].fillna(0,inplace=True)
user = user[['asset_rate','deposit','cash_flow_total','tran_hk_total','tran_us_total','tran_total','tran_stocks_all','ipo_apply_times','ipo_win_times']]


def bar_datazoom_both() -> Bar:
    c = (
        Bar()
            .add_xaxis(a)
            .add_yaxis("用户数", b)
            .set_global_opts(
            title_opts=opts.TitleOpts(title="Bar-DataZoom（slider+inside）"),
            datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        )
    )
    return c

def isRealOut(account):
    '''
    判断出金之后是否有入金
    :param account:
    :return:
    '''
    logger.info(account)
    cash_out_day = rfm[(rfm['account']==account) &(rfm['type']=='cash_out')].process_date
    if len(cash_out_day)==0:
        return False
    cash_out_first_day = cash_out_day.values[0]
    cash_in_day = rfm[(rfm['account']==account) &(rfm['process_date']>=cash_out_first_day)&(rfm['type']=='cash_in')].process_date
    cash_last_in_day = None
    if cash_in_day is not None and len(cash_in_day)>0:
        cash_last_in_day = cash_in_day.values[-1]
    if cash_last_in_day is not None and cash_last_in_day>cash_out_first_day:
        return False
    else:
        return True


m = account_full_withdraw_df
def get_amount_after(x):
    logger.info(x.account_id)
    return round(float(m[(m.account==x.account_id)&(m.process_date==str(x.process_date))].amount.values[0]),2)

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
sql = f'''
                    SELECT
                    a.account_id,
                    a.process_date,
                    sum(
                    a.net_assets * b.exchange_rate
                    ) AS total
                    FROM
                    (
                    SELECT
                    account_id,
                        market_value + trade_balance + ipo_frozen_before_close AS net_assets,
                        currency,
                        process_date
                    FROM
                        account_balance
                    WHERE
                        account_id in ({str(list(accounts.account_id.unique())).replace("[","").replace("]","")})
                    AND process_date>='2019-06-02'
                    AND process_date<='2019-12-24'
                    ) a
                    LEFT JOIN (
                    SELECT
                    process_date,
                    currency,
                    exchange_rate
                    FROM
                    currency_history
                    WHERE
                    process_date>='2019-06-02'
                    AND process_date<='2019-12-24'
                    ) b ON a.process_date = b.process_date
                    AND a.currency = b.currency
group by  a.account_id,a.process_date
                '''
account_balance = pd.read_sql(sql,con)

def getBalanceDay(x):
    total_balance = account_balance[account_balance['account_id']==x]
    total = len(total_balance)
    total_1 = len(total_balance[total_balance.total<=10000])
    total_13 = len(total_balance[(total_balance.total>10000)&(total_balance.total<=30000)])
    total_35 = len(total_balance[(total_balance.total>30000)&(total_balance.total<=50000)])
    total_5 = len(total_balance[total_balance.total>50000])
    return [total,total_1,total_13,total_35,total_5]

acc_uniq['total'] = acc_uniq['balance_t'].apply(lambda x:x[0])
acc_uniq['total_1'] = acc_uniq['balance_t'].apply(lambda x:x[1])
acc_uniq['total_13'] = acc_uniq['balance_t'].apply(lambda x:x[2])
acc_uniq['total_35'] = acc_uniq['balance_t'].apply(lambda x:x[3])
acc_uniq['total_5'] = acc_uniq['balance_t'].apply(lambda x:x[4])

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
sql = f'''
select account_id,min(process_date) as cash_in_first_day from cash_flow where purpose = 1 and remark like "%存款%" and account_id in ({str(list(cash_out_df.account_id.unique())).replace("[","").replace("]","")}) group by account_id
'''
cash_in_time = pd.read_sql(sql,con)


con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
sql = f'''
select account_id,min(process_date) as cash_in_first_day from cash_flow where purpose = 2 and remark like "%提款%" and account_id in ({str(list(cash_out_df.account_id.unique())).replace("[","").replace("]","")}) group by account_id
'''
cash_out_time = pd.read_sql(sql,con)

def getCashoutfirstdayamount(x):
    print(x.account_id)
    a = cash_out_df[(cash_out_df['account_id']==x['account_id'])&(cash_out_df['process_date']==x['cash_out_first_day'])]
    if len(a) == 0:
        return getTotalNetAssets(x.account_id,x.cash_out_first_day)
    else:
        return a['amount_befor'].values[0]
cash_in_time['cash_out_first_day_amount'] = cash_in_time.apply(lambda x:getCashoutfirstdayamount(x),axis = 1)

con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")

sql = '''
select process_date from account_position where process_date group by process_date order by process_date desc 
'''
process_date_list = pd.read_sql(sql,con)

def getEmptyday(account,process_date,amount):
    print(account)
    empty_sql = f'''
                    SELECT
                    account_id,process_date,
                    sum(
                        (
                            market_value + trade_balance + ipo_frozen_before_close
                        ) * (
                            CASE
                            WHEN currency = 1 THEN
                                1.1656370206
                            WHEN currency = 2 THEN
                                1
                            WHEN 4 THEN
                                7.8415000000
                            END
                        )
                    ) AS total_asset
                FROM
                    account_balance
                WHERE
                    account_id = '{account}'
                AND process_date >= '2019-06-02'
                AND process_date <= '2019-12-24'
                GROUP BY
                    account_id,process_date
                order by process_date desc
             '''
    data = pd.read_sql(empty_sql,con)
    da = myiter(data[data['process_date']<process_date])
    counter = 0
    for i in da:
        rate = i.total_asset/amount
        if rate>0.99 and rate<1.01:
            counter+=1
        else:
            break
    print(f"account:{account}\tprocess_date:{process_date}\tcounter:{counter}")
    return counter

cash_out_df['empty_day'] = cash_out_df.apply(lambda x:getEmptyday(x.account_id,x.process_date,x.amount_befor),axis=1)

def getMaxValue(x):
    total_list = [x['total_1'],x['total_13'],x['total_35'],x['total_5']]
    max_total = max(total_list)
    return total_list.index(max_total)


con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="report", charset="utf8")
sql = f'''
select funds_account,stat_time_day,trade_money as total_deposit_amount,withdraw_money as total_withdraw_amount from user_golden_info where funds_account in ({str(cash_out_list).replace("[","").replace("]","")}) group by funds_account
'''
cash_info = pd.read_sql(sql,con)

sql_product = f'''
select funds_account,sum(volume*rate*closing_price) as total_amount,purpose from stock_transfer where funds_account in ({str(cash_out_list).replace("[","").replace("]","")}) group by funds_account,purpose
'''
product_info = pd.read_sql(sql_product,con)


con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="miningtrade", charset="utf8")
sql_assets_first = f'''
select account,net_assets from trade_user_money_backup_bak01 where info_date='20190131' and account in ({str(cash_out_list).replace("[","").replace("]","")})
'''

assets_first = pd.read_sql(sql_assets_first,con)
sql_assets_end = f'''
select account,net_assets from trade_user_money where info_date='20191227' and account in ({str(cash_out_list).replace("[","").replace("]","")})
'''
assets_end = pd.read_sql(sql_assets_end,con)


def getstNetAsset(x):
    asset_st = assets_first[assets_first.account==str(x.funds_account)]
    if len(asset_st)==0:
        return 0
    else:
        return asset_st.net_assets.values[0]
def getendNetAsset(x):
    asset_ed = assets_end[assets_end.account==str(x.funds_account)]
    if len(asset_ed)==0:
        return 0
    else:
        return asset_ed.net_assets.values[0]
cash_info['assets_start'] = cash_info.apply(lambda x:getstNetAsset(x),axis=1)
cash_info['assets_end'] = cash_info.apply(lambda x:getendNetAsset(x),axis=1)


def getproin(x):
    p_in = product_info[(product_info.funds_account==x.funds_account)&(product_info.purpose=='1')]
    if len(p_in)==0:
        return 0
    else:
        return p_in.total_amount.values[0]

def getproot(x):
    p_ot = product_info[(product_info.funds_account==x.funds_account)&(product_info.purpose=='2')]
    if len(p_ot)==0:
        return 0
    else:
        return p_ot.total_amount.values[0]

cash_info['pro_in'] = cash_info.apply(lambda x:getproin(x),axis=1)
cash_info['pro_ot'] = cash_info.apply(lambda x:getproot(x),axis=1)


con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="miningtrade", charset="utf8")

sql1 = f'''
SELECT
	account,
	net_assets,
	withdraw,
	info_date
FROM
	trade_user_money
WHERE
	account in ({str(list(acc_uniq_out.index)).replace("[","").replace("]","")})
AND info_date <= '20191224'
AND info_date >= '20190501'
ORDER BY
	info_date DESC

'''
trade1 = pd.read_sql(sql1,con)

sql2 = f'''
SELECT
	account,
	net_assets,
	withdraw,
	info_date
FROM
	trade_user_money_backup_bak01
WHERE
	account in ({str(list(acc_uniq_out.index)).replace("[","").replace("]","")})
AND info_date <= '20190701'
AND info_date >= '20190401'
ORDER BY
	info_date DESC

'''
trade2 = pd.read_sql(sql2,con)
trade_user_money = pd.concat([trade1,trade2])
trade_user_money['is_equal'] = trade_user_money['net_assets'] == trade_user_money['withdraw']
def getLastProcessDate(account,process_date):
    ret = accounts[(accounts.account_id==account)&(accounts.process_date<process_date)].process_date.values
    if len(ret)==0:
        return '2017-07-01'
    else:
        return ret[0]
def getEmptyDays(account,process_date):
    print(f"{account}\t{process_date}")
    last_cash_date  = getLastProcessDate(account,process_date)
    last_cash_date = int(str(last_cash_date).replace("-",""))
    equal_days = trade_user_money[(trade_user_money['account']==account) & (trade_user_money['info_date']>last_cash_date) & (trade_user_money['info_date']<int(str(process_date).replace("-","")))].is_equal
    equal_days_list = list(equal_days.values)
    if len(equal_days_list)==0:
        return 0
    if False not in equal_days_list:
        return len(equal_days_list)
    return equal_days_list.index(False)

cash_out_df['equal_days'] = cash_out_df.apply(lambda x:getEmptyDays(x.account_id,x.process_date),axis=1)