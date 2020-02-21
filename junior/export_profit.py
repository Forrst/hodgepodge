#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-07 上午11:19
'''
from db.mysql.SqlUtil import Mysql
from tqdm import tqdm
db = Mysql("mysql5.105")
bms_from_date = '2019-02-01'
bms_to_date = '2019-11-29'


def getAccountFlow(accounts):
    xline = []
    for account in tqdm(accounts):
        deposit = 0
        withdraw = 0
        net_amount = 0
        #转入股票的金额
        deposit_product_sql = '''
                    select a.account_id,  
                       round(ifnull(sum(a.quantity * a.avg_price*ifnull(c.exchange_rate,d.exchange_rate)),0),2) as amount  
                    from product_flow a
                    left join product b on a.market_id = b.list_market_id and a.product_id = b.product_id
                    left join currency_history c on a.process_date = c.process_date  and ifnull(b.currency,'') = c.currency 
                    left join currency d on ifnull(b.currency,'') = d.currency
                    where 
                    a.account_id = '{}' and 
                    a.status = 'Confirmed' and 
                    a.process_date >= '{}'  and 
                    a.process_date <= '{}' and 
                    (a.remark like N'%%转入%%'  or remark like N'%%轉入%%' or  a.remark like N'%%转出%%') and 
                    a.purpose = 1
                    group by a.account_id;
                '''.format(account,bms_from_date,bms_to_date)
        deposit_product_data = db.execute(deposit_product_sql,'jcbms')
        if len(deposit_product_data) !=0:
            deposit+=deposit_product_data[0][1]
        #入金的金额
        deposit_cash_sql = '''
                    select 
                        a.account_id, 
                        round(ifnull(sum(a.amount * ifnull(b.exchange_rate,c.exchange_rate)),0),2) as amount  
                    from (select * from cash_flow where account_id = '{}' and (remark like '%%提款%%' or remark like '%%存款%%' or remark like "90\%入金;%") and purpose = 1 and process_date>='{}' and process_date<='{}' and status = 'Confirmed') a 
                    left join currency_history b on a.process_date = b.process_date and  a.currency = b.currency
                    left join currency c on a.currency = c.currency
                    group by a.account_id;
                '''.format(account,bms_from_date, bms_to_date)
        deposit_cash_data = db.execute(deposit_cash_sql,'jcbms')
        if len(deposit_cash_data) != 0:
            deposit+=deposit_cash_data[0][1]

        #出金金额
        withdraw_sql = '''
                    select a.account_id, 
                           round(ifnull(sum(a.amount * ifnull(b.exchange_rate,c.exchange_rate)),0),2) as amount  
                    from cash_flow a 
                    left join currency_history b on  a.process_date = b.process_date and a.currency = b.currency
                    left join currency c on a.currency = c.currency 
                    where
                    a.account_id = '{}' and
                    a.status = 'Confirmed' and   
                    a.process_date >= '{}' and 
                    a.process_date <= '{}'  and 
                    a.purpose = 2 and 
                     (a.remark like N'%提款%' )              
                    group by a.account_id;
                '''.format(account,bms_from_date,bms_to_date)
        withdraw_data = db.execute(withdraw_sql,'jcbms')
        if len(withdraw_data) !=0:
            withdraw+=withdraw_data[0][1]

        net_amount_sql = '''
        SELECT
            a.account_id,
            round(
                ifnull(
                    sum(
                        (
                            a.ipo_frozen_before_allot + a.market_value + a.trade_balance
                        ) * ifnull(
                            b.exchange_rate,
                            c.exchange_rate
                        )
                    ),
                    0
                ),
                3
            ) AS amount
        FROM
            account_balance a
        LEFT JOIN currency_history b ON a.process_date = b.process_date
        AND a.currency = b.currency
        LEFT JOIN currency c ON a.currency = c.currency
        WHERE
            a.process_date = '{}'
        AND a.account_id = '{}'
        GROUP BY
            a.account_id
        '''.format(bms_to_date,account)
        net_amount_data = db.execute(net_amount_sql,'jcbms')
        if len(net_amount_data) != 0:
            net_amount+=net_amount_data[0][1]
        xline.append([account,deposit,withdraw,net_amount])
    return xline

def getProfit(accounts):
    xline = []
    for account in tqdm(accounts):
        profit = 0
        profit_sql = '''
                        select b.account_id,
                           round(sum(funMin(31.9,b.loan_charge)),6) as sm 
                    from product_ipo_announcement a
                    inner join (select ipo_id,account_id,loan_charge from product_ipo_app where account_id = '{}') b on a.ipo_id = b.ipo_id
                    where  a.app_posted = 'Y'and close_time>='{}' and 
                    close_time<='{}'
                    group by b.account_id having(sum(funMin(31.9,b.loan_charge)))>0;
                '''.format(account,bms_from_date,bms_to_date)
        profit_data = db.execute(profit_sql,'jcbms')
        if len(profit_data) != 0:
            profit+=profit_data[0][1]
        xline.append([account,profit])
    return xline

import pandas as pd
flow = []
df = pd.DataFrame(flow)
df.to_csv("flow.csv",encoding="utf-8")

profit_df = pd.DataFrame(profit)
profit_df.to_csv("profit.csv",encoding="utf-8")
