#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-18 下午5:20
'''

import MySQLdb
import os
import datetime
# os.chdir("/home/eos/git/hodgepodge")
import logging
import pandas as pd


logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("---RFM REAL TIME---")

def str_to_date(x):
    x_list = list(x)
    x_list.insert(4,"-")
    x_list.insert(7,"-")
    return "".join(x_list)


#获取用户ipo申购的记录
real_now = datetime.datetime.now()-datetime.timedelta(days=1)
now = datetime.datetime(year=2020,month=2,day=21)
while now<real_now:
    end = datetime.datetime(year=now.year,month=now.month,day=now.day,hour=23,minute=59,second=59)
    start = end-datetime.timedelta(days=1)



    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
    ipo_bms_sql = f'''
    SELECT
        account_id as account,
        amount,
        created_time
    FROM
        product_ipo_app
    WHERE
        created_time>'{str(start)}'
        and created_time<='{str(end)}'
    order by created_time asc
    '''
    ipo_bms_info = pd.read_sql(ipo_bms_sql,con)
    logger.info(f"[起始：{start} 终止：{end}] 从bms获取到{len(ipo_bms_info)}条ipo申购记录")
    account_ipo = pd.DataFrame([])
    if len(ipo_bms_info)>0:
        ipo_bms_info['process_date'] = ipo_bms_info.apply(lambda x:str(x['created_time'].date()),axis=1)
        ipo_df = ipo_bms_info[['account', 'amount', 'process_date']]
        account_ipo = ipo_df.sort_values(by=['account','process_date'],ascending=True)
        account_ipo = account_ipo.reset_index(drop=True)
        account_ipo['type'] = 'ipo'
        account_ipo['desc'] = ""
        logger.info(f"---{str(end)[:10]}ipo申购{len(account_ipo)}条数据---")
    else:
        logger.info(f"---{str(end)[:10]}ipo申购0条数据---")

    #获取用户交易记录
    trade_sql = f'''
    select process_date,account_id,sum(exchange_rate*trade_amount) as day_trade_amount from account_trade where process_date='{str(now)[:10]}' group by process_date,account_id
    '''
    trade_info = pd.read_sql(trade_sql,con)
    logger.info(f"[起始：{start} 终止：{end}] 从bms获取到{len(trade_info)}条交易记录")

    trade_info = trade_info[['account_id',  'day_trade_amount','process_date']]
    trade_info.columns = ['account', 'amount', 'process_date']
    trade_info_df = trade_info.sort_values(by=['account','process_date'],ascending=True)
    trade_info_df = trade_info_df.reset_index(drop = True)
    trade_info_df['type'] = 'trade'
    trade_info_df['desc'] = ""
    rfm_df = pd.DataFrame([])
    if len(trade_info_df)>0:
        logger.info(f"---{str(end)[:10]}交易{len(trade_info_df)}条数据---")
    else:
        logger.info(f"---{str(end)[:10]}交易0条数据---")
    if len(trade_info_df)>0:
        rfm_df = pd.concat([account_ipo,trade_info_df])
        rfm_df = rfm_df.sort_values(by=['account','process_date','type'],ascending=True)
        rfm_df = rfm_df.reset_index(drop = True)


    #获取存取款数据
    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
    cash_flow_sql = f'''
    select process_date,account_id,amount,purpose from cash_flow where process_date='{str(end)[:10]}' and (remark like "%存款%" or remark like "%提款%") and status = 'Confirmed'
    '''
    cash_flow_info = pd.read_sql(cash_flow_sql,con)
    logger.info(f"[起始：{start} 终止：{end}] 从bms获取到{len(cash_flow_info)}条存取款数据")
    cash_type_dict = {1:"cash_in",2:"cash_out"}
    if len(cash_flow_info)>0:
        cash_flow_info['type'] = cash_flow_info.apply(lambda x:cash_type_dict[x['purpose']],axis=1)
        cash_flow_info = cash_flow_info[['account_id','amount','process_date','type']]
        cash_flow_info.columns = ['account', 'amount', 'process_date','type']
        cash_flow_info = cash_flow_info.reset_index(drop = True)
        cash_flow_info['desc'] = ""
        logger.info(f"---{str(end)[:10]}存取款{len(cash_flow_info)}条数据---")
        rfm_df = pd.concat([rfm_df,cash_flow_info])
        rfm_df['process_date'] = rfm_df[['process_date']].astype('str')
        rfm_df = rfm_df.sort_values(by=['account','process_date'],ascending=True)
        rfm_df = rfm_df.reset_index(drop = True)
        rfm_df['amount'] = rfm_df['amount'].astype('float')
    else:
        logger.info(f"---{str(end)[:10]}存取款0条数据---")



    #获取转入转出数据
    product_io_sql = f'''
    select process_date,account_id as account,quantity as amount,product_id,purpose from product_flow where process_date="{str(end)[:10]}" and (remark like "%转入%" or remark like "%转出%")
    '''
    product_info = pd.read_sql(product_io_sql,con=con)
    logger.info(f"[起始：{start} 终止：{end}] 从bms获取到{len(product_info)}条转入转出数据")
    product_type_dict = {1:"product_in",2:"product_out"}
    if len(product_info)>0:
        product_info['type'] = product_info.apply(lambda x:product_type_dict[x['purpose']],axis=1)
        product_info['desc'] = product_info['product_id']
        product_info = product_info[['account', 'amount', 'process_date','type','desc']]
        product_info['amount'] = product_info['amount'].astype("float")
        product_info['process_date'] = product_info['process_date'].astype("str")
        product_info = product_info.sort_values(by=['account','process_date'],ascending=True)
        product_info = product_info.reset_index(drop = True)
        logger.info(f"---{str(end)[:10]}转出转数据{len(product_info)}条数据---")
    else:
        logger.info(f"---{str(end)[:10]}转出转数据0条数据---")
    if len(product_info)>0:
        rfm_df = pd.concat([rfm_df,product_info])
        rfm_df = rfm_df.sort_values(by=['account','process_date'],ascending=True)
        rfm_df = rfm_df.reset_index(drop = True)

    #存数据
    if len(rfm_df)>0:
        rfm_df['amount'] = rfm_df['amount'].astype('float')
        rfm_df['process_date'] = rfm_df[['process_date']].astype('str')
        from sqlalchemy import create_engine
        engine = create_engine("mysql://root:zunjiazichan123@192.168.5.106/app_data",encoding="utf8",echo=False)
        rfm_df.to_sql("rfm_base",con=engine,index=False, if_exists='append')
        logger.info(f"{str(end)[:10]}成功存入rfm_base{len(rfm_df)} 条数据")
    else:
        logger.info(f"{str(end)[:10]}非交易日")
    now = now+datetime.timedelta(days=1)
#取数据
# con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="app_data", charset="utf8")
# rfm = pd.read_sql("select account,process_date,type from rfm_base",con =con)

