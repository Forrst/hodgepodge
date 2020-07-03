#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-07 上午9:33
'''
'''
鑫财通
资产
港股打新费用计算脚本
参数如下
python ~/git/hodgepodge/junior/export_report.py -d /home/eos/data/consumer_profit -c rpt3 -f 20191101 -t 20191129 -l  /home/eos/data/consumer_profit/accounts
rpt1 = "资金账户","首次入金时间","累计入金/转股金额(港币)","累计出金金额(港币)","打新笔数","交易笔数"
rpt2 = "资金账户","打新佣金费用"
rpt3 = "资金账户","累计入金/转股金额(港币)","累计出金金额(港币)","实时资产净值"
'''

import os
import traceback
import sys
import getopt
import pymssql
import csv
import pymysql
import configparser
import codecs

g_list_account=set()
# return ini configure reader
def configure():
    if not hasattr(configure,'cnf'):
        cur_path = os.path.dirname(os.path.realpath(__file__))
        cfg_path=os.path.join(cur_path,"configure.ini")
        configure.cnf=configparser.ConfigParser()
        configure.cnf.readfp(open(cfg_path))
    return configure.cnf

# get mysql connection object
def mysql_conn():
    if not hasattr(mysql_conn,'conn'):
        cnf = configure()
        mysql_conn.conn = pymysql.connect(
            host=cnf.get("mysql","host"),
            port=int(cnf.get("mysql","port")),
            user=cnf.get("mysql","user"),
            password=cnf.get("mysql","password"),
            database=cnf.get("mysql","database"),
            charset=cnf.get("mysql","charset"),
            cursorclass=pymysql.cursors.DictCursor)
    return mysql_conn.conn

# mysql fetch all
def mysql_fetch_all(sql):
    my_cur = mysql_conn().cursor()
    my_cur.execute(sql)
    result = my_cur.fetchall()
    return result

# mssql fetch all
def mssql_fetch_all(sql):
    ms_cur = mssql_conn().cursor()
    ms_cur.execute(sql)
    result = ms_cur.fetchall()
    return result

# get mysql connection object
def mssql_conn():
    if not hasattr(mssql_conn,'conn'):
        cnf = configure()
        mssql_conn.conn = pymssql.connect(
            host=cnf.get("mssql","host"),
            port=int(cnf.get("mssql","port")),
            user=cnf.get("mssql","user"),
            password=cnf.get("mssql","password"),
            database=cnf.get("mssql","database"),
            charset=cnf.get("mssql","charset"))
    return mssql_conn.conn

def saveToCsv(allLine, filename, mode, encoding, skipFirstLine):
    '''
    mode: a(append,附加), w(清空文件内容然后写入)
    encoding = 'utf_8'或'utf8'等.
    skipFirstLine = True
    '''
    assert type(allLine) is list
    if 0 < len(allLine):
        assert type(allLine[0]) in (list, tuple)
        if skipFirstLine:
            allLine = allLine[1:]
    dirName = os.path.dirname(filename)
    if dirName and (not os.path.exists(dirName)):
        os.makedirs(dirName)
    with open(filename, mode) as f:
        csvWriter = csv.writer(f)
        csvWriter.writerows(allLine)

#参数计算
def calc_args():
    file_path = None
    command = None
    from_date = None
    to_date = None
    list_account_file_path = None
    opts, args = getopt.getopt(sys.argv[1:], 'd:c:f:t:l:')
    for op, value in opts:
        if op == '-d':
            file_path = value
        if op == '-c':
            command = value
        if op == '-f':
            from_date = value
        if op == '-t':
            to_date = value
        if op == '-l':
            list_account_file_path = value
    return file_path,command,from_date,to_date,list_account_file_path

def init():
    # init mysql
    if mysql_conn() is None:
        print("mysql connect failed")
        return False
    # init mssql
    if mssql_conn() is None:
        print("mssql connect failed")
        return False
    return True

#business logic below
#split from_date ,to_date to bos_from_date, bos_to_date, bms_from_date, bms_to_date,
#before 20190801 we use bos data
def split_date(from_date,to_date):
    bos_from_date = ""
    bos_to_date =""
    bms_from_date = ""
    bms_to_date = ""
    #for bos
    if from_date<="20190731":
        bos_from_date = from_date
        if to_date>="20190801":
            bos_to_date = "20190731"
        else:
            bos_to_date = to_date

    #for bms
    if to_date>="20190801":
        bms_to_date = to_date
        if from_date <="20190731":
            bms_from_date = "20190801"
        else:
            bms_from_date = from_date

    return bos_from_date,bos_to_date,bms_from_date,bms_to_date


#查询累记入金
#key:account_id , amount
g_account_acrrued_cash_in = {}
g_account_first_cash_in_time = {}
def query_acrrued_cash_in(bos_from_date, bos_to_date, bms_from_date,bms_to_date):
    global g_account_acrrued_cash_in
    global g_account_first_cash_in_time
    g_account_acrrued_cash_in.clear()
    if bos_from_date != "" and bos_to_date != "":
        sql = '''
            select 
                a.client_acc_id, 
                round(isnull(sum(a.amount * b.rate),0),2) as amount,
                min(a.buss_date) as buss_date
            from trans_cacc_cash_in a 
            left join ccy_history b on a.ccy = b.ccy and a.buss_date = b.buss_date
            where (a.remark like N'%%提款%%' or a.remark like N'%%存款%%') and 
                a.status = 'Confirmed'  and  
                a.buss_date >= '{}' and 
                a.buss_date <= '{}'  
            group by a.client_acc_id;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for rs in result:
            g_account_acrrued_cash_in[rs[0]] = rs[1]
            if rs[0] in g_account_first_cash_in_time:
                if g_account_first_cash_in_time[rs[0]]>=rs[2]:
                    g_account_first_cash_in_time[rs[0]]=rs[2]
            else:
                g_account_first_cash_in_time[rs[1]]=rs[2]
    print(''' {},{}'''.format(bms_from_date,bms_to_date))
    if bms_from_date != "" and bms_to_date !="":
        sql = '''
            select 
                a.account_id, 
                round(ifnull(sum(a.amount * ifnull(b.exchange_rate,c.exchange_rate)),0),2) as amount,
                min(a.process_date) as process_date  
            from cash_flow a 
            left join currency_history b on a.process_date = b.process_date and  a.currency = b.currency
            left join currency c on a.currency = c.currency
            where (a.remark like '%%提款%%' or a.remark like '%%存款%%' or a.remark like "90\%入金;%") and 
                a.status = 'Confirmed'  and  
                a.process_date >= '{}' and 
                a.process_date <= '{}' and 
                a.purpose = 1
            group by a.account_id;
        '''.format(bms_from_date, bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if  line[0] not in g_account_acrrued_cash_in:
                g_account_acrrued_cash_in[line[0]] = line[1]
            else:
                g_account_acrrued_cash_in[line[0]]+= line[1]
                g_account_acrrued_cash_in[rs['account_id']] = rs['amount']
            if rs['account_id'] in g_account_first_cash_in_time:
                if g_account_first_cash_in_time[rs['account_id']]>=rs['process_date']:
                    g_account_first_cash_in_time[rs['account_id']]=rs['process_date']
            else:
                g_account_first_cash_in_time[rs['account_id']]=rs['process_date']
    return True


#查询累计入仓
#key:account_id , amount
g_account_acrrued_position_in = {}
def query_acrrued_position_in(bos_from_date, bos_to_date, bms_from_date,bms_to_date):
    global g_account_acrrued_position_in
    g_account_acrrued_position_in.clear()
    if bos_from_date !="" and bos_to_date !="":
        sql = '''
            select a.client_acc_id,  
                   round(isnull(sum(a.qty * a.avg_price*c.rate),0),2) as amount,
                   min(a.buss_date) as buss_date  
            from trans_cacc_product_in a, product b, ccy_history c 
            where 
            a.status = 'Confirmed' and 
            a.product_id = b.product_id and 
            b.ccy = c.ccy and a.buss_date = c.buss_date and 
            (a.remark like N'%%转入%%'  or remark like N'%%轉入%%' or  a.remark like N'%%转出%%')  and 
            a.buss_date >= '{}'  and 
            a.buss_date <= '{}'
            group by client_acc_id;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for rs in result:
            g_account_acrrued_position_in[rs[0]]=rs[1]
            if rs[0] in g_account_first_cash_in_time:
                if g_account_first_cash_in_time[rs[0]].isoformat()>=rs[2].isoformat():
                    g_account_first_cash_in_time[rs[0]]=rs[2]
            else:
                g_account_first_cash_in_time[rs[0]]=rs[2]
    if bms_from_date != "" and bms_to_date !="":
        sql = '''
            select a.account_id,  
               round(ifnull(sum(a.quantity * a.avg_price*ifnull(c.exchange_rate,d.exchange_rate)),0),2) as amount,
               min(a.process_date) as process_date  
            from product_flow a
            left join product b on a.market_id = b.list_market_id and a.product_id = b.product_id
            left join currency_history c on a.process_date = c.process_date  and ifnull(b.currency,'') = c.currency 
            left join currency d on ifnull(b.currency,'') = d.currency
            where 
            a.status = 'Confirmed' and 
            a.process_date >= '{}'  and 
            a.process_date <= '{}' and 
            (a.remark like N'%%转入%%'  or remark like N'%%轉入%%' or  a.remark like N'%%转出%%') and 
            a.purpose = 1
            group by a.account_id;
        '''.format(bms_from_date,bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_acrrued_position_in:
                g_account_acrrued_position_in[line[0]] = line[1]
            else:
                g_account_acrrued_position_in[line[0]]+= line[1]
            if rs['account_id'] in g_account_first_cash_in_time:
                if g_account_first_cash_in_time[rs['account_id']].isoformat()>=rs['process_date'].isoformat():
                    g_account_first_cash_in_time[rs['account_id']]=rs['process_date']
            else:
                g_account_first_cash_in_time[rs['account_id']]=rs['process_date']
    return True

#累记出金金额
#{account_id:amount}
g_account_acrrued_cash_out = {}
def query_accured_cash_out(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    global g_account_acrrued_cash_out
    g_account_acrrued_cash_out.clear()
    if bos_from_date!="" and bos_to_date !="" :
        sql = '''
            select a.client_acc_id, 
                   round(isnull(sum(a.amount * b.rate),0),2) as amount  
            from trans_cacc_cash_out a 
            left join ccy_history b on a.ccy = b.ccy and a.buss_date = b.buss_date 
            where (a.remark like N'%提款%' ) and 
            a.status = 'Confirmed' and   
            a.buss_date >= '{}' and 
            a.buss_date <= '{}'   
            group by a.client_acc_id;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for line in result:
            g_account_acrrued_cash_out[line[0]] = line[1]
    if bms_from_date!="" and bms_to_date !="" :
        sql = '''
            select a.account_id, 
                   round(ifnull(sum(a.amount * ifnull(b.exchange_rate,c.exchange_rate)),0),2) as amount  
            from cash_flow a 
            left join currency_history b on  a.process_date = b.process_date and a.currency = b.currency
            left join currency c on a.currency = c.currency 
            where
            a.status = 'Confirmed' and   
            a.process_date >= '{}' and 
            a.process_date <= '{}'  and 
            a.purpose = 2 and 
             (a.remark like N'%提款%' )              
            group by a.account_id;
        '''.format(bms_from_date,bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_acrrued_cash_out:
                g_account_acrrued_cash_out[line[0]] = line[1]
            else:
                g_account_acrrued_cash_out[line[0]]+= line[1]
    return True

#累计打新笔数
#{account_id,cnt}
g_account_accrued_ipo_cnt = {}
def query_accured_ipo_cnt(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    global g_account_accrued_ipo_cnt
    g_account_accrued_ipo_cnt.clear()
    if bos_from_date!="" and bos_to_date !="" :
        sql = '''
            select b.client_acc_id, 
                isnull(count(1),0)
            from product_ipo a
            inner join product_ipo_app b on a.id = b.id
            where 
                a.close_time >= '{}' and 
                a.close_time <= '{}' 
            group by b.client_acc_id;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for line in result:
            g_account_accrued_ipo_cnt[line[0]] = line[1]
    if bms_from_date !="" and bms_to_date !="":
        sql = '''
            select b.account_id, 
                ifnull(count(1),0) as amount
            from product_ipo_announcement a
            inner join product_ipo_app b on a.ipo_id = b.ipo_id
            where 
                a.close_time >= '{}' and 
                a.close_time <= '{}' 
            group by b.account_id;
        '''.format(bms_from_date,bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_accrued_ipo_cnt:
                g_account_accrued_ipo_cnt[line[0]] = line[1]
            else:
                g_account_accrued_ipo_cnt[line[0]]+= line[1]
    return True

#累记交易笔数
#{account_id,cnt}
g_account_accrued_trade_cnt = {}
def query_accured_trade_cnt(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    global g_account_accrued_trade_cnt
    g_account_accrued_trade_cnt.clear()
    if bos_from_date!="" and bos_to_date !="" :
        sql = '''
            select client_acc_id, count(1) 
            from trans_cacc_trade 
            where 
            status = 'Confirmed' and 
            buss_date >= '{}' and buss_date <= '{}' 
            group by client_acc_id;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for line in result:
            g_account_accrued_trade_cnt[line[0]] = line[1]
    if bms_from_date !="" and bms_to_date !="":
        sql = '''
            select 
            account_id, 
            count(1) as amount
            from account_trade 
            where 
            status = 'Confirmed' and 
            process_date >= '{}' and process_date <= '{}' 
            group by account_id;
        '''.format(bms_from_date,bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_accrued_trade_cnt:
                g_account_accrued_trade_cnt[line[0]] = line[1]
            else:
                g_account_accrued_trade_cnt[line[0]]+= line[1]
    return True

#打新佣金费用 尊嘉实得
g_account_accrued_ipo_loan_fee = {}
def query_accured_ipo_loan_fee(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    global g_account_accrued_ipo_loan_fee
    g_account_accrued_ipo_loan_fee.clear()
    if bos_from_date!="" and bos_to_date !="" :
        sql = '''
        select b.client_acc_id,
                    round((sum(case when b.charge-a.charge>31.9 then 31.9 else b.charge-a.charge end )+sum(b.interest)),6) as sm 
            from product_ipo a
            inner join product_ipo_app b on a.id = b.id
            where  a.app_posted = 'Y'and convert(varchar(8),close_time,112)>='{}' and 
            convert(varchar(8),close_time,112)<='{}' 
            group by b.client_acc_id having(sum(case when b.charge-a.charge>31.9 then 31.9 else b.charge-a.charge end))>0;
        '''.format(bos_from_date,bos_to_date)
        result = mssql_fetch_all(sql)
        for line in result:
            g_account_accrued_ipo_loan_fee[line[0]] = line[1]
    if bms_from_date !="" and bms_to_date !="":
        sql = '''
                select b.account_id,
                   round(sum(funMin(31.9,b.loan_charge)),6) as amount 
            from product_ipo_announcement a
            inner join product_ipo_app b on a.ipo_id = b.ipo_id
            where  a.app_posted = 'Y'and date_format(close_time,'%Y%m%d')>='{}' and 
            date_format(close_time,'%Y%m%d')<='{}'
            group by b.account_id having(sum(funMin(31.9,b.loan_charge)))>0;
        '''.format(bms_from_date,bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_accrued_ipo_loan_fee:
                g_account_accrued_ipo_loan_fee[line[0]] = line[1]
            else:
                g_account_accrued_ipo_loan_fee[line[0]]+= line[1]
    return True

#资产净值
g_account_asset={}
def query_accured_asset(bos_to_date,bms_to_date):
    global g_account_asset
    g_account_asset.clear()
    if  bos_to_date !="" :
        sql = '''
           select t1.client_acc_id, 
		   sum(t1.amount*t2.rate +t1.mv*t2.rate) as amount
		   from 
		   DAYEND_CACC_CASH_BAL t1 
		   inner join ccy_history t2 on t1.ccy = t2.ccy and t1.dayend_date = t2.buss_date
		   where t1.dayend_date = '{}'
		   group by t1.client_acc_id
        '''.format(bos_to_date)
        result = mssql_fetch_all(sql)
        for line in result:
            g_account_asset[line[0]] = line[1]
    if  bms_to_date !="":
        sql = '''
           select t1.account_id, 
		   sum(round(round(t1.trade_balance*ifnull(t2.exchange_rate,t3.exchange_rate),4) + round(t1.market_value*ifnull(t2.exchange_rate,t3.exchange_rate),4)+round(t1.ipo_frozen_before_allot*ifnull(t2.exchange_rate,t3.exchange_rate),4),3)) as amount
		   from 
		   account_balance t1 
		   left join currency_history t2 on t1.process_date = t2.process_date and t1.currency = t2.currency 
           left join currency t3 on t1.currency = t3.currency
		   where t1.process_date = '{}'
		   group by t1.account_id
        '''.format(bms_to_date)
        result = mysql_fetch_all(sql)
        for rs in result:
            # line = list(rs.values())
            line = [rs['account_id'],rs['amount']]
            if line[0] not in g_account_asset:
                g_account_asset[line[0]] = line[1]
            else:
                g_account_asset[line[0]]+= line[1]
    return True

#鑫财通
def export_rpt1(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    func_name = sys._getframe().f_code.co_name
    print('''{}[Start]'''.format(func_name))
    global g_account_acrrued_cash_in
    global g_account_acrrued_cash_out
    global g_account_acrrued_position_in
    global g_list_account
    allline = []
    allline.append(["资金账户","首次入金时间","累计入金/转股金额(港币)","累计出金金额(港币)","打新笔数","交易笔数"])
    if not query_acrrued_cash_in(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_acrrued_position_in(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_accured_cash_out(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_accured_ipo_cnt(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_accured_trade_cnt(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    for account in g_list_account:
        cash_position_in = 0
        cash_out = 0
        ipo_cnt = 0
        trade_cnt = 0
        if account in g_account_acrrued_cash_in:
            cash_position_in+=g_account_acrrued_cash_in[account]
        if account in g_account_acrrued_position_in:
            cash_position_in+=g_account_acrrued_position_in[account]
        if account in g_account_acrrued_cash_out:
            cash_out+=g_account_acrrued_cash_out[account]
        if account in g_account_accrued_ipo_cnt:
            ipo_cnt+=g_account_accrued_ipo_cnt[account]
        if account in g_account_accrued_trade_cnt:
            trade_cnt+=g_account_accrued_trade_cnt[account]
        if account not in g_account_first_cash_in_time:
            print(f"nor first cash in info for {account}")
            g_account_first_cash_in_time[account] = 'Null'
        allline.append([account,g_account_first_cash_in_time[account],cash_position_in,cash_out,ipo_cnt,trade_cnt])
    saveToCsv(allline,file_name,"w","utf8",False)
    print('''{}[Success->{}]'''.format(func_name, file_name))
    return True

#港股打新费用
def export_rpt2(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    func_name = sys._getframe().f_code.co_name
    print('''{}[Start]'''.format(func_name))
    global g_account_accrued_ipo_loan_fee
    global g_list_account
    allline = []
    allline.append(["资金账户","打新佣金费用"])

    if not query_accured_ipo_loan_fee(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False

    for account in g_list_account:
        fee = 0

        if account in g_account_accrued_ipo_loan_fee:
            fee+=g_account_accrued_ipo_loan_fee[account]
        allline.append([account,fee])
    saveToCsv(allline,file_name,"w","utf8",False)
    print('''{}[Success->{}]'''.format(func_name, file_name))
    return True

def export_rpt3(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date):
    func_name = sys._getframe().f_code.co_name
    print('''{}[Start]'''.format(func_name))
    global g_account_acrrued_cash_in
    global g_account_acrrued_cash_out
    global g_account_acrrued_position_in
    global g_list_account
    allline = []
    allline.append(["资金账户","累计入金/转股金额(港币)","累计出金金额(港币)","实时资产净值"])
    if not query_acrrued_cash_in(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_acrrued_position_in(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_accured_cash_out(bos_from_date,bos_to_date,bms_from_date,bms_to_date):
        return False
    if not query_accured_asset(bos_to_date,bms_to_date):
        return False
    for account in g_list_account:
        cash_position_in = 0
        cash_out = 0
        asset = 0
        if account in g_account_acrrued_cash_in:
            cash_position_in+=g_account_acrrued_cash_in[account]
        if account in g_account_acrrued_position_in:
            cash_position_in+=g_account_acrrued_position_in[account]
        if account in g_account_acrrued_cash_out:
            cash_out+=g_account_acrrued_cash_out[account]
        if account in g_account_asset:
            asset+=g_account_asset[account]
        allline.append([account,cash_position_in,cash_out,asset])
    saveToCsv(allline,file_name,"w","utf8",False)
    print('''{}[Success->{}]'''.format(func_name, file_name))
    return True

def load_file_into_list(filename, split = ','):
    allLine = []
    with codecs.open(filename, "r", "utf-8") as f:
        cols = None
        for line in f.readlines():
            line = line.strip('\r\n')
            if not line:
                continue
            if cols is None:
                cols = line.split(split)
                allLine.append(cols)
                continue
            fields = line.split(split)
            allLine.append(fields)
    return allLine

def read_list_account(file_path):
    global g_list_account
    lines = load_file_into_list(file_path)
    for line in lines:
        if line[0] not in g_list_account:
            g_list_account.add(line[0])
    return True

def process():
    file_path,command,from_date,to_date,list_account_file_path = calc_args()
    if list_account_file_path is None or not os.path.exists(list_account_file_path):
        print("Please input the correct list account file path(-l)")
        return False
    if not read_list_account(list_account_file_path):
        return False
    bos_from_date,bos_to_date,bms_from_date,bms_to_date = split_date(from_date,to_date)
    print('''bos[{}->{}],bms[{}->{}]'''.format(bos_from_date, bos_to_date, bms_from_date, bms_to_date))
    ret = False
    while(True):
        #rpt1:鑫财通报告
        if command == "rpt1":
            file_name = os.path.join(file_path,'''{}_{}_鑫财通.csv'''.format(from_date,to_date))
            ret=export_rpt1(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date)
            if not ret:
                break
        #rpt2:港股打新费用
        if command == "rpt2":
            file_name = os.path.join(file_path,'''{}_{}_港股打新费用.csv'''.format(from_date,to_date))
            ret=export_rpt2(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date)
            if not ret:
                break
        #rpt3:资产
        if command == "rpt3":
            file_name = os.path.join(file_path,'''{}_{}_资产.csv'''.format(from_date,to_date))
            ret=export_rpt3(file_name,bos_from_date,bos_to_date,bms_from_date,bms_to_date)
            if not ret:
                break
        break

    if not ret:
        print("{}[Fail]".format(command))
    return True

if __name__=="__main__":
    try:
        if not init():
            sys.exit(0)
        process()
    except:
        s=traceback.format_exc()
        print(s)
        sys.exit(1)
    sys.exit(0)