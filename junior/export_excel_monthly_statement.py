#!/usr/bin/python
# -*- coding:utf-8 -*-
import os
import sys
import csv
import xlwt
import getopt
import MySQLdb
from datetime import datetime
import time
###############################################################################
MSSQL_HOST = '10.8.8.5'
MSSQL_PORT = 1433
MSSQL_USER = 'beijingadmin'
MSSQL_PSWD = 'JuniorChina1'
MSSQL_DB = 'bos_juniorb'
MSSQL_CHARSET = 'utf8'
MSSQL_CONN = None


###############################################################################
def initMssqlConnection():
    global MSSQL_CONN
    MSSQL_CONN = MySQLdb.connect(
        host=MSSQL_HOST,
        port=MSSQL_PORT,
        user=MSSQL_USER,
        passwd=MSSQL_PSWD,
        db=MSSQL_DB,
        charset=MSSQL_CHARSET)


def select_Mssql(cursor, sqlStr, isListNotDict=True):
    cursor.execute(sqlStr)
    if isListNotDict:
        head = [col[0] for col in cursor.description]
        results = cursor.fetchall()
        results.insert(0, head)
    else:
        results = [dict(zip(head, data)) for data in cursor.fetchall()]
    return results


def saveToFile(wbk,allLine, filename, mode, encoding, skipFirstLine,Sheet):
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

    # 新建一个名为Sheet1的excel sheet。此处的cell_overwrite_ok =True是为了能对同一个单元格重复操作。
    sheet = wbk.add_sheet(Sheet,cell_overwrite_ok=True)

    for i in range(len(allLine)):
        #对allLine的每个子元素作遍历，
        for j in range(len(allLine[i])):
            #将每一行的每个元素按行号i,列号j,写入到excel中。
            sheet.write(i,j,allLine[i][j])
        

def bos_DailyTrades(wbk,file_path, from_date, to_date):
    '''DailyTrades'''
    sqlStr = '''
        SELECT
            RIGHT (trade_id, 8) AS 'Ref.No',
            process_date AS 'Tran.Date',
            a.settle_date AS 'Settle Date',
            CASE
        WHEN buy_sell = 1 THEN
            'Buy'
        WHEN buy_sell = 2 THEN
            'Sell'
        END AS 'Type',
         CONCAT(
            a.product_id,
            ':',
            CASE
        WHEN market_id = 1 THEN
            'HK'
        WHEN market_id = 2 THEN
            'US'
        WHEN market_id = 4 THEN
            'SHA'
        WHEN market_id = 8 THEN
            'SZA'
        END
        ) AS 'Product',
         b.external_code AS 'Isin Code',
         a.product_description AS 'Name',
         avg_price AS 'Avg.Price',
         quantity AS 'Quantity',
         trade_amount AS 'Trade Amount',
         charges AS 'Mkt Charges',
         commission AS 'Commission',
         net_amount AS 'Net Amount'
        FROM
            (
                SELECT
                    *
                FROM
                    account_trade
                WHERE
                    account_id = '88888881'
                AND process_date >= '{}'
                AND process_date <= '{}'
            ) a
        LEFT JOIN product b ON a.product_id = b.product_id
        AND a.market_id = b.list_market_id
    '''.format(from_date, to_date)
    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    saveToFile(wbk,allLine, file_path, 'w', 'utf8', False,'Transactions')

def bos_PortfolioSummary(wbk,file_path, from_date, to_date):
    '''Portfolio Summary'''
    sqlStr = '''
    select 
        b.ccy as Ccy,
        b.T0_AMT as 'Today Balance',
        b.SETTLE_AMT as 'Pendong Settlement',
        b.amount as 'Net Balance',
        isnull(c.Equity,0) as Equity,
        isnull(c.Bonds,0) as Bonds,
        isnull(c.funds,0) as funds,
        isnull(c.Others,0) as Others,
        isnull((c.Equity + c.Bonds + c.funds+c.Others),0) as 'Total Investment',
        isnull(ROUND(d.RATE,5),0) as RATE,
        isnull(ROUND((b.amount + c.Equity + c.Bonds + c.funds+c.Others)*d.RATE,2),0) as 'Protfolio Value HKD'
    from DAYEND_CACC_CASH_BAL b
    left join (select 
            a.ccy as 'Ccy',
            0.00 as 'Bonds',
            0.00 as 'Funds',
            0.00 as 'Others',
            sum(a.MV) as 'Equity'
            from 
            (select 
            e.CLIENT_ACC_ID,
            case when (RIGHT(e.PRODUCT_ID,3) in ('SZA','SHA')) then 'CNY' when RIGHT(e.PRODUCT_ID,2) = 'HK' then 'HKD' when RIGHT(e.PRODUCT_ID,2) = 'US' then 'USD' end as ccy,
            e.MV
            from DAYEND_CACC_PRO_BAL_DETAIL e
            where e.DAYEND_DATE = '{}' and e.CLIENT_ACC_ID = '88888881') a GROUP by a.ccy) c on c.Ccy = b.ccy
    left join CCY_HISTORY d on d.CCY = b.ccy and d.BUSS_DATE = b.DAYEND_DATE
    where b.dayend_date = '{}' and b.CLIENT_ACC_ID = '88888881';
    '''.format(to_date,to_date)
    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    saveToFile(wbk,allLine, file_path, 'w', 'utf8', False,'Portfolio Summary')


def bos_Position(wbk,file_path, from_date, to_date):
    '''Position'''
    sqlStr = '''
    select 
    LEFT(a.PRODUCT_ID,CHARINDEX(':',a.PRODUCT_ID,1)-1) as Code,
    b.ISIN_CODE as 'Isin Code',
    b.CCY as Ccy,
    b.Name as Name,
    a.T0_qty as 'Today Balance',
    a.SETTLE_QTY as 'Pending Settle',
    a.QTY as 'Net Balance',
    a.NOMINAL as 'Closing Price',
    a.MV as 'Market Value',
    0 as Margin,
    0.00 as 'Marginable Value'
    from DAYEND_CACC_PRO_BAL_DETAIL a
    left join PRODUCT b on b.PRODUCT_ID = a.PRODUCT_ID
    where DAYEND_DATE = '{py_yyyymmdd}' and RIGHT(a.PRODUCT_ID,3) = 'SZA' and a.CLIENT_ACC_ID = '88888881'
    UNION ALL
    select 
    LEFT(a.PRODUCT_ID,CHARINDEX(':',a.PRODUCT_ID,1)-1) as Code,
    b.ISIN_CODE as 'Isin Code',
    b.CCY as Ccy,
    b.Name as Name,
    a.T0_qty as 'Today Balance',
    a.SETTLE_QTY as 'Pending Settle',
    a.QTY as 'Net Balance',
    a.NOMINAL as 'Closing Price',
    a.MV as 'Market Value',
    0 as Margin,
    0.00 as 'Marginable Value'
    from DAYEND_CACC_PRO_BAL_DETAIL a
    left join PRODUCT b on b.PRODUCT_ID = a.PRODUCT_ID
    where DAYEND_DATE = '{py_yyyymmdd}' and RIGHT(a.PRODUCT_ID,3) = 'SHA' and a.CLIENT_ACC_ID = '88888881'
    UNION ALL
    select 
    LEFT(a.PRODUCT_ID,CHARINDEX(':',a.PRODUCT_ID,1)-1) as Code,
    b.ISIN_CODE as 'Isin Code',
    b.CCY as Ccy,
    b.Name as Name,
    a.T0_qty as 'Today Balance',
    a.SETTLE_QTY as 'Pending Settle',
    a.QTY as 'Net Balance',
    a.NOMINAL as 'Closing Price',
    a.MV as 'Market Value',
    0 as Margin,
    0.00 as 'Marginable Value'
    from DAYEND_CACC_PRO_BAL_DETAIL a
    left join PRODUCT b on b.PRODUCT_ID = a.PRODUCT_ID
    where a.DAYEND_DATE = '{py_yyyymmdd}' and right(a.PRODUCT_ID,2) = 'HK' and a.CLIENT_ACC_ID = '88888881'
    UNION ALL
    select 
    LEFT(a.PRODUCT_ID,CHARINDEX(':',a.PRODUCT_ID,1)-1) as Code,
    b.ISIN_CODE as 'Isin Code',
    b.CCY as Ccy,
    b.Name as Name,
    a.T0_qty as 'Today Balance',
    a.SETTLE_QTY as 'Pending Settle',
    a.QTY as 'Net Balance',
    a.NOMINAL as 'Closing Price',
    a.MV as 'Market Value',
    0 as Margin,
    0.00 as 'Marginable Value'
    from DAYEND_CACC_PRO_BAL_DETAIL a
    left join PRODUCT b on b.PRODUCT_ID = a.PRODUCT_ID
    where a.DAYEND_DATE = '{py_yyyymmdd}' and right(a.PRODUCT_ID,2) = 'US' and a.CLIENT_ACC_ID = '88888881';
    '''.format(py_yyyymmdd=to_date)

    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    saveToFile(wbk,allLine, file_path, 'w', 'utf8', False,'Position')

def bos_corpaction(wbk,file_path, from_date, to_date):
    '''CA'''
    sqlStr = '''
        SELECT
            c.product_id AS 'Code',
            c.product_description AS 'Name',
            b.bc_quantity AS 'Book Close Qty',
            a.description AS 'Desscription',
            CASE
        WHEN b.dividend_currency = 2 THEN
            'HKD'
        WHEN b.dividend_currency = 4 THEN
            'USD'
        WHEN b.dividend_currency = 1 THEN
            'CNY'
        END AS 'Dividend Ccy',
         b.dividend_amount AS 'Dividend Amount',
         b.dividend_quantity AS 'Dividend/Bonus Qty',
         a.pay_date AS 'Estimated Payable Date'
        FROM
            (
                SELECT
                    event_id,
                    product_id,
                    market_id,
                    description,
                    pay_date
                FROM
                    corp_action
                WHERE
                    posted_date >= '2019-08-01'
                AND posted_date <= '2019-09-30'
            ) a
        INNER JOIN (
            SELECT
                *
            FROM
                corp_action_detail
            WHERE
                account_id = '88888881'
        ) b ON a.event_id = b.event_id
        LEFT JOIN product c ON a.product_id = c.product_id
        AND a.market_id = c.list_market_id
    '''.format(from_date, to_date)
    #sqlStr = sqlStr.format(from_date, to_date)
    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    #allLine.insert(0, ["Code", "Name", "Book Close Qty", "Desscription", "Dividend Ccy", "Dividend Amount","Dividend/Bonus Qty","Estimated Payable Date"])
    saveToFile(wbk,allLine, file_path, 'w', 'utf8', False,'CorpAction')

def query_begin_balance(allLine, ms_cur, buss_date, ccy ):
    sql = '''
        select convert(varchar(8),dayend_date,112) ,amount from dayend_cacc_cash_bal where dayend_date  = convert(varchar(8),dateadd(day, -1, '{}'),112) and client_acc_id ='88888881' and ccy = '{}'
    '''.format(buss_date,ccy)
    ms_cur.execute(sql)
    result = ms_cur.fetchall()
    for rs in result:
        allLine.append(["",rs[0],"","B/F","","",ccy,rs[1]])

def query_momvement_detail(allLine, balance_left, ms_cur, from_date, to_date, ccy):
    sql = '''
    select * from 
    (
    select 
    tran_id as ref_no,
    convert(varchar(8),value_date,112) as settle_date,
    convert(varchar(8),buss_date,112) as tran_date, 
    'Deposit' as type,
    remark as dscription,
    qty as quantity,
    0 as amount
    from TRANS_CACC_product_in a 
    inner join product b on a.PRODUCT_ID = b.PRODUCT_ID and b.QUOTE_CCY = '{py_ccy}'
    where a.status = 'Confirmed' and client_acc_id = '88888881' and buss_date>='{py_from}' and buss_date<='{py_to}'
    union all 
    select 
    tran_id as ref_no,
    convert(varchar(8),value_date,112) as settle_date,
    convert(varchar(8),buss_date,112) as tran_date, 
    'Withdraw' as type,
    remark as dscription,
    -1*qty as quantity,
    0 as amount
    from TRANS_CACC_product_out a
    inner join product b on a.PRODUCT_ID = b.PRODUCT_ID and b.QUOTE_CCY = '{py_ccy}'
    where a.status = 'Confirmed' and client_acc_id = '88888881' and buss_date>='{py_from}' and buss_date<='{py_to}'
    union all 
    select 
    tran_id as ref_no,
    convert(varchar(8),value_date,112) as settle_date,
    convert(varchar(8),buss_date,112) as tran_date, 
    'Deposit' as type,
    remark as dscription,
    0 as quantity,
    amount as amount
    from TRANS_CACC_cash_in
    where status = 'Confirmed' and client_acc_id = '88888881' and buss_date>='{py_from}' and buss_date<='{py_to}' and ccy = '{py_ccy}'
    union all 
    select 
    tran_id as ref_no,
    convert(varchar(8),value_date,112) as settle_date,
    convert(varchar(8),buss_date,112) as tran_date, 
    'Withdraw' as type,
    remark as dscription,
    0 as quantity,
    -1*amount as amount
    from TRANS_CACC_cash_out
    where status = 'Confirmed' and client_acc_id = '88888881' and buss_date>='{py_from}' and buss_date<='{py_to}' and ccy = '{py_ccy}'
    union all 
    select 
    tran_id as ref_no,
    convert(varchar(8),value_date,112) as settle_date,
    convert(varchar(8),buss_date,112) as tran_date, 
    'Trade' as type,
    CONCAT(case BS_FLAG when 'B' then 'Buy ' when 'S' then 'Sell ' end, a.PRODUCT_ID,' ', isnull(b.name,''),'@' ,cast(avg_price as float)) as dscription,
    case bs_flag when 'B' then 1 else -1 end *qty as quantity,
    case bs_flag when 'B' then -1 else 1 end*net_amt as amount
    from TRANS_CACC_TRADE a 
    left join product b  on a.product_id = b.product_id
    where a.status = 'Confirmed' and client_acc_id = '88888881' and buss_date>='{py_from}' and buss_date<='{py_to}' and QUOTE_CCY = '{py_ccy}'
    ) t order by t.settle_date;
    '''.format(py_from = from_date,py_to = to_date, py_ccy = ccy)
    ms_cur.execute(sql)
    result = ms_cur.fetchall()
    for rs in result:
        balance_left = balance_left + rs[6]
        allLine.append([rs[0],rs[1],rs[2],rs[3],rs[4],rs[5],rs[6],balance_left])
        

def export_account_movement_by_ccy(wbk,file_path,ms_cur, from_date, to_date, ccy,Sheet ):
    #file_name = os.path.join(file_path,"account_movement_%s.csv"%(ccy))
    allLine = []
    allLine.append(["Ref.No","Settle Date","Tran.Date","Type","Descriptioin","Quantity","Amount","Balance(T)"])
    #查询承上余额
    query_begin_balance(allLine, ms_cur, from_date, ccy )
    query_momvement_detail(allLine,allLine[1][7],ms_cur,from_date,to_date,ccy)
    saveToFile(wbk,allLine, file_path, 'w', 'utf8', False, Sheet)
    #saveToCsv(allLine,file_name,'w', 'ansi', False)

def export_account_movement(wbk,file_path,ms_cur, from_date, to_date):
    export_account_movement_by_ccy(wbk,file_path,ms_cur, from_date, to_date, 'CNY','account movement CNY')
    export_account_movement_by_ccy(wbk,file_path,ms_cur, from_date, to_date, 'HKD','account movement HKD')
    export_account_movement_by_ccy(wbk,file_path,ms_cur, from_date, to_date, 'USD','account movement USD')


def WriteToFile(file_path, ms_cur,from_date, to_date):
    # 实例化一个Workbook()对象(即excel文件)
    wbk = xlwt.Workbook()

    bos_PortfolioSummary(wbk,file_path, from_date, to_date)
    bos_Position(wbk,file_path, from_date, to_date)
    bos_DailyTrades(wbk,file_name, from_date, to_date)
    export_account_movement(wbk,file_path,ms_cur, from_date, to_date)
    bos_corpaction(wbk,file_path, from_date, to_date)
    # 以传递的name+当前日期作为excel名称保存。
    wbk.save(file_path)
    print("export success")


def calc_args():
    file_path = None
    command = None
    from_date = None
    to_date = None
    opts, args = getopt.getopt(sys.argv[1:], 'd:c:f:t:')
    for op, value in opts:
        if op == '-d':
            file_path = value
        if op == '-f':
            from_date = value
        if op == '-t':
            to_date = value
    return file_path,from_date,to_date
    if file_path is None or from_date is None or to_date is None:
        raise Exception('need file_path(-d) and from_date(-f) and to_date(-t)')
    return file_path,from_date,to_date

if __name__ == "__main__":
    
    initMssqlConnection()
    ms_cur = MSSQL_CONN.cursor()
    file_path,from_date,to_date = calc_args()
    if from_date == to_date:
        Monthly = from_date[0:8]
    else:
        Monthly = from_date[0:6]
    file_name = os.path.join(file_path,'88888881_monthly_statement_{}.xls'.format(Monthly))
    print(file_name)
    WriteToFile(file_name, ms_cur, from_date, to_date)
