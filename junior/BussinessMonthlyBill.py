#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-11-09 上午10:58
'''
import datetime
import getopt
# !/usr/bin/python
# -*- coding:utf-8 -*-
import os
import sys

import MySQLdb
import xlwt

###############################################################################
MSSQL_HOST = '192.168.5.105'
MSSQL_PORT = 3306
MSSQL_USER = 'root'
MSSQL_PSWD = 'zunjiazichan123'
MSSQL_DB = 'jcbms'
MSSQL_CHARSET = 'utf8'
MSSQL_CONN = None
ISINCODE_A = {}
ISINCODE_HK = {}
ISINCODE_US = {}
ISINCODE = {}


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


def initIsInCode():
    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jy_uts", charset='utf8')
    cursor = con.cursor()
    sql_hk = '''
        SELECT
            CODE as code,
            IFNULL(ISIN,"") AS isin_code
        FROM
            hk_secumain
        GROUP BY
            CODE,
            ISIN
    '''
    cursor.execute(sql_hk)
    retHK = cursor.fetchall()
    for i in retHK: ISINCODE_HK[i[0]] = i[1]

    sql_a = '''
        SELECT
            CODE as code,
            IFNULL(ISIN,"") AS isin_code
        FROM
            secumain
        GROUP BY
            CODE,
            ISIN
    '''
    cursor.execute(sql_a)
    retA = cursor.fetchall()
    for i in retA: ISINCODE_A[i[0]] = i[1]
    sql_us = '''
        SELECT
            a. CODE as code,
            b.isin as isin_code
        FROM
            mv_us_cl a
        LEFT JOIN sym_isin_v1 b ON a.fsym_id_s = b.fsym_id
        ORDER BY
            a. CODE
    '''
    cursor.execute(sql_us)
    retUS = cursor.fetchall()
    for i in retUS:
        ISINCODE_US[i[0]] = i[1]
    global ISINCODE
    ISINCODE = {"CNY": ISINCODE_A, "HKD": ISINCODE_HK, "USD": ISINCODE_US}


def select_Mssql(cursor, sqlStr, isListNotDict=True):
    cursor.execute(sqlStr)
    head = [col[0] for col in cursor.description]
    if isListNotDict:
        results = cursor.fetchall()
        results = (tuple(head),) + results
        # results.insert(0, head)
    else:
        results = [dict(zip(head, data)) for data in cursor.fetchall()]
    return results


def saveToFile(wbk, allLine, filename, mode, encoding, skipFirstLine, Sheet):
    '''
    mode: a(append,附加), w(清空文件内容然后写入)
    encoding = 'utf_8'或'utf8'等.
    skipFirstLine = True
    '''
    assert type(allLine) is tuple
    if 0 < len(allLine):
        assert type(allLine[0]) in (list, tuple)
        if skipFirstLine:
            allLine = allLine[1:]
    dirName = os.path.dirname(filename)
    if dirName and (not os.path.exists(dirName)):
        os.makedirs(dirName)

    # 新建一个名为Sheet1的excel sheet。此处的cell_overwrite_ok =True是为了能对同一个单元格重复操作。
    sheet = wbk.add_sheet(Sheet, cell_overwrite_ok=True)

    for i in range(len(allLine)):
        # 对allLine的每个子元素作遍历，
        for j in range(len(allLine[i])):
            # 将每一行的每个元素按行号i,列号j,写入到excel中。
            tmp = allLine[i][j]
            if isinstance(tmp, datetime.date):
                tmp = str(tmp)
            elif isinstance(tmp,bytes):
                tmp = str(tmp, encoding="utf-8")
            try:
                sheet.write(i, j, tmp)
            except Exception as e:
                print(e)
    wbk.save(file_name)


def bos_DailyTrades(wbk, file_path, from_date, to_date, user):
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
                    account_id = '{user}'
                AND process_date >= '{from_date}'
                AND process_date <= '{to_date}'
                AND STATUS = 'Confirmed'
                ORDER BY
                    trade_id ASC
            ) a
        LEFT JOIN product b ON a.product_id = b.product_id
        AND a.market_id = b.list_market_id
    '''.format(from_date=from_date, to_date=to_date, user = user)

    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    allLineT = ()
    marketCcy = {"SHA": ISINCODE_A, "SZA": ISINCODE_A, "HK": ISINCODE_HK, "US": ISINCODE_US}
    for i, j in enumerate(allLine):
        if i == 0:
            allLineT = allLineT + (j,)
        else:
            codeMarket = j[4].split(":")
            code = codeMarket[0]
            market = codeMarket[1]
            isinCode = marketCcy[market][code]
            allLineT = allLineT + (
            (j[0], j[1], j[2], j[3], j[4], isinCode, j[6], j[7], j[8], j[9], j[10], j[11], j[12]),)
    saveToFile(wbk, allLineT, file_path, 'w', 'utf8', False, 'Transactions')


def bos_PortfolioSummary(wbk, file_path, to_date, user):
    '''Portfolio Summary'''
    sqlStr = '''
        SELECT
            CASE
        WHEN a.currency = 2 THEN
            'HKD'
        WHEN a.currency = 4 THEN
            'USD'
        WHEN a.currency = 1 THEN
            'CNY'
        END AS Ccy,
         settle_balance AS 'Today Balance',
         unsettled_amount AS 'Pendong Settlement',
         balance_avail AS 'Net Balance',
         market_value AS Equity,
         0 AS Bonds,
         0 AS funds,
         0 AS Others,
         market_value AS 'Total Investment',
         b.exchange_rate AS 'RATE',
         (balance_avail + market_value) * b.exchange_rate AS 'Protfolio Value HKD'
        FROM
            (
                SELECT
                    *
                FROM
                    account_balance
                WHERE
                    account_id = '{user}'
                AND process_date = '{to_date}'
            ) a
        LEFT JOIN currency_history b ON a.process_date = b.process_date
        AND a.currency = b.currency;
    '''.format(user=user, to_date=to_date)
    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    saveToFile(wbk, allLine, file_path, 'w', 'utf8', False, 'Portfolio Summary')


def bos_Position(wbk, file_path, to_date, user):
    '''Position'''
    sqlStr = '''
        SELECT
            b.product_id as 'Code',
            b.external_code as 'Isin Code' ,
            CASE
        WHEN a.currency = 2 THEN
            'HKD'
        WHEN a.currency = 4 THEN
            'USD'
        WHEN a.currency = 1 THEN
            'CNY'
        END AS Ccy,
         b.product_description as 'Name',
         net_quantity as 'Today Balance',
         unsettled_quantity as 'Pending Settle',
         nominee_quantity as 'Net Balance',
         closing_price as 'Closing Price',
         market_value as 'Market Value',
         0 as 'Margin',
         0 as 'Marginable Value'
        FROM
            (
                SELECT
                    *
                FROM
                    account_position
                WHERE
                    process_date = '{to_date}'
                AND account_id = '{user}'
                AND net_quantity > 0
            ) a
        LEFT JOIN product b ON a.product_id = b.product_id
        AND a.market_id = b.list_market_id
        ORDER BY
            Ccy,
            a.product_id ASC
    '''.format(to_date=to_date, user=user)

    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    allLineT = ()
    for i, j in enumerate(allLine):
        if i == 0:
            allLineT = allLineT + (j,)
        else:
            ccy = j[2]
            code = j[0]
            isinCode = ISINCODE[ccy][code]
            allLineT = allLineT + ((j[0], isinCode, j[2], j[3], j[4], j[5], j[6], j[7], j[8], j[9], j[10]),)
    saveToFile(wbk, allLineT, file_path, 'a', 'utf8', False, 'Position')


def bos_corpaction(wbk, file_path, from_date, to_date, user):
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
                    posted_date >= '{from_date}'
                AND posted_date <= '{to_date}'
            ) a
        INNER JOIN (
            SELECT
                *
            FROM
                corp_action_detail
            WHERE
                account_id = '{user}'
        ) b ON a.event_id = b.event_id
        LEFT JOIN product c ON a.product_id = c.product_id
        AND a.market_id = c.list_market_id
    '''.format(from_date=from_date, to_date=to_date, user=user)
    # sqlStr = sqlStr.format(from_date, to_date)
    allLine = select_Mssql(MSSQL_CONN.cursor(), sqlStr, True)
    # allLine.insert(0, ["Code", "Name", "Book Close Qty", "Desscription", "Dividend Ccy", "Dividend Amount","Dividend/Bonus Qty","Estimated Payable Date"])
    saveToFile(wbk, allLine, file_path, 'a', 'utf8', False, 'CorpAction')


def query_begin_balance(ms_cur, buss_date, user, ccy):
    sql = '''
        SELECT
        process_date,
        trade_balance
        FROM
            account_balance
        WHERE
            account_id = '{user}'
        AND currency = {ccy}
        AND process_date = (
            SELECT
                process_date
            FROM
                account_balance
            WHERE
                process_date < '{buss_date}'
            AND account_id = '{user}'
            ORDER BY
                process_date DESC
            LIMIT 1)
    '''.format(ccy=ccy, buss_date=buss_date, user=user)
    allLine = (("Ref.No", "Settle Date", "Tran.Date", "Type", "Descriptioin", "Quantity", "Amount", "Balance(T)"),)
    ms_cur.execute(sql)
    result = ms_cur.fetchall()
    for rs in result:
        currency = "CNY"
        if ccy == 2:
            currency = "HKD"
        elif ccy == 4:
            currency = "USD"
        allLine = allLine + (("", buss_date, "", "B/F", "", "", currency, rs[1]),)
    return allLine


def query_momvement_detail(allLine, balance_left, ms_cur, from_date, to_date, user, ccy):
    if ccy == 1:
        market_id_str = "market_id in (4,8)"
    elif ccy == 2:
        market_id_str = "market_id = 1"
    elif ccy == 4:
        market_id_str = "market_id = 2"
    sql = '''   
        SELECT
            RIGHT(trade_id, 8) AS 'Ref.No',
            a.settle_date AS 'Settle Date',
            process_date AS 'Tran.Date',
            'Trade' AS Type,
            CONCAT(
    
                IF (buy_sell = 1, 'Buy', 'Sell'),
                " ",
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
                END,
                " ",
                a.product_description,
                "@",
                avg_price
                )
            ) AS 'Description',
    
        IF (
            buy_sell = 1,
            quantity,
            - quantity
        ) AS 'Quantity',
    
    IF (
        buy_sell = 1 ,- net_amount,
        net_amount
    ) AS 'Amount'
    FROM
        (
            SELECT
                *
            FROM
                account_trade
            WHERE
                process_date >= '{from_date}'
            AND process_date <= '{to_date}'
            AND account_id = '{user}'
            AND {market_id_str}
            AND STATUS = 'Confirmed'
            ORDER BY
                trade_id ASC
        ) a
    LEFT JOIN product b ON a.product_id = b.product_id
    AND a.market_id = b.list_market_id
    UNION ALL
        (
            SELECT
                RIGHT(concat('000000', id), 8) AS 'Ref.No',
                process_date AS 'Settle Date',
                process_date AS 'Tran.Date',
    
            IF (
                purpose = 1,
                'Deposit',
                'Withdraw'
            ) AS Type,
            remark AS Description,
            0 AS Quantity,
    
        IF (purpose = 1, amount ,- amount) AS Amount
        FROM
            cash_flow
        WHERE
            process_date >= '{from_date}'
        AND process_date <= '{to_date}'
        AND account_id = '{user}'
        AND currency = {ccy}
        AND (
            remark NOT LIKE '%自动货币兑换%'
            AND remark NOT LIKE 'in;%'
            AND remark NOT LIKE 'out;%'
            AND remark NOT LIKE 'conversion;%'
        )
        AND STATUS = 'Confirmed'
        ORDER BY
            id ASC
        )
    UNION ALL
        (
            SELECT
                RIGHT(concat('000000', id), 8) AS 'Ref.No',
                settle_date AS 'Settle Date',
                process_date AS 'Tran.Date',
    
            IF (
                purpose = 1,
                'Deposit',
                'Withdraw'
            ) AS Type,
            concat(
                remark,
                " ",
                product_id,
                ":",
                CASE
            WHEN market_id = 1 THEN
                'HK'
            WHEN market_id = 2 THEN
                'US'
            WHEN market_id = 4 THEN
                'SHA'
            WHEN market_id = 8 THEN
                'SZA'
            END,
            "@",
            avg_price
            ) AS Description,
            quantity AS 'Quantity',
    
        IF (
            purpose = 1,
            quantity * avg_price ,- quantity * avg_price
        ) AS Amount
        FROM
            product_flow
        WHERE
            process_date >= '{from_date}'
        AND process_date <= '{to_date}'
        AND account_id = '{user}'
        AND {market_id_str}
        AND (
            remark LIKE '%转入%'
            OR remark LIKE '%转出%'
            OR remark LIKE '%轉入%'
            OR remark LIKE '%轉出%'
        )
        AND STATUS = 'Confirmed'
        ORDER BY
            id ASC
        )
    '''.format(from_date=from_date, to_date=to_date, market_id_str=market_id_str, ccy=ccy, user=user)
    ms_cur.execute(sql)
    result = ms_cur.fetchall()
    for rs in result:
        balance_left = balance_left + rs[6]
        allLine = allLine + ((rs[0], rs[1], rs[2], rs[3], rs[4], rs[5], rs[6], balance_left),)
    return allLine


def export_account_movement_by_ccy(wbk, file_path, ms_cur, from_date, to_date, user, ccy, Sheet):
    # file_name = os.path.join(file_path,"account_movement_%s.csv"%(ccy))

    # 查询承上余额
    allLine = query_begin_balance(ms_cur, from_date, user, ccy)
    allLine = query_momvement_detail(allLine, allLine[1][7], ms_cur, from_date, to_date, user, ccy)
    saveToFile(wbk, allLine, file_path, 'a', 'utf8', False, Sheet)
    # saveToCsv(allLine,file_name,'w', 'ansi', False)


def export_account_movement(wbk, file_path, ms_cur, from_date, to_date, user):
    export_account_movement_by_ccy(wbk, file_path, ms_cur, from_date, to_date, user, 1, 'account movement CNY')
    export_account_movement_by_ccy(wbk, file_path, ms_cur, from_date, to_date, user, 2, 'account movement HKD')
    export_account_movement_by_ccy(wbk, file_path, ms_cur, from_date, to_date, user, 4, 'account movement USD')


def WriteToFile(file_path, ms_cur, from_date, to_date, user):
    # 实例化一个Workbook()对象(即excel文件)
    wbk = xlwt.Workbook()

    bos_PortfolioSummary(wbk, file_path, to_date, user)
    bos_Position(wbk, file_path, to_date, user)
    bos_DailyTrades(wbk, file_name, from_date, to_date, user)
    export_account_movement(wbk, file_path, ms_cur, from_date, to_date, user)
    bos_corpaction(wbk, file_path, from_date, to_date, user)
    # 以传递的name+当前日期作为excel名称保存。
    wbk.save(file_path)
    print("export success")


def calc_args():
    file_path = None
    from_date = None
    to_date = None

    user = None
    print(sys.argv)
    opts, args = getopt.getopt(sys.argv[1:], 'd:f:t:u:')
    for op, value in opts:
        if op == '-d':
            file_path = value
        if op == '-f':
            from_date = value
        if op == '-t':
            to_date = value
        if op == '-u':
            user = value
    if file_path is None or from_date is None or to_date is None:
        raise Exception('need file_path(-d) and from_date(-f) and to_date(-t)')
    return file_path, from_date, to_date, user


if __name__ == "__main__":

    initMssqlConnection()
    initIsInCode()
    ms_cur = MSSQL_CONN.cursor()
    file_path, from_date, to_date, user = calc_args()
    if from_date == to_date:
        Monthly = from_date[0:8]
    else:
        Monthly = from_date[0:6]
    file_name = os.path.join(file_path, '{}_monthly_statement_{}.xls'.format(user, Monthly))
    print(file_name)
    WriteToFile(file_name, ms_cur, from_date, to_date, user)
