#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-04-24 上午9:20
'''
import datetime
import MySQLdb
import sys

ACCOUNT_SQL = '''
SELECT
	a.ACCT_NBR AS 账户,
	a.SOC_SEC_NBR AS 客户税ID,
	a.FED_ID_IND AS 客户税收类型,
	-- 'S'-Social Security Number,'T' - Tax ID number
	ifnull(b.EMAIL_ADDRESS, '') AS 电子邮箱,
	a.SHORT_NAME AS 客户姓名
FROM
	phase3.nmaddr a
LEFT JOIN phase3.nmadds b ON a.ACCT_NBR = b.ACCT_NBR
AND a.FIRM_NBR = b.FIRM_NBR
    '''

TRADE_SQL = '''
SELECT
	a.trade_full_date AS trade_date,
	a.SETTLE_FULL_DATE AS settle_full_date,
	a.entry_date AS entry_date,
	a.ACCT_NBR AS acct_nbr
FROM
	phase3.trddtl a
INNER JOIN phase3.secbas b ON a.cusip_number = b.cusip_number
AND b.product = 'ETFS'
WHERE
	a.TRADE_FULL_DATE = '{}'
'''


def headerRecord():
    time = datetime.datetime.now()
    dates = datetime.datetime.strftime(time, "%Y%m%d")
    mins = datetime.datetime.strftime(time, "%H%M%S")
    return "000" + "HEADER" + " " * 13 + dates + mins + " " * 284


def tailRecord(length):
    time = datetime.datetime.now()
    dates = datetime.datetime.strftime(time, "%Y%m%d")
    mins = datetime.datetime.strftime(time, "%H%M%S")
    return "999" + "TRAILER" + " " * 13 + dates + mins + str(length).rjust(10, "0") + " " * 284


def detailRecord(account_number, tax_number, customer_tax_type, customer_email, customer_respectfully_name):
    first_name = ""
    last_name = ""
    time = datetime.datetime.now()
    dates = datetime.datetime.strftime(time, "%Y%m%d")
    mins = datetime.datetime.strftime(time, "%H%M%S")
    return "3VT" + account_number.ljust(19, " ") + tax_number.ljust(9, " ") + customer_tax_type.ljust(1," ") + customer_email.ljust(
        70, " ") + customer_respectfully_name.ljust(10, " ") + first_name.ljust(30, " ") + last_name.ljust(30," ") + " " * 50 + " " * 30 + " " * 5 + " " * 13 + " " * 14 + " " + " " + " " + " " * 4 + "YNYYNNYYNN" + dates + mins + "87x" + " Y"


def get_data_from_db(sql):
    con = MySQLdb.connect(host='10.10.1.57',port = 3309, user='zunjiadu', passwd='zunjiazichan123', db='phase3', charset='utf8')
    cursor = con.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    con.commit()
    con.close()
    return result


def load_account_data():
    ret = {}
    accountTuple = get_data_from_db(ACCOUNT_SQL)
    for i in accountTuple:
        acct_nbr = i[0]
        ret[acct_nbr] = list(i)
    return ret


def get_account_data_by_day(accountData, trade_sql):
    ret = []
    d2 = get_data_from_db(trade_sql)
    for i in d2:
        acct_nbr = i[3]
        ret.append(accountData[acct_nbr])
    return ret


def get_by_date(accountData, date_str):
    #获取指定日期的记录
    acct_day = get_account_data_by_day(accountData, date_str)
    middleRecord = "\n"
    for oneAccount in acct_day:
        try:
            middleRecord += detailRecord(oneAccount[0],oneAccount[1],oneAccount[2],oneAccount[3],oneAccount[4]) + "\n"
        except Exception,e:
            print(e.message)
    if len(acct_day) == 0:
        return ""
    else:
        return headerRecord() + middleRecord + tailRecord(len(acct_day))+"\n"

def get_all_data(accountData,folder_name):
    #获取所有日期的记录
    sql = "select trade_full_date from phase3.trddtl group by trade_full_date order by trade_full_date desc"
    all_date = get_data_from_db(sql)
    ret = ""
    for i in all_date:
        date = str(i[0])
        path = folder_name+date+".dat"
        line = get_by_date(accountData,TRADE_SQL.format(date))
        if line != "":
            save_to_file(path,line)
    return ret

def save_to_file(path,text):
    f = open(path,"a+")
    f.write(text)
    f.flush()
    f.close()


def save_to_file(folder,date,text):
    f = open(folder+date+".dat","a+")
    f.write(text)
    f.flush()
    f.close()

if __name__ == '__main__':
    for i,j in enumerate(sys.argv):
        print "argv{}:".format(i),j
    accountData = load_account_data()
    today = str(datetime.datetime.now().date())
    folder =sys.argv[1]
    print "set folder :{}".format(folder)
    if len(sys.argv) > 2:
        today = sys.argv[2]
        print "set date :{}".format(today)
    else:
        print "set date :{} by default today".format(today)
    day_record = get_by_date(accountData, TRADE_SQL.format(today))
    if day_record != "":
        save_to_file(folder,today,day_record);
    # folder_name = "/home/eos/data/finra/file230/"
    # text = get_all_data(accountData,folder_name)
