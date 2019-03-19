#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-11-12 下午2:18
'''
import logging
import threading
import time
import datetime
import MySQLdb
import pandas as pd
import schedule

import boc_deposite
from log.const import const

#os.chdir("/home/eos/git/auto_account")



logging.config.dictConfig(const.LOGGING)

class synDbs:
    def __init__(self):
        self.taskFlag = False
        self.dbs_date_end = ""
        self.dbs_date_start = ""
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__file__)
    def jobsynDbs(self):
        if self.toStartJob():
            self.synDbs()
        else:
            self.logger.info("DBS SYN: No dbs update in mysql")
            time.sleep(1)

    def toStartJob(self):
        self.lock.acquire()
        start_flag = False
        dbs_date = boc_deposite.Boc().mysql2_231.execute("select update_time from dbs order by update_time desc limit 1", "bank")
        dbs_real_date = boc_deposite.Boc().mysql2_231.execute("select update_time from dbs_real order by update_time desc limit 1",
                                                             "bank")
        self.dbs_date_end = dbs_date[0][0]
        self.dbs_date_start = self.dbs_date_end-datetime.timedelta(minutes=10)
        dbs_real_date_str = dbs_real_date[0][0]
        if dbs_real_date_str < self.dbs_date_end:
            start_flag = True
        self.lock.release()
        return start_flag

    def synDbsThread(self):
        threading.Thread(target=self.jobsynDbs()).start()

    def run(self):
        schedule.every(5).minutes.do(self.synDbsThread)


    def synDbs(self):
        con = MySQLdb.connect(host="192.168.5.106", user="root", passwd="zunjiazichan123", db="bank", charset="utf8")
        sql_fetch = "select * from dbs where update_time >= '{}' and update_time<='{}'".format (self.dbs_date_start,self.dbs_date_end)
        data = pd.read_sql(sql_fetch, con)

        cr = []
        chgs = []
        cr_index = {}
        cr_date = {}
        chgs_index = {}
        for i, j in enumerate(data['trade_desc']):
            index = data['id'][i]
            column = j.split(" ")
            type = column[0]
            if type == 'CR':
                cr.append(column)
                cr_last_index = len(cr) - 1
                cr_index[cr_last_index] = index
                cr_date[index] = data['trade_date'][i]
            elif type == 'CHGS':
                chgs.append(column)
                chgs_last_index = len(chgs) - 1
                chgs_index[chgs_last_index] = index

        cr_pure = []
        for i in cr:
            type = i[0]
            transactionNo = i[1]
            customerNo = i[2]
            accountNo = i[3]
            fx = i[4]
            customerNo_1 = i[5]
            name_address = " ".join(i[6:-3])
            if len(i[5]) == 1:
                fx = fx + " " + i[5]
                customerNo_1 = i[6]
                name_address = " ".join(i[7:-3])
            accountNo_1 = i[-1]
            money = i[-2]
            rate = i[-3]
            cr_pure.append(
                [type, transactionNo, customerNo, accountNo, fx, customerNo_1, name_address, rate, money, accountNo_1])

        chgs_pure = []
        chgs_set = {}
        for j, i in enumerate(chgs):
            type = i[0]
            transactionNo = i[1]
            accountNo = i[2]
            customerNo = i[3]
            nameAbb = i[4]
            transactionNo_1 = i[5]
            name_address = " ".join(i[6:-3])
            accountNo_1 = i[-1]
            money = i[-2]
            rate = i[-3]
            chgs_pure.append(
                [type, transactionNo, accountNo, customerNo, nameAbb, transactionNo_1, name_address, rate, money,
                 accountNo_1])
            key = transactionNo + "_" + accountNo + "_" + customerNo + "_" + str(money)
            chgs_set[key] = j

        ret = []
        for o, i in enumerate(cr_pure):
            key = i[1] + "_" + i[3] + "_" + i[2] + "_" + i[8]
            sql_cr_index = cr_index[o]
            trade_date = cr_date[sql_cr_index]
            if key in chgs_set:
                k = chgs_set[key]
                sql_chgs_index = chgs_index[k]
                deposit = float(data[data['id'] == sql_cr_index]['deposit'])
                spendinig = float(data[data['id'] == sql_chgs_index]['spending'])
                ret.append([trade_date, sql_cr_index, sql_chgs_index, i[1], i[2], i[3], i[6], deposit, spendinig,
                            float(deposit) - float(spendinig)])
            else:
                try:
                    ret.append([trade_date, sql_cr_index, "", i[1], i[2], i[3], i[6], float(i[8]), 0, float(i[8])])
                except Exception,e:
                    self.logger.error(e, exc_info=True)
                    continue

        column = ["transaction_date", "cr_id", "chgs_id", "transaction_no", "customer_no", "account_no", "name_address",
                  "transfer_money", "fee", "real_money_recieved"]
        sql = "insert into dbs_real"
        boc_deposite.Boc().mysql2_231.executeMany(sql, columns=column, data=ret, db="bank")
        self.logger.info("DBS SYN: Total %s dbs records analyzed"%str(len(ret)))

if __name__ == "__main__":
    syn = synDbs()
    syn.jobsynDbs()