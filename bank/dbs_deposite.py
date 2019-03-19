#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-11-06 上午10:57
'''
import datetime
import logging
import threading
import time

import schedule

import boc_deposite
from log.const import const

#os.chdir("/home/eos/git/auto_account")

logging.config.dictConfig(const.LOGGING)


class Dbs:
    def __init__(self):
        self.pre_deposit_id = 0
        self.pre_dbs_real_id = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__file__)

    def getOrderedBankBills(self, db, apply):
        '''
        获取从apply时间往后到end时间的账单，再获取从apply到start
        :param db:
        :param apply:
        :param start:
        :param end:
        :return:
        '''
        ret = []
        if isinstance(apply, datetime.datetime):
            apply = apply.strftime('%Y-%m-%d')
        sql = "SELECT id,transaction_date,account_no,real_money_recieved,name_address,transaction_no from dbs_real WHERE transaction_date >= '%s'order by transaction_date asc" % apply
        bank_end = db.execute(sql, "bank")
        for i in bank_end:
            ret.append(i)
        return ret

    def genDBSList(self, banksBills):
        dbs = []
        for i in banksBills:
            id = str(i[0])
            transacntion_date = i[1]
            account_no = i[2]
            real_money_revieved = i[3]
            name_address = i[4]
            transaction_no = i[5]
            dbs.append([id, transacntion_date, account_no, real_money_revieved, name_address, transaction_no])
        return dbs

    def checkDbs(self):
        self.lock.acquire()
        deposit_detail = boc_deposite.Boc().mysql5_153.execute(
            "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and receive_bank_code = '016' order by apply_date desc",
            "miningaccount")

        self.lock.release()
        self.logger.info("Total %s DBS records in deposit_detail" % str(len(deposit_detail)))
        ret = []
        for i in deposit_detail:
            # if i[0] in deposit_id_found:
            #     continue
            # if i[0] != 8261l:
            #     continue
            user_name = i[1]
            funds_account = "79319" + str(i[2])
            apply_date = i[7]
            apply_date = datetime.datetime(apply_date.year, apply_date.month, apply_date.day)
            result = []
            banksBills = self.getOrderedBankBills(boc_deposite.Boc().mysql2_231, apply_date)
            wlb = self.genDBSList(banksBills)
            for j in wlb:
                # if j[0] != '156':
                #     continue
                transaction_no = "0" + j[5][-7:]
                try:
                    if funds_account == j[2]:
                        if user_name.upper() in j[4].replace(" ", ""):
                            x = float(i[5])-float(j[3])
                            y = float(i[5])/float(j[3])
                            if 0<=x <=500 or (1<=y<=1.2):
                                result.append([i[0], j[0], transaction_no])
                except Exception, e:
                    self.logger.error(e, exc_info=True)
            if len(result) > 0:
                ret.append(result[0])
            if len(result) == 0:
                pass
                # self.logger.info("DBS %s in deposit_detail cannot be matched," % str(i[0]))
        boc_deposite.Boc().saveToInAccount(ret, "dbs_real")
        self.logger.info(
            "Total %s/%s DBS records founded in deposit_detail" % (str(len(ret)), str(len(deposit_detail))))

    def jobDbs(self):
        if self.toStartJob("dbs", "016"):
            self.checkDbs()
        else:
            time.sleep(1)

    def toStartJob(self, bank_name, recieve_bank_code):
        self.lock.acquire()
        start_flag = False
        latest_bank_record = \
        boc_deposite.Boc().mysql2_231.execute("select id from %s order by id desc limit 1" % bank_name, "bank")[0][0]
        latest_deposit_record = boc_deposite.Boc().mysql5_153.execute(
            "select id from deposit_detail where receive_bank_code = %s order by id desc limit 1" % recieve_bank_code,
            "miningaccount")[0][0]
        latest_dbs_real_record = boc_deposite.Boc().mysql2_231.execute("select id from dbs_real order by id desc limit 1", "bank")[0][0]
        if latest_deposit_record != self.pre_deposit_id or latest_dbs_real_record != self.pre_dbs_real_id:
            start_flag = True
        self.pre_deposit_id = latest_deposit_record
        self.pre_dbs_real_id = latest_dbs_real_record
        self.lock.release()
        return start_flag

    def dbsThread(self):
        threading.Thread(target=self.jobDbs()).start()

    def run(self):
        schedule.every(5).minutes.do(self.dbsThread)

if __name__ == '__main__':
    dbs_job = Dbs()
    dbs_job.checkDbs()
