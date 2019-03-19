#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-20 下午5:24
'''

import datetime
import logging
import threading
import time

import schedule
from xpinyin import Pinyin

import boc_deposite
from log.const import const

logging.config.dictConfig(const.LOGGING)


class Icbc:
    def __init__(self):
        self.logger = logging.getLogger('icbc_deposite.py')
        self.DELAY_HOURS = 6
        self.pre_bank_id = 0
        self.pre_deposit_id = 0
        self.lock = threading.Lock()

    def getOrderedBankBills(self, db, apply, start, end):
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
            apply = apply.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(start, datetime.datetime):
            start = start.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(end, datetime.datetime):
            end = end.strftime('%Y-%m-%d %H:%M:%S')

        start_sql = "SELECT id,trade_date,income_amount,user_name,account from icbc WHERE trade_date>='%s' AND trade_date<='%s' \
                order by trade_date asc" % (apply, end)
        end_sql = "SELECT id,trade_date,income_amount,user_name,account from icbc WHERE trade_date>='%s' AND trade_date<='%s' \
                order by trade_date desc" % (start, apply)
        self.lock.acquire()
        bank_start = db.execute(start_sql, "bank")
        bank_end = db.execute(end_sql, "bank")
        self.lock.release()
        for i in bank_start:
            ret.append(i)
        for i in bank_end:
            ret.append(i)
        return ret

    def getBankBills(self, icbc_data):
        icbc = []
        for i in icbc_data:
            id = str(i[0])
            money = i[2].replace(",", "")
            disposeDate = i[1].strip()
            user_name = i[3] if not i[3] is None else ""
            account = i[4]
            icbc.append([id, money, disposeDate, user_name,account])
        return icbc

    def checkIcbc(self):
        self.lock.acquire()
        deposit_detail = boc_deposite.Boc().mysql5_153.execute(
            "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and receive_bank_code = '072' order by apply_date desc",
            "miningaccount")
        self.lock.release()
        self.logger.info("Total %s ICBC records in deposit_detail"%str(len(deposit_detail)))
        ret = []
        pinyin = Pinyin()
        for i in deposit_detail:
            # if i[0] != 8766:
            #     continue
            result = []
            apply_date = i[7]
            # left_date = i[7] - datetime.timedelta(hours=self.DELAY_HOURS)
            left_date = datetime.datetime(i[7].year,i[7].month,i[7].day,0,0,0)
            right_date = datetime.datetime(i[7].year,i[7].month,i[7].day,23,59,59)
            # right_date = i[7] + datetime.timedelta(hours=self.DELAY_HOURS)
            nameFlag = False
            bank_bills = self.getOrderedBankBills(boc_deposite.Boc().mysql2_231, apply_date, left_date, right_date)
            bank_records = self.getBankBills(bank_bills)
            temp = []
            for j in bank_records:
                # if j[0] != '324':
                #     continue
                if j[1] == "":
                    continue
                try:
                    if float(i[5]) != float(j[1]):
                        continue
                except Exception, e:
                    print e
                number_l = datetime.datetime.strptime(str(j[2]),'%Y-%m-%d %H:%M:%S').strftime("%m%d%H%M")
                account = j[4]
                number = number_l+account[-4:]
                temp.append([i[0], j[0], number, i[7], j[2]])
                if j[3] != "--":
                    try:
                        name = pinyin.get_pinyin(j[3], " ").upper()
                        names = name.split(" ")
                        nameFlag = boc_deposite.Boc().nameChecker(i[1], names)
                        if nameFlag == True:
                            result.append([i[0], j[0], number, i[7], j[2]])
                    except Exception, e:
                        self.logger.error(e, exc_info=True)
                        self.logger.info("工银姓名解析错误! 后台id：" + str(i[0]) + "\t银行id：" + str(j[0]) + "姓名：" + str(j[3]))
                elif j[3].strip() == "--":
                    if float(i[5]) == float(j[1]) and nameFlag == False:
                        result.append([i[0], j[0], number, i[7], j[2]])
                if len(result) != 0:
                    for m in result:
                        ret.append(m)
                    break
            if len(result) == 0:
                if len(temp) > 0:
                    ret.append(temp[0])
                    self.logger.info("ICBC "+str(i[0]) + "\t" + i[1] + " maybe " + temp[0][2])
                else:
                    self.logger.info("ICBC %s in deposit_detail cannot be matched,"%str(i[0]))
        boc_deposite.Boc().saveToInAccount(ret, "icbc")
        self.logger.info("Total %s/%s ICBC records founded in deposit_detail"%(str(len(ret)),str(len(deposit_detail))))

    def jobIcbc(self):
        if self.toStartJob("icbc", "072"):
            self.checkIcbc()
        else:
            time.sleep(1)

    def toStartJob(self, bank_name, recieve_bank_code):
        start_flag = False
        self.lock.acquire()
        latest_bank_record = \
        boc_deposite.Boc().mysql2_231.execute("select id from %s order by id desc limit 1" % bank_name, "bank")[0][0]
        latest_deposit_record = boc_deposite.Boc().mysql5_153.execute(
            "select id from deposit_detail where receive_bank_code = %s order by id desc limit 1" % recieve_bank_code,
            "miningaccount")[0][0]
        self.lock.release()
        if latest_bank_record != self.pre_bank_id or latest_deposit_record != self.pre_deposit_id:
            start_flag = True
        self.pre_bank_id = latest_bank_record
        self.pre_deposit_id = latest_deposit_record
        return start_flag

    def icbcThread(self):
        threading.Thread(target=self.jobIcbc()).start()

    def run(self):
        schedule.every(5).minutes.do(self.icbcThread)

if __name__ == '__main__':
    icbc = Icbc()
    icbc.checkIcbc()