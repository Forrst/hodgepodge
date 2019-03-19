#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:eos
创建时间:2018-08-20 下午5:24
'''

import datetime
import logging
import re
import threading
import time

import schedule

import boc_deposite
from baiduaip import BaiduOCR
from log.const import const

logging.config.dictConfig(const.LOGGING)


class Wlb:
    def __init__(self):
        self.logger = logging.getLogger('wlb_deposite.py')
        self.EBRegex = re.compile("EB.*?(?=\n)")
        self.AccountRegex = re.compile("[\d|\\*]{8,}")
        self.DELAY_HOURS = 3
        self.ocr = BaiduOCR()
        self.pre_bank_id = 0
        self.pre_deposit_id = 0
        self.lock = threading.Lock()

    def genWlbList(self, wlb_data):
        wlb = []
        for i in wlb_data:
            id = str(i[0])
            money = i[2].replace(",", "")
            disposeDate = i[1].strip()
            transactionAbstract = i[3] if not i[3] is None else ""
            wlb.append([id, money, disposeDate, transactionAbstract])
        return wlb

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
        sql = "SELECT id,dealTime,credit,transactionAbstract from wlb WHERE dealTime = '%s'order by dealTime desc" % apply
        bank_end = db.execute(sql, "bank")
        for i in bank_end:
            if i[2].strip() == "" or i[2] is None:
                continue
            ret.append(i)
        return ret

    def getOcrResult(self, db, id, path):
        path_handled = db.execute("select path from wlb_ocr where deposit_id = %s" % id, "bank")
        if len(path_handled) != 0:
            path_handled = path_handled[0][0]
        ret = ""
        ocr_ret = []
        if len(path_handled) == 0:
            paths = path.split(";")
            for i in paths:
                ocr_ret.append(self.ocr.getHttpContent(i))
            ret = ";".join(ocr_ret).replace("\'s","").replace("?.I::\n+952352113\n\n","")
            if len(ret)>3000:
                ret = ret[:3000]
            sql = "insert into wlb_ocr(deposit_id,path,ocr_text) values('%s','%s','%s')" % (id, path, ret)
            try:
                db.execute(sql, 'bank')
            except Exception,e:
                self.logger.error(e, exc_info=True)
                self.logger.info([sql,id,path,ret])
        elif len(path_handled) > 0 and path_handled != path and path_handled in path:
            ret_existed = db.execute("select ocr_text from wlb_ocr where ocr_id = %s" % id)[0][0]
            ocr_ret.append(ret_existed)
            paths = path.split(";")
            paths_existed = path_handled.split(";")
            for i in paths:
                if not i in paths_existed:
                    paths_existed.append(i)
                    ocr_ret.append(self.ocr.getHttpContent(i))
            ret = ";".join(ocr_ret)
            path_new = ";".join(paths_existed)
            sql = "update wlb_ocr set ocr_text = '%s',path = %s where deposit_id = %s)" % (ret, path_new, id)
            db.execute(sql, "bank")
        elif len(path_handled) > 0 and path_handled == path:
            ret = db.execute("select ocr_text from wlb_ocr where deposit_id = %s" % id, "bank")[0][0]
        return ret

    def getEBCode(self, text):
        ret = ""
        eb_finded = self.EBRegex.findall(text)
        for i in eb_finded:
            if len(i) == 8:
                ret = i
                break
        return ret

    def getAccountCode(self, text):
        ret = ""
        text = text.replace("-","").replace("\n","")
        eb_finded = self.AccountRegex.findall(text)
        if len(eb_finded) == 1:
            ret = eb_finded[0]
        for i in eb_finded:
            if (len(i) == 11 or len(i) == 10) and i[-4:] != u'7726':
                ret = i
                break
        return ret.replace("*","")

    def checkWlb(self):
        self.lock.acquire()
        deposit_detail = boc_deposite.Boc().mysql5_153.execute(
            "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and receive_bank_code = '020' and apply_date >'2018-07-03 00:00:00' order by apply_date desc",
            "miningaccount")
        self.lock.release()
        self.logger.info("Total %s WLB records in deposit_detail"%str(len(deposit_detail)))
        ret = []
        for i in deposit_detail:
            # if i[0] in deposit_id_found:
            #     continue
            # if i[0] != 6845l:
            #     continue
            # print(i)
            path = i[8]
            ocr_result = self.getOcrResult(boc_deposite.Boc().mysql2_231, i[0], path)
            eb_code = self.getEBCode(ocr_result)
            account_code = self.getAccountCode(ocr_result)

            apply_date = i[7]
            apply_date = datetime.datetime(apply_date.year, apply_date.month, apply_date.day)
            result = []
            banksBills = self.getOrderedBankBills(boc_deposite.Boc().mysql2_231, apply_date)
            wlb = self.genWlbList(banksBills)
            for j in wlb:
                try:
                    if float(i[5]) == float(j[1]):
                        bankAccount = self.getAccountCode(j[3])
                        if len(account_code)==11 and account_code == bankAccount:
                            result.append([i[0], j[0], eb_code])
                            break
                        elif len(account_code)==10 and account_code == bankAccount[-10:]:
                            result.append([i[0], j[0], eb_code])
                            break
                        elif len(account_code)==4 and account_code == bankAccount[-4:]:
                            result.append([i[0], j[0], eb_code])
                            break
                        elif account_code !="" and account_code[-4:] == bankAccount[-4:]:
                            result.append([i[0], j[0], eb_code])
                            break
                        elif account_code == "":
                            result.append([i[0], j[0], eb_code])
                except Exception, e:
                    self.logger.info("i[5]:"+str(i[5])+" j[1]:"+str(j[1]))
                    self.logger.error(e, exc_info=True)
            if len(result) > 0:
                ret.append(result[0])
            if len(result) == 0:
                self.logger.info("WLB %s in deposit_detail cannot be matched,"%str(i[0]))
        boc_deposite.Boc().saveToInAccount(ret, "wlb")
        self.logger.info("Total %s/%s WLB records founded in deposit_detail"%(str(len(ret)),str(len(deposit_detail))))
    def jobWlb(self):
        if self.toStartJob("wlb", "020"):
            self.checkWlb()
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

    def wlbThread(self):
        threading.Thread(target=self.jobWlb()).start()

    def run(self):
        schedule.every(5).minutes.do(self.wlbThread)


if __name__ == '__main__':
    wlb = Wlb()
    wlb.checkWlb()
