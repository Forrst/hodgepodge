#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-11-16 上午10:25
'''
# !/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:eos
创建时间:2018-08-07 下午3:33
'''
import threading

import schedule
from xpinyin import Pinyin
import boc_deposite

'''
尊嘉后台自动对账
'''
import logging
import time
import socket

socket.setdefaulttimeout(10.0)

from db.mysql import SqlUtil
from log.const import const

logging.config.dictConfig(const.LOGGING)


class BocWithdraw:
    def __init__(self):
        # 任务启动信号,检查上个任务的最后一条是否处理过
        self.preBankId = 0
        self.preDepositId = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger('boc_withdraw.py')
        self.mysql5_153 = SqlUtil.Mysql("mysql5.153")
        self.mysql2_231 = SqlUtil.Mysql("mysql2.231")

    def getBankListByCardNo(self, bank_card_num, funds_account, insert_date):
        try:
            sql = "select * from transfer_boc_detail where accountNumber = %s and toPayeeMessage='%s' and dealDateAndTime>='%s' order by dealDateAndTime asc" % (
            bank_card_num, funds_account, insert_date)
            ret = self.mysql2_231.execute(sql, "bank")
            if len(ret) == 0:
                sql = "select * from transfer_boc_detail where accountNumber = %s and toPayeeMessage='%s' and dealDateAndTime>='%s' order by dealDateAndTime asc" % (
                bank_card_num[3:], funds_account, insert_date)
                ret = self.mysql2_231.execute(sql, "bank")
        except Exception, e:
            self.logger.error(e, exc_info=True)
        return ret

    def getBocHKBankList(self, bank_card_num, insert_date):
        try:
            sql = "select * from transfer_boc_detail where accountNumber = %s and dealDateAndTime>='%s' order by dealDateAndTime asc" % (
                bank_card_num, insert_date)
            ret = self.mysql2_231.execute(sql, "bank")
            if len(ret) == 0:
                sql = "select * from transfer_boc_detail where accountNumber = %s and dealDateAndTime>='%s' order by dealDateAndTime asc" % (
                    bank_card_num[3:], insert_date)
                ret = self.mysql2_231.execute(sql, "bank")
        except Exception, e:
            self.logger.error(e, exc_info=True)
        return ret



    def getBankListByBankType(self, bank_code, funds_account, insert_date):
        try:
            sql = "select * from transfer_boc_detail where bankNo = %s and toPayeeMessage='%s' and dealDateAndTime>='%s' order by dealDateAndTime asc" % (
            bank_code, funds_account, insert_date)
            ret = self.mysql2_231.execute(sql, "bank")
        except Exception, e:
            self.logger.error(e, exc_info=True)
        return ret

    def checkBocWithdraw(self):

        # 获取后台记录
        self.lock.acquire()
        transfer_money_apply = self.mysql5_153.execute(
            "select id,bank_name,currency,payee_name,bank_card_num,amount,amount_real,funds_account,insert_date,bank_card_num from transfer_money_apply where status = '2001' and insert_date>'2018-10-22 13:33:00' order by insert_date desc",
            "miningaccount")

        self.lock.release()
        self.logger.info("Total %s BOC records in transfer money" % str(len(transfer_money_apply)))

        ret = []
        finded_set = set()
        pinyin = Pinyin()
        for i in transfer_money_apply:
            # if i[0] != 2006:
            #     continue
            try:
                bank_code = i[1].split(" ")[0]
                amount = i[5]
                name = i[3]
                funds_account = "JCS-%s REFUND" % i[7]
                insert_date = i[8].strftime("%Y/%m/%d %H:%M:%S")
                if i[9] is None:
                    continue
                bank_card_num = i[9].replace("-", "").replace(" ", "")
                # if i[7] == '60822758':
                #     self.logger.info(i[7])

                if bank_code == u"中银香港" or bank_card_num[:3] == "012":
                    sql_ret = self.getBocHKBankList(bank_card_num,insert_date)
                elif str.isdigit(bank_code.encode("utf8")):
                    sql_ret = self.getBankListByBankType(bank_code, funds_account, insert_date)
                else:
                    sql_ret = self.getBankListByCardNo(bank_card_num, funds_account, insert_date)
                flag = False
                for j in sql_ret:
                    debitAsset = j[6].replace(",", "")
                    if float(debitAsset) == float(amount) :
                        if bank_card_num == "中银香港" or bank_card_num[:3] =="012":
                            transfer_name = pinyin.get_pinyin(name," ")
                            bank_name = pinyin.get_pinyin(j[16]).split(" ")
                            if boc_deposite.Boc.nameChecker(transfer_name, bank_name):
                                ret.append([i[0], j[0], "withdraw"])
                                flag = True
                            else:
                                self.logger.info("name not same transfer name {} bank name {},transfer id:{} bank id:{}".format(transfer_name,j[9],i[0],j[0]))
                        else:
                            ret.append([i[0], j[0], "withdraw"])
                            flag = True
                        # if j[0] == "B318040781":
                        #     self.logger.info(j[0])
                        if j[0] not in finded_set:
                            finded_set.add(j[0])
                        else:
                            self.logger.info("%s is used bankid: %s" % (str(i[0]), str(j[0])))
                        break
                if flag == False:
                    self.logger.info("can't find %s" % str(i[0]))
            except Exception,e:
                self.logger.error(e,exc_info=True)
        self.saveToInAccount(ret, "transfer_boc_detail")
        self.logger.info("Total %s/%s BOC records withdraw  founded" % (str(len(ret)), str(len(transfer_money_apply))))

    def jobBocWithdraw(self):
        if self.toStartJob("transfer_boc_detail"):
            self.checkBocWithdraw()
        else:
            time.sleep(1)

    def saveToInAccount(self, ret, bank_name):
        self.lock.acquire()
        counter = 0
        for j, i in enumerate(ret):
            try:
                exist = self.mysql2_231.execute(
                    "select * from in_account where deposit_id = %s and bank_name = '%s' limit 1" % (i[0], bank_name),
                    "bank")
                if len(exist) == 0:
                    self.mysql2_231.execute(
                        "insert into in_account(deposit_id,bank_id,`desc`,bank_name) values('%s','%s','%s','%s')" % (
                            i[0], i[1], i[2].strip(), bank_name), "bank")
                    counter += 1
                else:
                    self.mysql2_231.execute(
                        "update in_account set bank_id = %s,`desc` = '%s' where deposit_id = %s and bank_name = '%s'" % (
                        i[1], i[2].strip(), i[0], bank_name), "bank")
            except Exception, e:
                self.logger.error(e, exc_info=True)
        self.lock.release()

    def toStartJob(self, bank_name):
        self.lock.acquire()
        start_flag = False
        latest_bank_record = \
        self.mysql2_231.execute("select transactionID from %s order by dealDateAndTime desc limit 1" % bank_name, "bank")[0][0]
        latest_deposit_record = self.mysql5_153.execute(
            "select id from transfer_money_apply order by id desc limit 1",
            "miningaccount")[0][0]
        if latest_bank_record != self.preBankId or latest_deposit_record != self.preDepositId:
            start_flag = True
        self.preBankId = latest_bank_record
        self.preDepositId = latest_deposit_record
        self.lock.release()
        return start_flag

    def bocWithdrawThread(self):
        threading.Thread(target=self.jobBocWithdraw()).start()

    def run(self):
        schedule.every(5).minutes.do(self.bocWithdrawThread)


if __name__ == '__main__':
    BocWithdraw().checkBocWithdraw()
# 支票不处理，特别标注，FPS IN/FRN20181030
# CBS TRANSFER不处理
# ATM DEP 存款
# ATM TRF atm转账
# CDM DEP 支票 标注一下
# CNCB INTL PHB/IB AC 香港中信银行跨行转帐
# REMIT IN 表示国内转过去的
# CHATS表示 香港转过去的
# SMALL VALUE TRANSFER
# 扣账 不管
# 浮动额 支票的浮动额不为0

# 问题
# 61887698 2018-08-07 15:48:05审核的但是转账时间是2018/08/09 14:20为什么是先审核再转账的
# 同行转账超时2天

# 抽查3个匹配成功的
# 8732，8743,8733,8726,8723,8712,8721,8719,8689,其中8727,的由于未抓取到失败
