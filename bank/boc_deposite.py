#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-07 下午3:33
'''
import threading

import schedule

'''
尊嘉后台自动对账
'''
import datetime
import json
import logging
import time
import urllib2
import socket
socket.setdefaulttimeout(10.0)
from poster.streaminghttp import register_openers

from db.mysql import SqlUtil
from log.const import const
import icbc_deposite
import wlb_deposite
import  dbs_deposite
import dbs_deposite_syn
import boc_withdraw
import random
from xpinyin import Pinyin
logging.config.dictConfig(const.LOGGING)




class Boc:
    def __init__(self):
        # 任务启动信号,检查上个任务的最后一条是否处理过
        self.preBankId = 0
        self.preDepositId = 0
        self.bankTag = ['REMIT IN', 'E-BANK' ,'ATM', 'FPS IN', 'CHATS', 'CDM', 'CBS TRANSFER', '//', 'CNCB', 'SMALL']
        self.transactionDict = {
            u"自動櫃員機現金交易": 3,
            u"自動櫃員機轉賬交易": 1,
            u"交換票": 2,
            u"交換票存入": 2,
            u"現金交易": 3,
            u"利息": 0,
            u"自動轉賬": 1,
            u"轉賬交易": 1
        }
        self.leftdays = 1
        self.rightDays = 1
        self.lock = threading.Lock()
        self.logger = logging.getLogger('boc_deposite.py')
        self.mysql5_153 = SqlUtil.Mysql("mysql5.153")
        self.mysql2_231 = SqlUtil.Mysql("mysql2.231")
    def toStr(self, integer):
        ret = integer
        if integer is None:
            ret = ""
        try:
            if isinstance(integer, unicode):
                ret = integer.encode("utf8")
            elif not isinstance(integer, str):
                ret = str(integer)
        except Exception:
            print(integer)
        return ret
    @staticmethod
    def nameChecker(name, nameList):
        if len(nameList) == 0:
            return False
        for i in nameList:
            if not i.strip() in name.upper():
                return False
        return True

    def getSet(self, sqlret):
        ret = set()
        for i in sqlret:
            ret.add(i[0])
        return ret

    def isName(self, transactionAbstract):
        if transactionAbstract is None or transactionAbstract.strip() == "":
            return False
        for i in self.bankTag:
            if i in transactionAbstract:
                return False
        if "FPS/" in transactionAbstract and "/FRN" in transactionAbstract:
            return True
        if " " in transactionAbstract:
            return True

    def isHKTradingDay(self, times):
        time_info = times.strftime('%Y%m%d%H%M%S')
        data = {
            "header": {
                "version": 1,
                "imei": "046097B8690EA0D2DDFC76CA05D957C8",
                "key_code": "1B1D9D39F50EE4302D65A3438FD43067",
                "user_type": 1,
                "user_name": "15699885506",
                "auth_code": "345C51A23487E33CC0E72601B855C1F2",
                "system_time": time.time()
            },
            "request_data": {
                "market": "HK",
                "time_info": time_info
            }
        }
        url = "http://app.investassistant.com:80/MiningStock/stock/isStockTime"
        body_value = data
        register_openers()
        body_value = json.JSONEncoder().encode(body_value)
        request = urllib2.Request(url, body_value)
        request.add_header('Content-Type', 'application/json')
        result = self.getUrlContent(request)
        ret = json.loads(result)
        is_stockdate = ret['response_data']['is_stockdate']
        if is_stockdate == 1:
            return True
        elif is_stockdate == 0:
            return False
    def getUrlContent(self,request):
        ret = ""
        try:
            ret = urllib2.urlopen(request).read()
        except Exception,e:
            time.sleep(random.randint(2,8))
            ret = self.getUrlContent(request)
        finally:
            if ret =="":
                return self.getUrlContent(request)
            elif ret !="":
                return ret

    def getWorkDay(self, now, x):
        '''
        :param now:当前的datetime
        :param x: x个工作日
        :return:
        '''
        delta = x / abs(x)
        counter = abs(x)
        endDay = now
        while counter != 0:
            endDay = endDay + datetime.timedelta(days=delta)
            if self.isHKTradingDay(endDay):
                counter -= 1
        return endDay
        # delta = x / abs(x)
        # endDay = now + datetime.timedelta(days=delta+9)
        return endDay


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
            apply = apply.strftime('%Y/%m/%d %H:%M')
        if isinstance(start, datetime.datetime):
            start = start.strftime('%Y/%m/%d %H:%M')
        if isinstance(end, datetime.datetime):
            end = end.strftime('%Y/%m/%d %H:%M')
        self.lock.acquire()
        start_sql = "SELECT id, money, a.transactionAbstract, disposeDate, b.remittanceMoney, b.bankBuyingPrice, b.bankNumber, b.remitterNameAndAddress,b.remittanceBankName ,a.number,a.transactionType \
                FROM bank_of_china_hk a \
                LEFT JOIN remittance_detail b ON a.transactionAbstract = b.transactionAbstract \
                WHERE disposeDate>='%s' AND disposeDate<='%s' \
                AND a.accounts != '扣賬 '\
                order by disposeDate asc" % (apply, end)
        end_sql = "SELECT id, money, a.transactionAbstract, disposeDate, b.remittanceMoney, b.bankBuyingPrice, b.bankNumber, b.remitterNameAndAddress,b.remittanceBankName ,a.number,a.transactionType\
        FROM bank_of_china_hk a \
        LEFT JOIN remittance_detail b ON a.transactionAbstract = b.transactionAbstract \
        WHERE disposeDate>='%s' AND disposeDate<='%s' \
        AND a.accounts != '扣賬 '\
        order by disposeDate desc" % (start, apply)
        bank_start = db.execute(start_sql, "bank")
        bank_end = db.execute(end_sql, "bank")
        self.lock.release()
        for i in bank_start:
            ret.append(i)
        for i in bank_end:
            ret.append(i)
        return ret

    def getBankList(self, bank_bills):
        bank_of_china_hk_detail = []
        error_set = set()
        for i in bank_bills:
            id = str(i[0])
            money = i[1].replace(",", "")
            transactionAbstract = i[2].strip()
            disposeDate = i[3].strip()
            if len(i[3].strip()) == 10:
                disposeDate = i[3].strip() + " 00:00:00"
            if len(i[3].strip()) == 16:
                disposeDate = i[3].strip() + ":00"
            remittanceMoney = i[4].replace(",", "").replace("HKD", "").replace("USD", "").replace("EUR", "").strip() if not i[
                                                                                                             4] is None else ""
            fee = i[5].replace(",", "").replace("HKD", "").replace("USD", "").strip() if not i[5] is None else ""
            bankNumber = i[6] if not i[6] is None else ""
            nameAddress = i[7] if not i[7] is None else ""
            bankName = i[8] if not i[8] is None else ""
            number = i[9]
            transactiontype = self.transactionDict[i[10].strip()] if i[10].strip() in self.transactionDict else ""
            if transactiontype == "" and not i[10] in error_set:
                self.logger.info("error, %s is not in transactionDict" % i[10])
                error_set.add(i[10])
            bank_of_china_hk_detail.append(
                [id, money, transactionAbstract, disposeDate, remittanceMoney, fee, bankNumber, nameAddress, bankName,
                 number, transactiontype])
        return bank_of_china_hk_detail

    def checkBoc(self):

        # 获取后台记录
        self.lock.acquire()
        deposit_detail = self.mysql5_153.execute(
            "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and (receive_bank_code = '012' or receive_bank_code = '1750108') and apply_date>'2018-07-01 00:00:00' order by apply_date desc",
            "miningaccount")

        self.lock.release()
        self.logger.info("Total %s BOC records in deposit_detail"%str(len(deposit_detail)))
        cheque_list = []
        ret = []
        pinyin = Pinyin()
        for i in deposit_detail:
            # if i[0] != 4591:
            #     continue
            deposit_name = pinyin.get_pinyin(i[1],"")
            # 姓名标志
            nameFlag = False
            # 是否找到的标志
            result = []
            # 申请时间，前一个小时和后一个工作日
            time_apply = i[7]
            # if time_apply> datetime.datetime(2018,11,3):
            #     continue
            left_date = time_apply - datetime.timedelta(days=self.leftdays)
            right_date = self.getWorkDay(time_apply, self.rightDays)
            left_day = datetime.datetime(left_date.year, left_date.month, left_date.day,right_date.hour,right_date.minute,right_date.second)
            right_day = datetime.datetime(right_date.year, right_date.month, right_date.day,right_date.hour,right_date.minute,right_date.second)
            bank_bills = self.getOrderedBankBills(self.mysql2_231, time_apply, left_day, right_day)
            bank_records = self.getBankList(bank_bills)
            for j in bank_records:
                # if j[0] != '250':
                #     continue
                # 银行获取到的姓名 空格隔开成两个
                try:
                    bank_name = []

                    # 表示支票
                    if j[10] == 2 and j not in cheque_list:
                        cheque_list.append(j)
                        continue

                    # 金额不一样不看后面的
                    if j[4] != "":
                        rate = float(i[5])/float(j[4])
                        if rate>1.1 or rate<0.99:
                            continue
                    elif j[4] == "":
                        rate = float(i[5])/float(j[1])
                        if rate>1.1 or rate<0.99:
                            continue

                    # 表示交易摘要为CBS TRANSFER 或者交易类别为利息的不处理
                    if 'CBS TRANSFER' in j[2] or j[10] == 0:
                        continue
                    # 表示交易类型不为0 和 -1并且交易类型不匹配除FPS
                    # if j[10] != -1 and i[9] != j[10] and i[9]!=5:
                    #     continue
                    if i[9]==5 and "FPS" not in j[2].upper():
                        continue
                    # 表示与12181002CBS23644 //类似的不处理
                    if "//" in j[2]:
                        continue
                except Exception,e:
                    self.logger.error(e, exc_info=True)
                    self.logger.error(["bank records: ",j])
                # 第一步：检查后台姓名的拼音和银行的拼音是否一致
                if self.isName(j[2]):
                    if "FPS/" in j[2] and "FRN" in j[2]:
                        bank_name = j[2].split("/")[1].split(" ")
                        if "MS" in bank_name:
                            bank_name.remove("MS")
                        if "MR" in bank_name:
                            bank_name.remove("MR")
                        if "MRS" in bank_name:
                            bank_name.remove("MRS")
                        if "MISS" in bank_name:
                            bank_name.remove("MISS")
                    else:
                        bank_name = j[2].split(" ")
                    nameFlag = self.nameChecker(deposit_name, bank_name)
                    if nameFlag == False:
                        continue
                if j[7] != "" and j[7] is not None:
                    nameaddr = j[7].replace(u"ADD", u"\xc2\xa0").replace(u"1/", u"").split(",")[0]
                    if "REMIT" in j[2]:
                        bank_name = nameaddr[:len(deposit_name)].split(" ")
                    if "CHATS" in j[2]:
                        bank_name = nameaddr.split(u"\xc2\xa0\xc2\xa0")[0].split(u"\xc2\xa0")[0].split("  ")[0].split(" ")
                    if "FPS" in j[2]:
                        bank_name = nameaddr.strip().split(" ")
                    if "MS" in bank_name:
                        bank_name.remove("MS")
                    if "MR" in bank_name:
                        bank_name.remove("MR")
                    if "MRS" in bank_name:
                        bank_name.remove("MRS")
                    if "MISS" in bank_name:
                        bank_name.remove("MISS")
                    try:
                        nameFlag = self.nameChecker(deposit_name, bank_name)
                        if nameFlag == False:
                            continue
                    except UnicodeDecodeError:
                        self.logger.info("姓名编码错误! \t后台id" + str(i[0]) + "\t银行id: " + str(j[0]))

                # if i[3] == i[4] and j[2].strip().lower() != "e-banking transfer":
                #     continue
                if i[3] != i[4] and j[2].strip().lower() == "e-banking transfer":
                    continue

                # 处理 e-banking transfer,假设用户输入的入金金额和凭证上一样 写错的 3/254
                if i[3] == i[4] and j[2].strip().lower() == "e-banking transfer":
                    # 银行到账时间time_bank
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],'ebank'])
                # 处理 remit,假设用户输入的入金金额和凭证上一样 写错的  写错的3/104
                elif "REMIT" in j[2] or "CHATS" in j[2]:
                    if nameFlag == True:
                        result.append([i[0], j[0], j[9],nameFlag])
                elif self.isName(j[2]):

                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],nameFlag])
                elif 'FPS' in j[2] and nameFlag==True:
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],True])
                elif 'FPS' in j[2] and nameFlag==False:
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],'fps'])
                elif 'ATM' in j[2]:
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],'atm'])
                elif 'CNCB' in j[2]:
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],'cncb'])
                elif i[3] == i[4] and j[2].strip().lower() == "e-banking transfer":
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],nameFlag])
                else:
                    if float(i[5]) == float(j[1]):
                        result.append([i[0], j[0], j[9],nameFlag])
            if len(result) != 0:
                if len(result)==1:
                    ret.append(result[0])
                if len(result)>=2:
                    tf_flag = False
                    for i in result:
                        if i[3] == True:
                            ret.append(i)
                            tf_flag = True
                            break
                    if tf_flag == False:
                        ret.append(result[0])
            # if len(result)==0:
            #     self.logger.info("BOC %s in deposit_detail cannot be matched, apply_date: %s"%(str(i[0]),str(time_apply)))
        self.saveToInAccount(ret, "bank_of_china_hk")

    def jobBoc(self):
        if self.toStartJob("bank_of_china_hk", "012"):
            self.checkBoc()
        else:
            time.sleep(1)

    def saveToInAccount(self, ret, bank_name):
        self.lock.acquire()
        success = 0
        resubmit = 0
        for j, i in enumerate(ret):
            try:
                exist = self.mysql2_231.execute(
                    "select * from in_account where deposit_id = %s and bank_name = '%s' limit 1" % (i[0], bank_name),"bank")
                if bank_name == "bank_of_china_hk":
                    already_pass = self.mysql5_153.execute(
                        "select * from deposit_detail where transfer_certificate_no like '%{}%' and audit_status = '300010' and (receive_bank_code = '012' or receive_bank_code = '1750108')".format(i[2].replace(u'\xa0',"").strip()),"miningaccount")
                    if len(already_pass)>0:
                        record = already_pass[0]
                        founds_account = record[3]
                        receive_bank_code = record[33].replace(";","")
                        self.logger.info(u"resubmit founds_account:{} receive_bank_code:{}".format(founds_account,receive_bank_code))
                        resubmit+=1
                        continue
                if bank_name == "dbs_real":
                    dbs_exist = self.mysql2_231.execute(
                        "select * from in_account where bank_id = '%s' and `desc` = '%s' and bank_name = '%s'" % (i[1], i[2],bank_name),"bank")
                    if len(dbs_exist)>0:
                        continue
                if len(exist) == 0:
                    self.mysql2_231.execute(
                        "insert into in_account(deposit_id,bank_id,`desc`,bank_name) values(%s,%s,'%s','%s')" % (
                            i[0], i[1], i[2].replace(u'\xa0',"").strip(), bank_name), "bank")
                    self.logger.info(u"{},{} is inserted".format(bank_name,i[0]))
                    success+=1
                # else:
                #     self.mysql2_231.execute(
                #         "update in_account set bank_id = %s,`desc` = '%s' where deposit_id = %s and bank_name = '%s'" %(i[1], i[2].replace(u'\xa0',"").strip(), i[0], bank_name), "bank")
                #     self.logger.info(u"{},{} is updated".format(bank_name,i[0]))
            except Exception, e:
                self.logger.error("bank id:{}".format(i[1]))
                self.logger.error(e, exc_info=True)
        self.logger.info("Total {} BOC records founded in deposit_detail resubmit:{} success:{} existed:{}".format(len(ret),resubmit,success,len(ret)-resubmit-success))
        self.lock.release()

    def toStartJob(self, bank_name, recieve_bank_code):
        self.lock.acquire()
        start_flag = False
        latest_bank_record = self.mysql2_231.execute("select id from %s order by id desc limit 1" % bank_name, "bank")[0][0]
        latest_deposit_record = self.mysql5_153.execute(
            "select id from deposit_detail where receive_bank_code = %s order by id desc limit 1" % recieve_bank_code,
            "miningaccount")[0][0]
        if latest_bank_record != self.preBankId or latest_deposit_record != self.preDepositId:
            start_flag = True
        self.preBankId = latest_bank_record
        self.preDepositId = latest_deposit_record
        self.lock.release()
        return start_flag

    def bocThread(self):
        threading.Thread(target=self.jobBoc()).start()

    def run(self):
        schedule.every(5).minutes.do(self.bocThread)


if __name__ == '__main__':
    logging.info("begin to check account")
    Boc().run()
    icbc_deposite.Icbc().run()
    wlb_deposite.Wlb().run()
    dbs_deposite.Dbs().run()
    dbs_deposite_syn.synDbs().run()
    boc_withdraw.BocWithdraw().run()
    while True:
        schedule.run_pending()
        time.sleep(1)
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
