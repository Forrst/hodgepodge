#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:eos
创建时间:2018-08-20 下午5:24
'''

import datetime
import account_boc
from db.mysql import SqlUtil
import logging
from log.const import const

logging.config.dictConfig(const.LOGGING)
logger = logging.getLogger('account_wlb.py')

def to_str(i):
    a = i
    if i is None:
        a = ""
    try:
        if isinstance(i, unicode):
            a = i.encode("utf8")
        elif not isinstance(i, str):
            a = str(i)
    except Exception:
        print(i)
    return a


def name_checker(name, nameList):
    for i in nameList:
        if not i.strip().upper() in name.upper():
            return False
    return True


def main():
    mysql1 = SqlUtil.Mysql("mysql5.153")
    mysql2 = SqlUtil.Mysql("mysql2.231")
    deposit_detail = mysql1.execute(
        "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and receive_bank_code = '020' and apply_date >'2018-07-03 00:00:00' order by apply_date desc",
        "miningaccount")
    # log.info("total deposit_detail with status 100010 is: %d",len(deposit_detail))

    deposit_id_found = account_boc.getSet(mysql2.execute("select deposit_id from in_account", "bank"))

    wlb_data = mysql2.execute("\
    SELECT id,dealTime,credit,transactionAbstract from wlb", "bank")
    # log.info("total bank_of_china_hk before 2018-08-11 is: %d",len(bank_of_china_hk))

    wlb = []
    for i in wlb_data:
        id = str(i[0])
        money = i[2].replace(",", "")
        disposeDate = i[1].strip()
        transactionAbstract = i[3] if not i[3] is None else ""
        wlb.append([id, money, disposeDate, transactionAbstract])

    delay_hour = 24
    delay_day = 2
    result = []
    error = []
    # ocr = BaiduOCR()
    for i in deposit_detail:
        if i[0] in deposit_id_found:
            continue
        # if i[0] != 4172l:
        #     continue
        # print(i)
        # path = i[8]
        # ocr_result = ocr.getHttpContent(path)
        apply_date = i[7]
        #left_date = apply_date-datetime.timedelta(hours=delay_hour)
        flag = 0
        for j in wlb:
            # if j[0] != '156':
            #     continue
            if j[1]=="":
                continue
            try:
                if float(i[6])!=float(j[1]):
                    continue
            except Exception,e:
                print e
            handle_date_formated = datetime.datetime.strptime(j[2], "%Y-%m-%d")
            try:
                if float(i[6])==float(j[1]) and apply_date.date() ==handle_date_formated.date():
                    result.append([i[0],i[1],i[2],i[3],i[4],i[5], i[6], i[7], i[8], i[9],j[0],j[1],j[2],j[3]])
                    flag = 1
                    break
            except Exception,e:
                logger.error(e,exc_info=True)
        if flag == 0:
            error.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]])
    counter = 0
    logger.info(str(len(result))+" wlb deposit record matched")
    for j, i in enumerate(result):
        try:
            mysql2.execute("insert into in_account(deposit_id,bank_id,bank_name) values(%s,%s,'wlb')"%(i[0],i[10]),"bank")
            counter+=1
            # line = "\t".join(j) + "\n"
            # f_bank_of_china_hk_detail.write(line)
        except Exception, e:
            logger.error(e,exc_info=True)
    logger.info("Success !"+str(counter)+" wlb deposit record inserted")
    # f_bank_of_china_hk_detail = open("/home/eos/临时文件/account/wlb_normal.csv", "a+")
    # for j, i in enumerate(result):
    #     try:
    #         j = map(to_str, i)
    #         line = "\t".join(j) + "\n"
    #         f_bank_of_china_hk_detail.write(line)
    #     except Exception, e:
    #         print e, e
    # f_bank_of_china_hk_detail.close()
    #
    # f_bank_of_china_hk_detail_error = open("/home/eos/临时文件/account/wlb_error.csv", "a+")
    # for j, i in enumerate(error):
    #     try:
    #         j = map(to_str, i)
    #         line = "\t".join(j) + "\n"
    #         f_bank_of_china_hk_detail_error.write(line)
    #     except Exception, e:
    #         print e, j
    # f_bank_of_china_hk_detail_error.close()


if __name__ == '__main__':

    # transactionDict = {
    #     u"自動櫃員機現金交易": 3,
    #     u"自動轉賬": 1,
    #     u"轉賬交易": 1,
    #     u"自動櫃員機轉賬交易": 1,
    #     u"交換票":2,
    #     u"交換票存入":2,
    #     u"現金交易":3
    # }
    main()

# 问题
# 61887698 2018-08-07 15:48:05审核的但是转账时间是2018/08/09 14:20为什么是先审核再转账的
# 同行转账超时2天
