#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-20 下午5:24
'''

import datetime
from xpinyin import Pinyin
from db.mysql import SqlUtil
import account_boc

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
        "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '100010' and receive_bank_code = '072' and apply_date >'2018-07-08 00:00:00' order by apply_date desc",
        "miningaccount")
    # log.info("total deposit_detail with status 100010 is: %d",len(deposit_detail))

    deposit_id_found = account_boc.getSet(mysql2.execute("select deposit_id from in_account", "bank"))

    icbc_data = mysql2.execute("\
    SELECT id,trade_date,income_amount,user_name from icbc", "bank")
    # log.info("total bank_of_china_hk before 2018-08-11 is: %d",len(bank_of_china_hk))

    icbc = []
    for i in icbc_data:
        id = str(i[0])
        money = i[2].replace(",", "")
        disposeDate = i[1].strip()
        user_name = i[3] if not i[3] is None else ""
        icbc.append([id, money, disposeDate, user_name])

    delay_hour = 3
    delay_day = 2
    result = []
    error = []
    # ocr = BaiduOCR()
    pinyin = Pinyin()
    for i in deposit_detail:
        if i[0] in deposit_id_found:
            continue
        # if i[0] != 6431l:
        #     continue
        # print(i)
        # path = i[8]
        # ocr_result = ocr.getHttpContent(path)
        apply_date = i[7]
        left_date = i[7]-datetime.timedelta(hours=delay_hour)
        right_date = i[7]+datetime.timedelta(hours=delay_hour)
        nameFlag = False
        flag = 0
        for j in icbc:
            # if j[0] != '324':
            #     continue
            if j[1]=="":
                continue
            try:
                if float(i[6])!=float(j[1]):
                    continue
            except Exception,e:
                print e
            handle_date_formated = datetime.datetime.strptime(j[2], "%Y-%m-%d %H:%M:%S")
            if j[3] != "--":
                try:
                    name = pinyin.get_pinyin(j[3]," ")
                    names = name.split(" ")
                    nameFlag = name_checker(i[1], names)
                except Exception:
                    print "user_name error! \t后台id" + str(i[0]) + "\t银行id: " + str(j[0])
                if float(i[6])==float(j[1]) and nameFlag == True and left_date<handle_date_formated<right_date:
                    result.append([i[0],i[1],i[2],i[3],i[4],i[5], i[6], i[7], i[8], i[9],j[0],j[1],j[2],j[3]])
                    flag = 1
                    break
            if j[3].strip() == "--":
                if float(i[6])==float(j[1]) and nameFlag == False and left_date<handle_date_formated<right_date:
                    result.append([i[0],i[1],i[2],i[3],i[4],i[5], i[6], i[7], i[8], i[9],j[0],j[1],j[2],j[3]])
                    flag = 1
                    break
        if flag == 0:
            error.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]])

    for j, i in enumerate(result):
        try:
            mysql2.execute("insert into in_account(deposit_id,bank_id,bank_name) values(%s,%s,%s)"%(j[0],j[10],"icbc"),"bank")
            # line = "\t".join(j) + "\n"
            # f_bank_of_china_hk_detail.write(line)
        except Exception, e:
            print e
    # f_bank_of_china_hk_detail = open("/home/eos/临时文件/account/icbc_normal.csv", "a+")
    # for j, i in enumerate(result):
    #     try:
    #         j = map(to_str, i)
    #         line = "\t".join(j) + "\n"
    #         f_bank_of_china_hk_detail.write(line)
    #     except Exception, e:
    #         print e, e
    # f_bank_of_china_hk_detail.close()
    #
    # f_bank_of_china_hk_detail_error = open("/home/eos/临时文件/account/icbc_error.csv", "a+")
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
