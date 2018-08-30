#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-07 下午3:33
'''

'''
尊嘉后台自动对账
'''

import datetime
import account_icbc
import account_wlb
from db.mysql import SqlUtil


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
        if not i.strip() in name.upper():
            return False
    return True

def getSet(sqlret):
    ret = set()
    for i in sqlret:
        ret.add(i[0])
    return ret

def main():
    delay_hour = 3
    delay_day = 2
    result = []
    error = []

    mysql1 = SqlUtil.Mysql("mysql5.153")
    mysql2 = SqlUtil.Mysql("mysql2.231")

    #已经找到的deposit_id
    deposit_id_found = getSet(mysql2.execute("select deposit_id from in_account", "bank"))


    deposit_detail = mysql1.execute(
        "select id,account_name,funds_account,deposit_bank_code,receive_bank_code,deposit_amount,transfer_amount,apply_date,certificate,deposit_type from deposit_detail where audit_status = '300010' and receive_bank_code = '012' order by apply_date desc",
        "miningaccount")

    bank_of_china_hk = mysql2.execute("\
    SELECT id, money, a.transactionAbstract, disposeDate, b.remittanceMoney, b.bankBuyingPrice, b.bankNumber, b.remitterNameAndAddress,b.remittanceBankName ,a.number,a.transactionType\
    FROM bank_of_china_hk a \
    LEFT JOIN remittance_detail b ON a.transactionAbstract = b.transactionAbstract \
    order by disposeDate desc", "bank")
    # log.info("total bank_of_china_hk before 2018-08-11 is: %d",len(bank_of_china_hk))

    bank_of_china_hk_detail = []
    for i in bank_of_china_hk:
        id = str(i[0])
        money = i[1].replace(",", "")
        transactionAbstract = i[2].strip()
        disposeDate = i[3].strip()
        if len(i[3].strip()) == 10:
            disposeDate = i[3].strip() + " 00:00:00"
        if len(i[3].strip()) == 16:
            disposeDate = i[3].strip() + ":00"
        remittanceMoney = i[4].replace(",", "").replace("HKD", "").replace("USD", "").strip() if not i[4] is None else ""
        fee = i[5].replace(",", "").replace("HKD", "").replace("USD", "").strip() if not i[5] is None else ""
        bankNumber = i[6] if not i[6] is None else ""
        nameAddress = i[7] if not i[7] is None else ""
        bankName = i[8] if not i[8] is None else ""
        number = i[9]
        transactiontype = transactionDict[i[10].strip()]
        bank_of_china_hk_detail.append(
            [id, money, transactionAbstract, disposeDate, remittanceMoney, fee, bankNumber, nameAddress, bankName,
             number, transactiontype])


    # ocr = BaiduOCR()
    for i in deposit_detail:
        if i[0] in deposit_id_found:
            continue
        # if i[0] != 4612l:
        #     continue
        # print(i)
        # path = i[8]
        # ocr_result = ocr.getHttpContent(path)
        name = i[1]
        nameFlag = False
        flag = 0
        for j in bank_of_china_hk_detail:
            # if j[0] != '250':
            #     continue
            if i[9] != j[10]:
                continue
            if j[7] != "":
                nameaddr = j[7].encode('utf8').replace("ADD", "\xc2\xa0").replace("1/","")
                names = nameaddr.split("\xc2\xa0\xc2\xa0")[0].split("\xc2\xa0")
                if "MS" in names:
                    names.remove("MS")
                if "MR" in names:
                    names.remove("MR")
                if "MRS" in names:
                    names.remove("MRS")
                if "MRS" in names:
                    names.remove("MISS")
                try:
                    nameFlag = name_checker(name, names)
                except UnicodeDecodeError:
                    print "error! \t后台id" + str(i[0]) + "\t银行id: " + str(j[0])
            if i[3] == i[4] and j[2].strip().lower() != "e-banking transfer":
                continue
            if i[3] != i[4] and j[2].strip().lower() == "e-banking transfer":
                continue

            # 同一个银行转账
            if i[3] == i[4] and j[2].strip().lower() == "e-banking transfer":
                time_apply = i[7]
                left_date = time_apply - datetime.timedelta(hours=delay_hour)
                right_date = time_apply + datetime.timedelta(hours=delay_hour)
                time_bank = datetime.datetime.strptime(j[3], "%Y/%m/%d %H:%M:%S")
                if j[5] != "" and (float(j[4]) - float(j[5]) == float(j[1])):
                    if i[6] != "" and (i[6] is not None) and float(i[6]) == float(
                            j[1]) and left_date < time_bank < right_date and (
                            float(i[5]) - float(i[6] == float(j[5]))):
                        # number = j[9]
                        # if number in ocr_result:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                if j[5] == "":
                    if i[6] != "" and (i[6] is not None) and float(i[6]) == float(
                            j[1]) and left_date < time_bank < right_date:
                        # number = j[9]
                        # if number in ocr_result:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                    elif (i[6] == "" or i[6] is None) and float(i[5]) == float(
                            j[1]) and left_date < time_bank < right_date:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
            # 跨行转账
            if i[3] != i[4] and j[2].strip().lower() != "e-banking transfer":
                time_apply = i[7]
                right_date = time_apply + datetime.timedelta(days=delay_day)
                time_bank = datetime.datetime.strptime(j[3], "%Y/%m/%d %H:%M:%S")
                if j[5] != "" and (float(j[4]) - float(j[5]) == float(j[1])):
                    if i[6] != "" and (i[6] is not None) and float(i[6]) == float(j[1]) and (
                            float(i[5]) - float(i[6]) == float(j[5])) and time_bank < right_date:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                    if (i[6] == "" or i[6] is None) and (float(i[5]) == float(j[4])) and time_bank < right_date:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                elif j[5] == "" and j[1] != "" and time_bank < right_date:
                    if i[6] != "" and i[6] is not None and float(i[6]) == float(j[1]):
                        # number = j[9]
                        # if number in ocr_result:
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                    elif i[6] == "" or i[6] is None and float(i[5]) == float(j[1]):
                        if nameFlag == True:
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9],
                                 j[0], j[1], j[2], j[3], j[4], j[5], j[6], j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
                        elif j[7] != "":
                            continue
                        elif j[7] == "":
                            result.append(
                                [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], j[0], j[1], j[2], j[3],
                                 j[4], j[5], j[6],
                                 j[7], j[8], j[9], j[10]])
                            flag = 1
                            break
        if flag == 0:
            error.append([i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9]])

    # f_bank_of_china_hk_detail = open("/home/eos/临时文件/bank_of_china_hk.csv", "a+")
    for j, i in enumerate(result):
        try:
            mysql2.execute("insert into in_account(deposit_id,bank_id,bank_name) values(%s,%s,'bank_of_china_hk')"%(i[0],i[10]),"bank")
            # line = "\t".join(j) + "\n"
            # f_bank_of_china_hk_detail.write(line)
        except Exception, e:
            print e
    # f_bank_of_china_hk_detail.close()
    #
    # f_bank_of_china_hk_detail_error = open("/home/eos/临时文件/bank_of_china_hk_error.csv", "a+")
    # for j, i in enumerate(error):
    #     try:
    #         j = map(to_str, i)
    #         line = "\t".join(j) + "\n"
    #         f_bank_of_china_hk_detail_error.write(line)
    #     except Exception, e:
    #         print e, j
    # f_bank_of_china_hk_detail_error.close()


if __name__ == '__main__':

    transactionDict = {
        u"自動櫃員機現金交易": 3,
        u"自動轉賬": 1,
        u"轉賬交易": 1,
        u"自動櫃員機轉賬交易": 1,
        u"交換票":2,
        u"交換票存入":2,
        u"現金交易":3
    }
    main()
    account_icbc.main()
    account_wlb.main()
# 问题
# 61887698 2018-08-07 15:48:05审核的但是转账时间是2018/08/09 14:20为什么是先审核再转账的
# 同行转账超时2天
