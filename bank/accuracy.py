#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-11-14 下午5:03
'''
#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2018-10-31 下午2:27
'''
from db.mysql.SqlUtil import Mysql
import datetime
import pandas as pd
import json
import urllib2
from poster.streaminghttp import register_openers
import time
def isHKTrading_day(times):
    time_info = times.strftime('%Y%m%d%H%M%S')
    data = {
        "header" : {
            "version" : 1,
            "imei" : "046097B8690EA0D2DDFC76CA05D957C8",
            "key_code" : "1B1D9D39F50EE4302D65A3438FD43067",
            "user_type" : 1,
            "user_name" : "15699885506",
            "auth_code" : "345C51A23487E33CC0E72601B855C1F2",
            "system_time" : time.time()
        },
        "request_data" : {
            "market" : "HK",
            "time_info" : time_info
        }
    }
    url = "http://app.investassistant.com:80/MiningStock/stock/isStockTime"
    body_value = data
    register_openers()
    body_value  = json.JSONEncoder().encode(body_value)
    request = urllib2.Request(url, body_value)
    request.add_header('Content-Type','application/json')
    result = urllib2.urlopen(request).read()
    ret = json.loads(result)
    is_stockdate = ret['response_data']['is_stockdate']
    if is_stockdate == 1:
        return True
    elif is_stockdate ==0:
        return False

def getWorkDay(now,x):
    '''
    :param now:当前的datetime
    :param x: x个工作日
    :return:
    '''
    delta = x/abs(x)
    counter = abs(x)
    endDay = now
    while counter!=0:
        endDay = endDay+datetime.timedelta(days=delta)
        if isHKTrading_day(endDay):
            counter-=1
    return endDay

mysql2_231 = Mysql('mysql2.231')
mysql5_153 = Mysql('mysql5.153')

boc = mysql2_231.execute("select deposit_id,bank_id,`desc`,update_time,create_time,bank_name from in_account where bank_name = 'bank_of_china_hk'",'bank')

deposit_detail = mysql5_153.execute("select id,apply_date,audit_date,audit_status,transfer_certificate_no,deposit_amount,transfer_amount,deposit_bank_code,receive_bank_code,deposit_type from deposit_detail where receive_bank_code = '012' and audit_date>='2018-07-15 00:00:00' and audit_status = '300010' and transfer_certificate_no is not NULL and transfer_certificate_no !=''",'miningaccount')

boc_list = []
for i in boc:
    deposit_id = i[0]
    bank_id = i[1]
    number = i[2]
    boc_list.append([deposit_id,bank_id,number])

deposit_detail_list = []
deposit_number = {}
normal = {}
for i,j in enumerate(deposit_detail):
    id = j[0]
    # apply_date = j[1]
    # audit_date = j[2]
    # audit_status = j[3]
    transfer_certificate_no = j[4].replace(u";","")
    # deposit_amount = j[5]
    # transfer_amount = j[6]
    # deposit_bank_code = j[7]
    # receive_bank_code = j[8]
    # transactionType = j[9]
    deposit_detail_list.append([id,transfer_certificate_no])
    deposit_number[transfer_certificate_no] = i
    normal[id] = transfer_certificate_no
ret = []
counter = 0
for i in boc_list:
    # if i[-1]!='61264892':
    #     continue
    if i[2] in deposit_number:
        counter+=1
        bank = deposit_detail_list[deposit_number[i[2]]]
        # if float(bank[5])!=float(dep[4].replace("HKD","").replace("USD",'').replace(",","").strip()):
        #     print "error",bank[5],dep[5],dep[6],dep[4]
        #     continue
        if i[0] == bank[0]:
            ret.append([i,bank])
        else:
            if i[0] in normal:
                print i," is ",normal[i[0]]
            else:
                print i,"cannot find"
        # print dep[-1],bank[-1],dep[0],bank[0]
        # number = dep[-1]
        # transactionAbstract = dep[2]
        # bank_id = bank[0]
        # dep_id = dep[0]
        # disposeDate = i[1].replace(u"\xa0","")
        # if len(disposeDate.strip())==10:
        #     disposeDate = disposeDate+" 12:00"
        # disposeDate = datetime.datetime.strptime(disposeDate,'%Y/%m/%d %H:%M')
        # apply_date = bank[1]
        # delta_hours = (disposeDate-apply_date).total_seconds()*1.0/(60*60)
        # ret.append([bank_id,dep_id,transactionAbstract,delta_hours,number])
    else:
        if i[0] in normal:
            print i," is ",normal[i[0]]
        else:
            print i,"cannot find"
df = pd.DataFrame(ret,columns=['bank_id','dep_id','transactionAbstract','delta_hour','number'])
df.to_csv("/home/eos/data/other/auto_bank_time.csv",encoding='utf8')




