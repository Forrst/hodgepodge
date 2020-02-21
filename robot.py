#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-26 上午9:59
'''
import threading
import itchat
import logging
import time
import requests
import schedule
import json
from itchat.content import *
from db.mysql.SqlUtil import Mysql

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("[robot.py]")

url = "https://app.investassistant.com/MiningStock/stock/getStockBasicInfo"
#http://www.zinvestglobal.com/?hmsr=wx
code_exchange = {}
code_name = {}
name_code = {}
code_exchange_name = {}

def syn():
    logger.info("\n\n:::::::::::::::::::开始pull港美A股基础表:::::::::::::::::::")
    global code_exchange_name
    global code_name
    global name_code
    global code_exchange_name
    logger.info(":::::::::::::::::::同步前code_exchange总量：{}:::::::::::::::::::".format(len(code_exchange)))
    logger.info(":::::::::::::::::::同步前code_name总量：{}:::::::::::::::::::".format(len(code_name)))
    logger.info(":::::::::::::::::::同步前name_code总量：{}:::::::::::::::::::".format(len(name_code)))
    logger.info(":::::::::::::::::::同步前code_exchange_name总量：{}:::::::::::::::::::".format(len(code_exchange_name)))
    db = Mysql("mysql5.106")
    cn_sql = "select exchange,code,name from stock_info group by exchange,code "
    hk_sql = "select exchange,code,name from stock_info_hk group by exchange,code"
    us_sql = "select exchange,code,name,chname from stock_info_us group by exchange,code"
    cn_data = db.execute(cn_sql,"app_data")
    hk_data = db.execute(hk_sql,"app_data")
    us_data = db.execute(us_sql,"us_data")
    for i in cn_data:
        code_exchange_name[i[1]+"."+i[0]] =i[2]
        if i[1] in code_exchange:
            # print i[1],code_exchange[i[1]],code_name[i[1]]
            # print i[0],i[1],i[2]
            code_exchange[i[1]] = code_exchange[i[1]]+";"+i[0]
        else:
            code_exchange[i[1]] = i[0]
        name_code[i[2]]=i[1]+"_"+i[0];code_name[i[1]]=i[2]
    for i in hk_data:
        code_exchange_name[i[1]+"."+i[0]] =i[2]
        if i[1] in code_exchange:
            # print i[1],code_exchange[i[1]],code_name[i[1]]
            # print i[0],i[1],i[2]
            code_exchange[i[1]] = code_exchange[i[1]]+";"+i[0]
        else:
            code_exchange[i[1]] = i[0]
        name_code[i[2]]=i[1]+"_"+i[0];code_name[i[1]]=i[2]
    for i in us_data:
        code_exchange_name[i[1]+"."+i[0]] =i[2]
        if i[1] in code_exchange:
            # print i[1],code_exchange[i[1]],code_name[i[1]]
            # print i[0],i[1],i[2]
            code_exchange[i[1]] = code_exchange[i[1]]+";"+i[0]
        else:
            code_exchange[i[1]] = i[0]
        name_code[i[2]]=i[1]+"_"+i[0];name_code[i[3]]=i[1]+"_"+i[0];code_name[i[1]]=i[2]
    logger.info("\n\n:::::::::::::::::::结束pull港美A股基础表:::::::::::::::::::")
    logger.info(":::::::::::::::::::同步后code_exchange总量：{}:::::::::::::::::::".format(len(code_exchange)))
    logger.info(":::::::::::::::::::同步后code_name总量：{}:::::::::::::::::::".format(len(code_name)))
    logger.info(":::::::::::::::::::同步后name_code总量：{}:::::::::::::::::::".format(len(name_code)))
    logger.info(":::::::::::::::::::同步后code_exchange_name总量：{}:::::::::::::::::::".format(len(code_exchange_name)))

def synThread():
    threading.Thread(target=syn()).start()


@itchat.msg_register(TEXT, isGroupChat=True)
def group_reply_text(msg):
    username = msg['ActualNickName']
    chatroom_id = msg['FromUserName']
    chatroom_name = msg['User']['NickName']
    # logger.info(u"【{}】群的【{}】说: {}".format(chatroom_name,username,msg['Content']))
    if msg['Type'] == TEXT:
        content = msg['Content'].upper()
        if isinstance(content.encode("utf-8"),str) and content.encode("utf-8").isdigit() and len(content.encode("utf-8"))==4:
            content = ("0"+content.encode("utf-8")).decode("utf-8")
        if isinstance(content.encode("utf-8"),str) and content.encode("utf-8").isdigit() and len(content.encode("utf-8"))==3:
            content = ("00"+content.encode("utf-8")).decode("utf-8")
        if content in name_code or content in code_exchange or content in code_exchange_name:
            logger.info(u"【{}】群的【{}】查询报价: {}".format(chatroom_name,username,msg['Content']))
            response = getRequestDate(content)
            itchat.send_msg(response,msg['FromUserName'])
            # if chatroom_id == u'@@805faaab88bffc6001f2df8efa1c1338c8291f825f3d8ce50da8c0089df0a5ba':
            #     # 发送者的昵称
            #     url = msg['Url']
            #     print url
            #     title = msg['Text']
            # # 消息并不是来自于需要同步的群
            # if msg['Type'] == TEXT:
            #     content = msg['Content']
            # elif msg['Type'] == SHARING:
            #     content = msg['Text']

@itchat.msg_register(TEXT,isGroupChat=False)
def reply_one_text(msg):
    RemarkName = msg['User']['RemarkName']
    NickName = msg['User']['NickName']
    # logger.info(u"【{}】群的【{}】说: {}".format(chatroom_name,username,msg['Content']))
    if msg['Type'] == TEXT:
        content = msg['Content'].upper()
        if isinstance(content.encode("utf-8"),str) and content.encode("utf-8").isdigit() and len(content.encode("utf-8"))==4:
            content = ("0"+content.encode("utf-8")).decode("utf-8")
        if isinstance(content.encode("utf-8"),str) and content.encode("utf-8").isdigit() and len(content.encode("utf-8"))==3:
            content = ("00"+content.encode("utf-8")).decode("utf-8")
        if content in name_code or content in code_exchange or content in code_exchange_name:
            logger.info(u"您的昵称为【{}】备注为【{}】的好友查询报价: {}".format(NickName,RemarkName,msg['Content']))
            response = getRequestDate(content)
            itchat.send_msg(response,msg['FromUserName'])






def getRequestDate(content):
    result = ""
    code = ''
    exchange = ''
    if content in name_code:
        nameCodeList = name_code[content].split("_")
        code = nameCodeList[0]
        exchange = nameCodeList[1]
    elif content in code_exchange:
        code = content
        code_exchangeList = code_exchange[content].split(";")
        if len(code_exchangeList)>=2:
            exchange = code_exchangeList
        else:
            exchange = code_exchange[content]
    elif content in code_exchange_name:
        codeExchange = content.split(".")
        code = codeExchange[0]
        exchange = codeExchange[1]
    currency = {"HKEX":u"港元","SESH":u"元","SESZ":u"元","NASDAQ":u"美元","NASDAQ":u"美元","NYSE":u"美元"}
    exchange_code = {"HKEX":u"HK","SESH":u"SH","SESZ":u"SZ","NASDAQ":u"US","NYSE":u"US"}
    currency_name = ""
    attention = ""
    if isinstance(exchange,list):
        stockstr = ""
        orstr = ""
        for i in exchange:
            stockstr = stockstr+code_exchange_name[code+"."+i]+"("+code+"."+i+")"+"\n"
            orstr = orstr+code+u"."+i+u"或者"
        orstr =orstr[:-2]
        attention = u"温馨提示：您输入的股票代码包含多个股票\n{}您可以尝试输入{}以查询具体的股票".format(stockstr,orstr)
        if code.startswith(u"00") and u"SESH" in exchange:
            exchange = u"SESZ"
            attention = u"\n(温馨提示：您输入的股票代码与指数代码一样默认为您显示股票的行情\n{}您可以尝试输入{}以查询具体的股票)".format(stockstr,orstr)
    if isinstance(exchange,list):
        result = attention
    if isinstance(exchange,unicode):
        if exchange in currency:
            currency_name = currency[exchange]
        data = {
            "header": {
                "area_code": "+86",
                "auth_code": "9AA08948E2A35D4D38DC1BDE37337CA0",
                "imei": "02:00:4C:4F:4F:50",
                "key_code": "B74DEDE6D5808D161DA4125FC28A14E9",
                "system_time": int(time.time()),
                "ua": {
                    "app_version": "1.1.10Beta",
                    "channel": "pc",
                    "height": 1080,
                    "model": "windows",
                    "os_version": "7sp1",
                    "platform": "pc_win",
                    "trader": "mining_p",
                    "width": 1920
                },
                "user_name": "15210861736",
                "user_password": "",
                "user_type": 1,
                "version": 1
            },
            "request_data": {
                "code": "{}".format(code),
                "exchange": "{}".format(exchange)
            }
        }
        r = requests.get(url,data=json.dumps(data))
        ret = r.json()
        r.close()
        print(ret['response_data'])
        if 'stock_info' in ret['response_data']:
            try:
                name = ret['response_data']['stock_info']['name']
                code = ret['response_data']['stock_info']['code']
                exchangecode = ret['response_data']['stock_info']['exchange']
                close = ret['response_data']['close_px']
                pre_close = ret['response_data']['pre_close_px']
                if u"open_px" not in ret['response_data']:
                    if pre_close == 0:
                        riseandfall = 0.0
                    else:
                        riseandfall = round((close-pre_close)/pre_close*100,2)
                    result = u'''{}({}.{})\n\n【最新价】  {}{}\n【涨跌幅】  {}%\n【状  态】  {}\n\n本消息来源于港美股永久0佣的【尊嘉金融】\n36只热门美股免费送，戳☞ https://w.url.cn/s/Amphhew{}
                    '''.format(name,code,exchange_code[exchangecode],close,currency_name,riseandfall,ret['response_data']['stock_info']['time_info'],attention)
                else:
                    open = ret['response_data']['open_px']
                    high = ret['response_data']['high_px']
                    low = ret['response_data']['low_px']
                    volumn = ret['response_data']['total_volume_trade']
                    total_value = ret['response_data']['total_value_trade']
                    result = u'''{}({}.{})\n\n【最新价】  {}{}\n【涨跌幅】  {}%\n【开盘价】  {}{}\n【成交量】  {}万股\n【成交额】  {}亿{}\n【状  态】  {}\n\n本消息来源于港美股永久0佣的【尊嘉金融】\n36只热门美股免费送，戳☞ https://w.url.cn/s/Amphhew{}
                    '''.format(name,code,exchange_code[exchangecode],close,currency_name,round((close-pre_close)/pre_close*100,2),open,currency_name,round(volumn*1.0/10000,2),round(total_value*1.0/100000000,3),currency_name,ret['response_data']['stock_info']['time_info'],attention)
            except Exception as e:
                logger.error(e,exc_info=True)
    return result
if __name__ == "__main__":
    itchat.auto_login(enableCmdQR=2,hotReload=False)
    itchat.run()
    syn()
    # itchat.auto_login(enableCmdQR=2,hotReload=True)
    # itchat.run()
    schedule.every(12).hours.do(synThread)
    while True:
        schedule.run_pending()
        time.sleep(1)