#!/usr/bin/python3
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
import re
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

robot_qa = {}

def loadRobot_qa():
    logger.info(":::::::::::::::::::开始导入机器人关键词库:::::::::::::::::::")
    global robot_qa
    db = Mysql("mysql2.231")
    robot_qa_sql = "select keywords,answer from robot_qa"
    robot_qa_data = db.execute(robot_qa_sql,"refine")
    for i in robot_qa_data:
        robot_qa[i[0]] = i[1]
    logger.info(f":::::::::::::::::::导入关键词{len(robot_qa_data)}组:::::::::::::::::::")

def get_robot_keys(content):
    for key in robot_qa.keys():
        regex_str = get_regex(key)
        regex = re.compile(regex_str)
        finds = regex.findall(content)
        if len(finds)>0:
            return key
    return ""

def get_regex(str):
    '''
    生成正则表达式
    :param str:
    :return:
    '''
    keywordlist = str.split("||")
    words_str_list = []
    for key in keywordlist:
        words = key.split("&&")
        words_str = "("
        for word in words:
            str = f"(?=.*{word})"
            words_str = words_str+str
        words_str = words_str+")"
        words_str_list.append(words_str)
    return "|".join(words_str_list)+"^.*$"

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
        if i[1] in code_exchange and code_exchange[i[1]]!=i[0]:
            # print i[1],code_exchange[i[1]],code_name[i[1]]
            # print i[0],i[1],i[2]
            code_exchange[i[1]] = code_exchange[i[1]]+";"+i[0]
        else:
            code_exchange[i[1]] = i[0]
        name_code[i[2]]=i[1]+"_"+i[0];code_name[i[1]]=i[2]
    for i in hk_data:
        code_exchange_name[i[1]+"."+i[0]] =i[2]
        if i[1] in code_exchange and code_exchange[i[1]]!=i[0]:
            # print i[1],code_exchange[i[1]],code_name[i[1]]
            # print i[0],i[1],i[2]
            code_exchange[i[1]] = code_exchange[i[1]]+";"+i[0]
        else:
            code_exchange[i[1]] = i[0]
        name_code[i[2]]=i[1]+"_"+i[0];code_name[i[1]]=i[2]
    for i in us_data:
        code_exchange_name[i[1]+"."+i[0]] =i[2]
        if i[1] in code_exchange and code_exchange[i[1]]!=i[0]:
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

def synRobot_qa():
    threading.Thread(target=loadRobot_qa()).start()

@itchat.msg_register(TEXT, isGroupChat=True)
def group_reply_text(msg):
    username = msg['ActualNickName']
    chatroom_id = msg['FromUserName']
    chatroom_name = msg['User']['NickName']
    # logger.info(u"【{}】群的【{}】说: {}".format(chatroom_name,username,msg['Content']))
    if msg['Type'] == TEXT:
        content = msg['Content'].upper()
        if isinstance(content,str) and content.isdigit() and len(content)==4:
            content = ("0"+content)
        if isinstance(content,str) and content.isdigit() and len(content)==3:
            content = ("00"+content)
        if content in name_code or content in code_exchange or content in code_exchange_name:
            logger.info("【{}】群的【{}】查询报价: {}".format(chatroom_name,username,msg['Content']))
            response = getRequestDate(content)
            itchat.send_msg(response,msg['FromUserName'])
        robot_key = get_robot_keys(content)
        # if "华美" in content and "入金" in content:
        #     logger.info("【{}】群的【{}】查询华美入金: {}".format(chatroom_name,username,msg['Content']))
        #     itchat.send_msg("尊嘉华美入金流程请参阅：https://w.url.cn/s/AlX3IFt",msg['FromUserName'])
        if "大陆" in content and "入金" in content:
            logger.info("【{}】群的【{}】查询大陆入金: {}".format(chatroom_name,username,msg['Content']))
            itchat.send_msg('''目前大陆银行卡入金失败率较高，建议使用境外银行卡入金。如您尝试大陆银行卡直接入金尊嘉，中间损失的手续费是需要您个人承担的，请您慎重选择。
更多入金说明https://www.zinvestglobal.com/qa/second/002''',msg['FromUserName'])
        elif len(robot_key)>0:
            logger.info("【{}】群的【{}】查询: {}".format(chatroom_name,username,msg['Content']))
            itchat.send_msg(robot_qa[robot_key],msg['FromUserName'])
            logger.info(robot_qa[robot_key])
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
        if isinstance(content,str) and content.isdigit() and len(content)==4:
            content = ("0"+content)
        if isinstance(content,str) and content.isdigit() and len(content)==3:
            content = ("00"+content)
        if content in name_code or content in code_exchange or content in code_exchange_name:
            logger.info("您的昵称为【{}】备注为【{}】的好友查询报价: {}".format(NickName,RemarkName,msg['Content']))
            response = getRequestDate(content)
            itchat.send_msg(response,msg['FromUserName'])
        robot_key = get_robot_keys(content)
        # if "华美" in content and "入金" in content:
        #     logger.info("【{}】群的【{}】查询华美入金: {}".format(NickName,RemarkName,msg['Content']))
        #     itchat.send_msg("尊嘉华美入金流程请参阅：https://w.url.cn/s/AlX3IFt",msg['FromUserName'])
        if "大陆" in content and "入金" in content:
            logger.info("【{}】群的【{}】查询大陆入金: {}".format(NickName,RemarkName,msg['Content']))
            itchat.send_msg('''目前大陆银行卡入金失败率较高，建议使用境外银行卡入金。如您尝试大陆银行卡直接入金尊嘉，中间损失的手续费是需要您个人承担的，请您慎重选择。
更多入金说明https://www.zinvestglobal.com/qa/second/002''',msg['FromUserName'])
            logger.info("尊嘉大陆入金优选中信/工商，更多入金说明：https://w.url.cn/s/Ad3t8mk")
        elif len(robot_key)>0:
            logger.info("【{}】群的【{}】查询: {}".format(NickName,RemarkName,msg['Content']))
            itchat.send_msg(robot_qa[robot_key],msg['FromUserName'])
            logger.info(robot_qa[robot_key])

def getRequestDate(content):
    result = ""
    code = ''
    exchange = ''
    if content in name_code and "老虎证券" not in content and "富途证券" not in content:
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
        attention = "温馨提示：您输入的股票代码包含多个股票\n{}您可以尝试输入{}以查询具体的股票".format(stockstr,orstr)
        if code.startswith("00") and "SESH" in exchange:
            exchange = "SESZ"
            attention = "\n(温馨提示：您输入的股票代码与指数代码一样默认为您显示股票的行情\n{}您可以尝试输入{}以查询具体的股票)".format(stockstr,orstr)
    if isinstance(exchange,list):
        result = attention
    if isinstance(exchange,str):
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
                eps = None
                pe = None
                epsstr = ''
                pestr = ''
                if 'eps' in ret['response_data']['stock_info']:
                    eps = ret['response_data']['stock_info']['eps']
                if 'pe' in ret['response_data']:
                    pe = ret['response_data']['pe']
                if eps is not None:
                    epsstr = f'【每股收益】  {round(eps,4)}\n'
                if pe is not None:
                    pestr = f'【市盈率】      {round(pe,4)}\n'
                exchangecode = ret['response_data']['stock_info']['exchange']
                close = ret['response_data']['close_px']
                pre_close = ret['response_data']['pre_close_px']
                if u"open_px" not in ret['response_data']:
                    if pre_close == 0:
                        riseandfall = 0.0
                    else:
                        riseandfall = round((close-pre_close)/pre_close*100,2)
                    result = u'''{}({}.{})\n\n【最新价】      {}{}\n【涨跌幅】      {}%\n{}{}【状   态】      {}\n\n尊嘉金融：随时随地，零佣交易。支持港股、美股和中华通（A股）。https://www.zinvestglobal.com?hmsr=rb
                    '''.format(name,code,exchange_code[exchangecode],close,currency_name,riseandfall,pestr,epsstr,ret['response_data']['stock_info']['time_info'],attention)
                else:
                    open = ret['response_data']['open_px']
                    # high = ret['response_data']['high_px']
                    # low = ret['response_data']['low_px']
                    volumn = ret['response_data']['total_volume_trade']
                    # total_value = ret['response_data']['total_value_trade']
                    result = u'''{}({}.{})\n\n【最新价】      {}{}\n【涨跌幅】      {}%\n【开盘价】      {}{}\n【成交量】      {}万股\n{}{}【状   态】      {}\n\n尊嘉金融：随时随地，零佣交易。支持港股、美股和中华通（A股）。https://www.zinvestglobal.com?hmsr=rb                    '''.format(name,code,exchange_code[exchangecode],close,currency_name,round((close-pre_close)/pre_close*100,2),open,currency_name,round(volumn*1.0/10000,2),pestr,epsstr,ret['response_data']['stock_info']['time_info'],attention)

            except Exception as e:
                logger.error(e,exc_info=True)
    return result
if __name__ == "__main__":
    getRequestDate("6666")
    syn()
    loadRobot_qa()
    itchat.auto_login(enableCmdQR=2,hotReload=False)
    itchat.run(blockThread=False)
    schedule.every(12).hours.do(synThread)
    schedule.every(1).hours.do(synRobot_qa)
    while True:
        schedule.run_pending()
        time.sleep(1)

