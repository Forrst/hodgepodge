#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-08-29 上午11:43
'''
import urllib2

import datetime
from MySQLdb import times
from flask import Flask, render_template, jsonify, abort, make_response, request
import logging
import json
from poster.streaminghttp import register_openers

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("dbs_flask")
app = Flask(__name__)
tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

hostname = ""
keyId = ""
orgId = ""


@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    return jsonify({'tasks': tasks})

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = filter(lambda t: t['id'] == task_id, tasks)
    if len(task) == 0:
        abort(404)
    return jsonify({'task': task[0]})

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/todo/api/v1.0/tasks', methods=['POST'])
def create_task():
    logging.info(request.json)
    if not request.json or not 'title' in request.json:
        abort(400)
    task = {
        'id': tasks[-1]['id'] + 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
        'done': False
    }
    tasks.append(task)
    return jsonify({'task': task}), 201

# def inward_insert(request_json):
#     orgId = request_json['orgId']
#     apiKey = request_json['apiKey']
#     content = request_json

@app.route('/api/bank/v1.0/dbs/inward', methods=['POST'])
def inward():
    logger.info("\n\n")
    logger.info("****************received message as below****************")
    logger.info(request.json)
    logger.info("**********************************************************")
    logger.info("\n\n")
    return jsonify({'response': request.json,'status':'ok'}), 200





def edda_initiation_specification():
    '''
    eDDA 开通申请
    通过post请求dbs的地址
    :return:
    '''
    time_info = times.strftime('%Y%m%d%H%M%S')
    #自增的请求id 星展会检查这个id是否请求过
    msgID = 0
    ctry = ""
    ddaRef = ""
    seqType = ""
    frqcyType = ""

    #如果seqType 为RCUR此项需要填写
    countPerPeriod = ""
    effDate = ""
    expDate = ""
    collAmt = ""
    collAmtCcy = ""
    data = {
        "header" : {
            "msgId" : msgID,
            "orgId" : orgId,
            "timeStamp" : datetime.datetime.now(),
            "ctry" : ctry

        },
        "txnInfo" : {
            "txnType" : "DDA",
            "ddaRef" : ddaRef,
            #DDMU表示电子版的 DDMP表示纸质版的
            "mandateType":"DDMU",
            #recurring表示循环的 one-off表示一次性的
            "seqType":seqType,
            "frqcyType":frqcyType,
            "countPerPeriod":countPerPeriod,
            "effDate":effDate,
            "expDate":expDate,
            "collAmt":collAmt,
            "collAmtCcy":collAmtCcy,
            #备填但不可与每次从账户扣的最多的钱数
            "maxAmt":"",
            "reason":""
        },
        "creditor":{
            "name":"",
            "accountNo":"",
            "accountCcy":""
        },
        "debtor":{
            "name":"",
            "proxyType":"",
            "bankId":"",
            "ultimateName":"",
            "corpCustomer":"",
            "corpCutomerId":"",
            "corpCustomerIdType":"",
            "prvtCustomer":"",
            "prvtCustomerId":"",
            "prvtCustomerIdType":""
        }
    }
    url = hostname+"/rapid/hkfps/v1/edda/setup"
    body_value = data
    register_openers()
    body_value  = json.JSONEncoder().encode(body_value)
    request = urllib2.Request(url, body_value)
    request.add_header('Content-Type','application/json')
    result = urllib2.urlopen(request).read()
    ret = json.loads(result)
    return ret

def edda_initiation_status():



    time_info = times.strftime('%Y%m%d%H%M%S')
    data = {
        "header" : {
            "msgId" : 1,
            "orgId" : "046097B8690EA0D2DDFC76CA05D957C8",
            "timeStamp" : "1B1D9D39F50EE4302D65A3438FD43067",
            "ctry" : 1

        },
        "txnInfo" : {
            "txnType" : "HK",
            "ddaRef" : time_info,
            "mandateType":"",
            "seqType":"",
            "frqcyType":"",
            "countPerPeriod":"",
            "effDate":"",
            "expDate":"",
            "collAmt":"",
            "collAmtCcy":"",
            "maxAmt":"",
            "reason":""
        },
        "creditor":{
            "name":"",
            "accountNo":"",
            "accountCcy":""
        },
        "debtor":{
            "name":"",
            "proxyType":"",
            "bankId":"",
            "ultimateName":"",
            "corpCustomer":"",
            "corpCutomerId":"",
            "corpCustomerIdType":"",
            "prvtCustomer":"",
            "prvtCustomerId":"",
            "prvtCustomerIdType":""
        }
    }
    url = hostname+"/rapid/enquiry/v1/edda/status"
    body_value = data
    register_openers()
    body_value  = json.JSONEncoder().encode(body_value)
    request = urllib2.Request(url, body_value)
    request.add_header('Content-Type','application/json')
    result = urllib2.urlopen(request).read()
    ret = json.loads(result)
    return ret


@app.route('/api/bank/v1.0/dbs/edda/ack2',methods=['POST'])
def edda_ack2():
    logger.info("\n\n")
    logger.info("****************received message as below****************")
    logger.info(request.json)
    logger.info("**********************************************************")
    logger.info("\n\n")
    return jsonify({'response': request.json,'status':'ok'}), 200






@app.route('/rapid/hkfps/v1/edda/setup',methods=['POST'])
def edda_setup():

    orgId = '123456'






if __name__ == '__main__':
    # app.run(debug=True,host='1.119.141.146',port=8099)
    app.run(debug=True,host='localhost',port=8099)

'''
orgId : DBTCQ
apiKey : d0cc814c-c6c5-4f6a-b1d9-8e866c3a75cf
Content-Type : application/json
{
    "header": {
        "msgId": "20180122172128 ",
        "orgId": "DBSTDS01",
        "timeStamp": "2018-01-22T15:29:53.202",
        "ctry": "HK"
    },
    "txnInfo": {
        "txnType": "INCOMING ACT",
        "customerReference": "920171277999001",
        "txnRefId": "20180122SDC760714",
        "txnDate": "2018-01-22",
        "valueDt": "2018-01-22",
        "receivingParty": {
            "name": "rpName",
            "accountNo": "10001356388",
            "virtualAccountNo": "920171277999001"
        },
        "amtDtls": {
            "txnCcy": "HKD",
            "txnAmt": 35.3
        },
        "senderParty": {
            "name": " Company Name1",
            "accountNo": "0039008414"
        },
        "rmtInf": {
            "paymentDetails": "TEST 123456",
            "addtlInf": "Test"
        }
    }
}



b. Non Virtual Account
orgId : DBTCQ
apiKey : d0cc814c-c6c5-4f6a-b1d9-8e866c3a75cf
Content-Type : application/json
{
    "header": {
        "msgId": "20180122172128 ",
        "orgId": "DBSTDS01",
        "timeStamp": "2018-01-22T15:29:53.202",
        "ctry": "HK"
    },
    "txnInfo": {
        "txnType": "FPS",
        "customerReference": "TEST1234",
        "txnRefId": "0016FAST12345",
        "txnDate": "2018-01-23",
        "valueDt": "2018-01-23",
        "receivingParty": {
            "name": "rpName",
            "accountNo": "0039009100",
            "proxyType": "M",
            "proxyValue": "+852-12345678"
        },
        "amtDtls": {
            "txnCcy": "HKD",
            "txnAmt": 38.5
        },
        "senderParty": {
            "name": " Company Name2",
            "accountNo": "0039008414",
            "senderBankId": "185",
            "senderBankCode": "185",
            "senderBranchCode": "927"
        },
        "rmtInf": {
            "paymentDetails": "TEST 123456",
            "purposeCode": "CXBSNS"
        }
    }
}
'''
