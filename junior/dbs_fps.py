#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2020-06-17 下午12:59
'''
'''
此脚本用于补充dbs丢失数据
'''
import os
os.chdir("E:\git\hodgepodge")
from db.mysql.SqlUtil import Mysql
import logging
import json
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s',filename='dbs.log',filemode='a')
logger =logging.getLogger("【DBS】 ")

db = Mysql("mysql5.106")
sql = "select * from bank.dbs_inward where create_time>'2020-05-01' and txnType = 'INCOMING ARE' and status = '2' order by create_time desc"
data = db.execute(sql,"bank")

logger.info("\n\n"
			"************ 以下为缺失的数据 ************"
			"\n")
ret = []
for i in data:
	id_ = i[0]
	txnRefid = i[9]
	date = str(i[10])
	name = i[20]
	currency = i[18]
	amount = i[19]
	fps_sql = f"select send_json,reception_json from bank.dbs_fps where receivingname = '{name}' and bankreference ='{txnRefid}' and txnccy='{currency}' and txnAmount={float(amount)}"
	fps_ret = db.execute(fps_sql)
	line = [name,date,amount,currency,txnRefid]
	if len(fps_ret) == 1:
		fps_json = fps_ret[0]
		fps_info = fps_json[0]
		fps = json.loads(fps_info)
		detail = fps['txnInfo']['rmtInf']['paymentDetails']
		ret.append([id_,detail,detail,0])
		logger.info(f'''fps记录 {str(line)}''')
	else:
		logger.info(f'''非fps记录 {str(line)}''')

logger.info("\n\n"
			"************ 开始补充fps数据 ************"
			"\n")

for i in ret:
	update_sql = f"update bank.dbs_inward set paymentDetails='{i[1]}',addtlInf='{i[2]}',status={i[3]} where id={i[0]}"
	db.execute(update_sql)
	logger.info(f'''更新了fps记录 {str(i)}''')