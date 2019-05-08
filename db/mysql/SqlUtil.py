#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:eos
创建时间:2018-08-07 下午3:33
'''
import ConfigParser
import logging.config
import os

import MySQLdb

from log.const import const

logging.config.dictConfig(const.LOGGING)
logger = logging.getLogger(__file__)
class Mysql():

    def __init__(self,server):
        '''
        :param host:
        '''
        self.confPath = "db/config/mysql.cfg"
        self.host = self.getConfig(server)

    def getConfig(self,server):
        '''
        :return:
        '''
        conf = ConfigParser.ConfigParser()
        conf.readfp(open(self.confPath))
        host = conf.get(server,"host")
        user = conf.get(server,"user")
        passwd = conf.get(server,"passwd")
        return {"host":host,"user":user,"passwd":passwd}



    def execute(self,sql,db = "report"):
        '''
        :rtype: object
        :param sql:
        :param db:
        :return:
        '''
        con = MySQLdb.connect(self.host['host'],self.host['user'],self.host['passwd'],db,charset='utf8')
        cursor = con.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        con.commit()
        con.close()
        return result
    def executeMany(self,sql,columns=[],data = None,db = "app_data"):
        '''
        只适用于插入insert into
        其中sql不需要写 '(......) values (......)'
        :param sql:
        :param columns:
        :param data:
        :param db:
        :return:
        '''
        con = MySQLdb.connect(self.host['host'],self.host['user'],self.host['passwd'],db,charset='utf8')
        cursor = con.cursor()
        counter = 0
        sql_end = "(%s) values (%s)"%(','.join(columns),','.join(['%s']*len(columns)))
        sql = sql+sql_end
        if len(data)>1000:
            k = len(data)/1000+1
            for i in range(k):
                start = i*1000
                end = i*1000+1000
                if end>=len(data):
                    end = len(data)
                data_k = data[start:end]
                #sql ="select title from news where rowkey in {}".format(columns).replace("[","(").replace("]",")")
                #sql = "insert into table (%s) values (%s)"%(','.join(columns),','.join(['%s']*len(columns)))
                cursor.executemany(sql,data_k)
                counter=counter+end-start
        else:
            cursor.executemany(sql,data)
            counter = len(data)
        cursor.close()
        con.commit()
        con.close()
        import re
        finds = re.findall("(?<=from|into).*",sql)
        table_name = finds[0] if len(finds)>0 else "?"
        logger.info(":::::::save %s items to %s.%s"%(counter,db,table_name))
        # return result
    def getDBColumns(self,db,table):
        '''
        :param db:
        :param table:
        :return:
        '''
        sql = "select GROUP_CONCAT(COLUMN_NAME) from information_schema.COLUMNS where table_name = '%s' and table_schema ='%s'"%(table,db)
        con = MySQLdb.connect(self.host['host'],self.host['user'],self.host['passwd'],db,charset='utf8')
        cursor = con.cursor()
        cursor.execute(sql)
        columns = cursor.fetchall()
        cursor.close()
        con.commit()
        con.close()
        return str(columns[0][0]).split(",")

    def genDictMysql(self,items,columns):
        '''
        :param items:items为mysql的结果
        :param columns:
        :return:
        '''
        ret = {}
        for i,j in enumerate(items):

            ret[columns[i]] = j
        return ret