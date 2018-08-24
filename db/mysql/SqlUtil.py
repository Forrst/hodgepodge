#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:eos
创建时间:2018-08-07 下午3:33
'''
import MySQLdb
import ConfigParser

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

    def getDBColumns(self,db,table):
        '''
        :param db:
        :param table:
        :return:
        '''
        sql = "select GROUP_CONCAT(COLUMN_NAME) from information_schema.COLUMNS where table_name = %s and table_schema =%s"%(db,table)
        con = MySQLdb.connect(self.host['host'],self.host['user'],self.host['passwd'],db,charset='utf8')
        cursor = con.cursor()
        cursor.execute(sql)
        columns = cursor.fetchall()
        cursor.close()
        con.commit()
        con.close()
        return columns

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