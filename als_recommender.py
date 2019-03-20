#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-03-18 下午3:59
'''
import datetime

import MySQLdb
import numpy as np
import pandas as pd
import logging
from pandas.tslib import Timestamp as timestamp
from sklearn.metrics.pairwise import pairwise_distances

NUM_DAY = 23
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger = logging.getLogger(__file__)


class UserCF:
    def __init__(self):
        self.news_read_time = None
        self.id_half_baked = set()
        self.index_userid = None
        self.userid_index = None
        self.index_item = None
        self.item_index = None
        self.user_similarity = None
        self.record = None

    def loadData(self):
        '''
        获取23天以来用户的阅读数据
        :return:
        '''
        now = datetime.datetime.now()
        now = datetime.datetime(now.year, now.month, now.day)
        start_date = now - datetime.timedelta(days=NUM_DAY)
        conn = MySQLdb.connect(host='192.168.5.105', user='root', passwd='zunjiazichan123', db='report')
        sql = "select id,dt,user_id, news_id, news_type, start_time,stop_time, read_time from news_read_time where read_date>='%s' and read_date<='%s' and news_type = 1 or news_type = 9 order by read_date desc" % (
            str(start_date), str(now))
        columns = ['id', 'dt', 'user_id', 'news_id', 'news_type', 'start_time', 'stop_time', 'read_time']
        news_read = pd.read_sql(sql, conn, columns=columns)
        logger.info("load {} news_read_time records from read_date {} to {}".format(len(news_read), start_date, now))
        conn.close()
        self.news_read_time = news_read.dropna()

    def fillHalfBaked(self):
        '''
        用均值填补法填充缺少开始阅读的时间和停止阅读的时间
        :return:
        '''
        unique_userid = self.news_read_time['user_id'].unique()
        for id_ in unique_userid:
            mean = self.news_read_time[(self.news_read_time['user_id'] == id_) & (
                    self.news_read_time['start_time'] != timestamp('2029-01-01 00:00:00')) & (
                                               self.news_read_time['stop_time'] != timestamp(
                                           '2029-01-01 00:00:00'))]['read_time'].mean()
            self.news_read_time.loc[:, 'read_time'][(self.news_read_time['user_id'] == id_) & (
                    (self.news_read_time['start_time'] == timestamp('2029-01-01 00:00:00')) | (
                    self.news_read_time['stop_time'] == timestamp('2029-01-01 00:00:00')))] = mean
            # print "id:{} start_time or stop_time is null set default to mean:{}".format(id_, mean)
        self.news_read_time = self.news_read_time.dropna()

    def setMaxMin(self):
        '''
        将最大阅读时间设置为600，最小阅读时间设置为1秒
        :return:
        '''
        self.news_read_time.loc[:, 'read_time'][self.news_read_time['read_time'] > 600] = 600
        self.news_read_time.loc[:, 'read_time'][self.news_read_time['read_time'] == 0] = 1

    def computeMean(self):
        '''
        归一化
        :return:
        '''
        min_ = 1
        unique_userid = self.news_read_time['user_id'].unique()
        for id_ in unique_userid:
            # read_time_ = self.news_read_time[(self.news_read_time['user_id'] == id_) & (self.news_read_time['start_time'] != timestamp('2029-01-01 00:00:00')) & (self.news_read_time['stop_time'] != timestamp('2029-01-01 00:00:00'))]['read_time']
            self.news_read_time.loc[:, 'read_time'][self.news_read_time['user_id'] == id_] = (self.news_read_time[
                                                                                                  self.news_read_time[
                                                                                                      'user_id'] == id_][
                                                                                                  'read_time'] - min_ + 1) / 600

    def setIndex(self):
        '''
        设置用户id和新闻id的索引值
        :return:
        '''
        (self.index_userid, self.userid_index) = self.getIndexDict(self.news_read_time.user_id)
        (self.index_item, self.item_index) = self.getIndexDict(self.news_read_time.news_id)

    def getIndexDict(self, series):
        keyset = set()
        index = 0
        index_name = {}
        name_index = {}
        for i in series:
            if i not in keyset:
                index_name[index] = i
                name_index[i] = index
                keyset.add(i)
                index += 1
        return (index_name, name_index)

    def getSimlarMatrix(self):
        '''
        计算用户相似矩阵
        :return:
        '''
        n_items = self.news_read_time.news_id.unique().shape[0]
        n_users = self.news_read_time.user_id.unique().shape[0]
        user_item_id = self.news_read_time
        user_item_id['user_id'] = user_item_id['user_id'].map(lambda x: self.userid_index[x])
        user_item_id['news_id'] = user_item_id['news_id'].map(lambda x: self.item_index[x])
        train_data = user_item_id.ix[:, ['user_id', 'news_id', 'read_time']]
        train_data_matrix = np.zeros((n_users, n_items))
        for line in train_data.itertuples():
            train_data_matrix[line[1], line[2]] = line[3]
        self.user_similarity = pairwise_distances(train_data_matrix, metric='euclidean')

    def saveDistance(self):
        '''
        保存相似距离
        :return:
        '''
        ret = []
        total_user = len(self.user_similarity)
        for i in xrange(total_user):
            for j in xrange(total_user):
                userid1 = self.index_userid[i]
                userid2 = self.index_userid[j]
                u1_u2_distance = self.user_similarity[i, j]
                ret.append([userid1, userid2, u1_u2_distance])
        self.record = ret
        self.saveMany(data=ret)

    def saveMany(self, data=None, db="report"):
        '''
        只适用于插入insert into
        其中sql不需要写 '(......) values (......)'
        :param sql:
        :param columns:
        :param data:
        :param db:
        :return:
        '''
        con = MySQLdb.connect(host='192.168.5.105', user='root', passwd='zunjiazichan123', db=db, charset='utf8')
        cursor = con.cursor()
        total = len(data)
        now = datetime.datetime.now()
        logger.info("start insert into user_cf_all from {}".format(now))
        #以下有则更新无则插入的方法比较好
        sql = "insert into user_cf_all (from_user_id,to_user_id,distance) values(%s,%s,%s) on duplicate key update from_user_id = values(from_user_id),to_user_id = values(to_user_id), distance = values(distance) "
        num_once = 10000
        if len(data)>num_once:
            k = len(data)/num_once+1
            for i in range(k):
                start = i*num_once
                end = i*num_once+num_once
                if end>=len(data):
                    end = len(data)
                data_k = data[start:end]
                cursor.executemany(sql,data_k)
                con.commit()
        else:
            cursor.executemany(sql,data)
        cursor.close()
        con.commit()
        con.close()
        end = datetime.datetime.now()
        logger.info("end insert into user_cf_all at {}".format(end))
        period = (end-now).seconds
        logger.info("total records {} total time use: {} seconds".format(total,period))

    def process(self):
        usercf = UserCF()
        usercf.loadData()
        usercf.setMaxMin()
        usercf.fillHalfBaked()
        usercf.computeMean()
        usercf.setIndex()
        usercf.getSimlarMatrix()
        usercf.saveDistance()


if __name__ == "__main__":
    print "start time {}".format(datetime.datetime.now())
    usercf = UserCF()
    usercf.process()
