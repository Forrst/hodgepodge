#!/usr/bin/python
# -*- coding:utf-8 -*- 
"""
作者:eos
创建时间:2018-08-06 上午11:46
"""
import os
os.chdir("/home/eos/git/Mr.Jaryn/")
import logging.config
from db.mysql import SqlUtil
from log.const import const
import pandas as pd
logging.config.dictConfig(const.LOGGING)
logger = logging.getLogger(__file__)
'''
#自选5.105
#获取dt=2018-07-31的用户阅读时长
# date = "2018-07-01"
# getAllUsers = "select a.user_id,name,phone_number,funds_account,sum(read_time) read_times from news_read_time a left join user_account_info b on a.user_id = b.user_id where read_day >= '%s' and funds_account is not null group by user_id having read_times >= 2 order by read_times desc"%date
mysql = SqlUtil.Mysql("mysql5.105")
sql = "select id,read_day,read_hour,user_id,news_type,news_id,news_channel,start_time,stop_time,read_time from news_read_time order by user_id desc,read_day desc,read_hour desc"
con = mysql.getCon()
index_col = "id,read_day,read_hour,user_id,news_type,news_id,news_channel,start_time,stop_time,read_time".split(",")
news_read_time = pd.read_sql(sql,con,columns=index_col)
news_read_time_preprocess = news_read_time[news_read_time['stop_time']<'2018-12-10 00:00:00'][news_read_time['read_time']>1]
news_read_time_sorted = news_read_time_preprocess.sort_values(by='read_time',axis=0,ascending=False)
news_filter_by_read_time = news_read_time_sorted[news_read_time_sorted['read_time']<900]
news_read_time_sum = news_filter_by_read_time.groupby('user_id',as_index=False).sum()
total_news_read_time = news_read_time_sum.sort_values(by='read_time',axis=0,ascending=False)
news_read_time['url'] = "http://t.financialdatamining.com/news/"+news_read_time['news_type']+"/"+news_read_time['news_id']+".html"
news_read_time[news_read_time['user_id']=='29100']
'''


'''
# all_user_read_time = mysql.execute(getAllUsers)
#
# f = open("/home/eos/临时文件/user_info.csv","a+")
# for i in all_user_read_time:
#     ids = i[0].encode("utf8") if i[0] is not None else "None"
#     name = i[1].encode("utf8") if i[1] is not None else "None"
#     phone = i[2].encode("utf8") if i[2] is not None else "None"
#     account = i[3].encode("utf8") if i[3] is not None else "None"
#     readTime = str(i[4])
#     line = "\t".join([ids,name,phone,account,readTime])+"\n"
#     f.write(line)
#     f.flush()
# f.close()
#
#
# user_info = SqlUtil.select("select name,phone_number from user_account_info where user_id = '29462'")
#
# #港股募资额
# select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>'2018-01-01' order by IssueEndDate desc
#
# select a.IssueEndDate,b.SECUCODE,b.CHINAME,a.TotalProceeds from HK_SHAREIPO a inner join HK_SECUMAIN b on a.innercode = b.innercode where b.SECUMARKET = 72 and b.SECUCATEGORY = 51 and b.LISTEDSTATE = 1  and to_char(IssueEndDate,'YYYY-MM-DD')>='2017-01-01' and to_char(IssueEndDate,'YYYY-MM-DD')<='2017-07-31' order by IssueEndDate des
#
#
#
# #前20的信息
# # head20 = [i[0] for i in result[0:20]]
# # sql_user_info = "select * from user_account_info where user_id = '16380'"
# # user_info = getDataFrom5_105(sql_user_info)
# #
# # #获取具体点击的数据
# # sql = "select distinct user_id,news_type,news_id,news_channel,start_time,stop_time,read_time,platform,channel,dt from news_read_time where dt = '2018-07-31' and user_id = '16380' order by start_time desc"
# # result = getDataFrom5_105(sql)
#
# #获取用户标签
# user_id = "16380"
# md5_user_id = hashlib.md5(user_id).hexdigest()[:16]
#
# # con = happybase.Connection("192.168.2.232")
import happybase
con = happybase.Connection("192.168.5.156")
con.open()
table = con.table("user_tag")
#481c7ffffe9859ff915b7c00d74c0000
ret = table.row("481c7ffffe9859a23d83ed31f8310000".decode("hex"))
ret = table.row("481c7ffffe9859ff915b7c00d74c0000".decode("hex"))
# table.delete("b706835de79a2b4e02da000000000000")
for i in ret:
    if i == 'info:new_tag_results' or i=='info:update_tag_results':
        print i,ret[i]
# con.close()
'''
sql = "select id,read_day,read_hour,user_id,news_type,news_id,news_channel,start_time,stop_time,read_time from news_read_time where dt>='2018-10-01' order by user_id desc,read_day desc,read_hour desc"
index_col = "id,read_day,read_hour,user_id,news_type,news_id,news_channel,start_time,stop_time,read_time".split(",")
mysql = SqlUtil.Mysql("mysql5.105")
con = mysql.getCon()
news_read_time = pd.read_sql(sql,con,columns=index_col)

from pandas.tslib import Timestamp as timestamp
import numpy as np

news_read_time_ = news_read_time[news_read_time['news_type']==u'1'][news_read_time['start_time']!=timestamp('2029-01-01 00:00:00')][news_read_time['stop_time']!=timestamp('2029-01-01 00:00:00')][news_read_time['read_time']>=1]

user_item = news_read_time_[['user_id','news_id','read_time','stop_time']]
user_item_ = user_item.drop_duplicates(keep='first')


def getNumberDict(series):
    keyset = set()
    index = 0
    index_name = {}
    name_index = {}
    for i in series:
        if i not in keyset:
            index_name[index] = i
            name_index[i] = index
            keyset.add(i)
            index+=1
    return (index_name,name_index)
(index_userid,userid_index) = getNumberDict(user_item_['user_id'])
(index_item,item_index) = getNumberDict(user_item_['news_id'])

user_item_id = user_item_
user_item_id['user_id'] = user_item_id['user_id'].map(lambda x:userid_index[x])
user_item_id['news_id'] = user_item_id['news_id'].map(lambda x:item_index[x])


n_items = user_item_id.news_id.unique().shape[0]
n_users = user_item_id.user_id.unique().shape[0]
#userid 为6502阅读过得新闻为:

'''
user_id_dataframe = user_item_id
df = user_id_dataframe[user_item_id['user_id']==userid_index['6502']]
df['rowkey'] = df[user_item_id['user_id']==userid_index['6502']]['news_id'].map(lambda x:index_item[x])
'''
from sklearn import cross_validation as cv
# train_data, test_data = cv.train_test_split(user_item_id, test_size=0.25)
train_data = user_item_id
train_data_matrix = np.zeros((n_users, n_items))
for line in train_data.itertuples():
    train_data_matrix[line[1], line[2]] = line[3]

# test_data_matrix = np.zeros((n_users, n_items))
# for line in test_data.itertuples():
#     test_data_matrix[line[1], line[2]] = line[3]

from sklearn.metrics.pairwise import pairwise_distances
user_similarity = pairwise_distances(train_data_matrix, metric='cosine')
item_similarity = pairwise_distances(train_data_matrix.T, metric='cosine')

def predict(ratings, similarity, type='user'):
    if type == 'user':
        #axis=1表示对行求均值
        mean_user_rating = ratings.mean(axis=1)
        #You use np.newaxis so that mean_user_rating has same format as ratings
        ratings_diff = (ratings - mean_user_rating[:, np.newaxis])
        pred = mean_user_rating[:, np.newaxis] + similarity.dot(ratings_diff) / np.array([np.abs(similarity).sum(axis=1)]).T
    elif type == 'item':
        pred = ratings.dot(similarity) / np.array([np.abs(similarity).sum(axis=1)])
    return pred

# item_prediction = predict(train_data_matrix, item_similarity, type='item')
user_prediction = predict(train_data_matrix, user_similarity, type='user')
print(user_prediction)

#推荐的新闻
userid = '768'
u_index = userid_index[userid]

ret = []
for i,j in enumerate(user_prediction[u_index]):
    ret.append([i,j])
df = pd.DataFrame(ret)
df.columns = ['index','score']
df_ = df.sort_values('score',ascending=True)
df_['rowkey'] = df_.apply(lambda x:index_item[x[0]],axis=1)

df_fil = df_.head(100)
df_fil['title'] = df_fil.apply(lambda x:get_title(x['rowkey']),axis=1)
df_fil.to_csv("/home/eos/df_fil.csv",encoding="utf-8")
import happybase
def get_title(x):
    con = happybase.Connection("192.168.5.156")
    con.open()
    table = con.table("news")
    ret = table.row(x.decode("hex"))
    con.close()
    return ret['info:title']

#查看他最近的阅读
for j in [i for i in train_data_matrix[userid_index['16380']] if i != 0]:
    rowkey = index_item[j]
    title = get_title(rowkey)
    print rowkey,"\t",title

#查找最相近的用户
ret = []
for i,j in enumerate(user_similarity[u_index]):
    ret.append([i,j])
user_df = pd.DataFrame(ret)
user_df.columns = ['index','score']
user_df_ = user_df.sort_values('score',ascending=True)

user_df_fil = user_df_.head(100)
user_df_fil['user_id'] = user_df_fil.apply(lambda x:index_userid[x[0]],axis=1)
user_df_fil.to_csv("/home/eos/user_df_fil.csv",encoding="utf-8")

#查看共同点击的新闻
user_index = 818
ret = []
for i,j in enumerate(train_data_matrix[u_index]):
    if train_data_matrix[user_index][i] >0 and j > 0:
        ret.append(i)
for i in ret:
    rowkey = index_item[i]
    title = get_title(rowkey)
    print rowkey,title

# from sklearn.metrics import mean_squared_error
# from math import sqrt
# def rmse(prediction, ground_truth):
#     prediction = prediction[ground_truth.nonzero()].flatten()
#     ground_truth = ground_truth[ground_truth.nonzero()].flatten()
#     return sqrt(mean_squared_error(prediction, ground_truth))
#
# print 'User-based CF RMSE: ' + str(rmse(user_prediction, test_data_matrix))
# print 'Item-based CF RMSE: ' + str(rmse(item_prediction, test_data_matrix))