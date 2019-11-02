#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-09-18 下午3:57
'''


import pandas as pd
import logging
from db.mysql.SqlUtil import Mysql
from collections import namedtuple

db = Mysql("mysql5.105")
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("withdraw_compute.py")

train_start = '20190501'
train_end = '20190801'

#资产一直为0的用户不计算在内
import time
start = time.time()
all_accounts_sql = '''
SELECT
	a.account,
	net_assets,
	withdraw,
	stock_assets,
	info_date
FROM
	(
		SELECT
			account,
			net_assets,
			withdraw,
			stock_assets,
			info_date
		FROM
			trade_user_money
		WHERE
			info_date >= '{}'
		AND info_date < '{}'
	) a
LEFT JOIN (
	SELECT
		account
	FROM
		(
			SELECT
				account,
				count(*) AS times
			FROM
				trade_user_money
			WHERE
				net_assets = withdraw
			AND withdraw = 0
			AND info_date >= '{}'
			AND info_date < '{}'
			GROUP BY
				account
			HAVING
				times = 42
		) b
) c ON a.account = c.account
WHERE
	c.account IS NULL
ORDER BY
	account,
	info_date ASC
'''.format(train_start,train_end,train_start,train_end)
accounts_all = db.execute(all_accounts_sql,"miningtrade")
end = time.time()
logger.info("获取训练数据：起始时间{}\t截止时间{}\t 数据库中总共取出的有效用户记录数为{}，花费{}秒!".format(len(accounts_all),end-start))


#形成dataframe
account_info = []
for i in accounts_all:
    account = i[0]
    net_assets = i[1]
    withdraw = i[2]
    stock_assets = i[3]
    info_date = i[4]
    account_info.append([account,net_assets,withdraw,stock_assets,info_date])
dfna = pd.DataFrame(account_info,columns=['account','net_assets','withdraw','stock_assets','info_date'])

#消除空值用上一条记录取代下一条的方式
df = dfna.fillna(method='ffill')
# dateList = sorted(df.info_date.unique())


def myiter(d, cols=None):
    if cols is None:
        v = d.values.tolist()
        cols = d.columns.values.tolist()
    else:
        j = [d.columns.get_loc(c) for c in cols]
        v = d.values[:, j].tolist()

    n = namedtuple('MyTuple', cols)

    for line in iter(v):
        yield n(*line)


def getAccountInfo(account):
    #计算用户为以下三种取值的个数

    data = df[df.account==account]
    #账户总资产为余额并且没有出金的个数
    account_positive = 0

    #账户总资产为余额并且部分出金的个数
    account_neutral = 0

    #账户总资产为余额全部出金的个数
    account_negative = 0
    for index,line in enumerate(myiter(data)):
        if index == 0:
            last_line = line
            continue
        if last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets == line.withdraw and line.withdraw==0.0:
            account_negative+=1
        elif last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets == line.withdraw and line.withdraw !=0.0 and line.withdraw < last_line.withdraw:
            account_neutral+=1
        elif last_line.net_assets == last_line.withdraw and last_line.withdraw != 0.0 and line.net_assets != line.withdraw:
            account_positive+=1
        last_line = line
    return [account,account_positive,account_neutral,account_negative]

account_list = df.account.unique().tolist()
start = time.time()
account_info = map(getAccountInfo,account_list)
end = time.time()
logger.info("计算总计{}用户的A B C类型的个数花费的时间为{}秒!".format(len(account_list),end-start))

account_info = pd.DataFrame(account_info,columns=['account','account_positive','account_neutral','account_negative'])

withdraw_info = pd.read_csv("/home/eos/data/train_x.csv",index_col=0)

withdraw_info_y = pd.read_csv("/home/eos/data/train_y.csv",index_col=0)

def normalization(x):
    if x>0:
        return 1
    else:
        return 0

withdraw_info['y'] = withdraw_info_y.apply(lambda x:normalization(x['account_negative']),axis=1)

x = withdraw_info[['account_positive','account_neutral','account_negative']]
y = withdraw_info['y']
X = x.values
Y = y.values
from sklearn.naive_bayes import GaussianNB
clf = GaussianNB()
clf.fit(X, y)

print X[2],clf.predict([X[2]])


def normalization(x):
    if x>0:
        return 1
    else:
        return 0
import pandas as pd

dfx = pd.read_csv("/home/eos/data/withdraw/data567/withdraw_info_56.csv",index_col=0)
dfx = dfx.sort_values(['account'],ascending=True)
dfx = dfx.drop([14513])
dfx['total'] = dfx['account_positive'] + dfx['account_neutral'] + dfx['account_negative']
dfx_true = dfx[dfx['total']>0]


dfy = pd.read_csv("/home/eos/data/withdraw/data567/withdraw_train_7.csv",index_col=0)
dfy = dfy.sort_values(['account'],ascending=True)
dfy['y'] = dfy.apply(lambda x:normalization(x['account_negative']),axis=1)
dfy['total'] = dfy['account_positive'] + dfy['account_neutral'] + dfy['account_negative']


dftest = pd.read_csv("/home/eos/data/withdraw/data567/withdraw_test_8.csv",index_col=0)
dftest['y'] = dftest.apply(lambda x:normalization(x['account_negative']),axis=1)
dftest = dftest.sort_values(['account'],ascending=True)


dfmerge = pd.merge(dfx_true,dfy,how="inner",on='account',left_index=True)



train_x = dfx_true[['account_positive','account_neutral','account_negative']].values
train_y = dfmerge[['y']].values


#计算真实值
dfy_true = dfy[dfy['total']>0]
dftest_merge = pd.merge(dfy_true,dftest,how="inner",on='account',left_index=True)
true_x = dfy_true[['account_positive','account_neutral','account_negative']].values
true_y = dftest_merge['y_y'].values


from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score

MNB = GaussianNB()
MNB.fit(train_x,train_y)
predict_y = MNB.predict(true_x)

print(accuracy_score(true_y, predict_y))

