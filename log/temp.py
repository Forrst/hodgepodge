import pandas as pd
from io import StringIO
from sklearn import linear_model
import matplotlib.pyplot as plt
import numpy as np
lines = open("/home/eos/下载/pdf/temp","r+").read().split("\n")
lines.remove("")
a = []
b = []
for i in lines:
    l = i.split("\t")
    a.append(float(l[0]))
    b.append(float(l[1]))



# 建立线性回归模型
a = np.asarray(a)
b = np.asarray(b)
regr = linear_model.LinearRegression()

# 拟合
regr.fit(a.reshape(-1, 1), b)
 # 注意此处.reshape(-1, 1)，因为X是一维的！

# 不难得到直线的斜率、截距
m, n = regr.coef_, regr.intercept_

# 1.真实的点
plt.scatter(a, b, color='blue')

plt.show()

print "y = ax+b :"+"a="+str(m)+",b="+str(n)

import MySQLdb
con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select * from mazhan_data"
cursor.execute(sql)
mazhan = cursor.fetchall()
cursor.close()
con.close()

con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='trade_data',charset='utf8')
cursor = con.cursor()
sql = "select * from hk_new_stock_extra"
cursor.execute(sql)
extra = cursor.fetchall()
cursor.close()
con.close()

mzmpower = {}
for i in mazhan:
    mzmpower[i[2]] = i[4]

power = {}
price = {}        
for i in extra:
    if i[5] == 'N/A' or i[4] == 'N/A':
        continue
    try:
        power[i[0]] = float(i[5])+1
        price[i[0]] = float(i[4])
    except Exception,e:
        print e
        power[i[0]] = i[5]
        price[i[0]] = i[4]
        print i[5]

for i in mzmpower.keys():
    try:
        print i,mzmpower[i],power[i],price[i]
    except KeyError,e:
        print ""
        continue
        
        
import MySQLdb
import re
con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select * from hk_distribution"
cursor.execute(sql)
distribution = cursor.fetchall()
cursor.close()
con.close()

re.search()
ret = []
for i in distribution:
    ret.append(i[6])
p1 = re.compile(u"[一|兩|二|三|四|五|六|七|八|九|十|\d]{1,}\）{0,1}份")
p2 = re.compile(u"[一|兩|二|三|四|五|六|七|八|九|十|\d]{1,}\）{0,1}項")
p3 = re.compile(u"[一|兩|二|三|四|五|六|七|八|九|十|\d]{1,}\）{0,1}個")


for i in ret:
    matched1 = p1.findall(i)
    matched2 = p2.findall(i)
    matched3 = p3.findall(i)
    print i
    try:
        if len(matched1)>0:
            print matched1[0]
        elif len(matched2)>0:
            print matched2[0]
        elif len(matched3)>0:
            print matched3[0]
    except Exception,e:
        print e

