#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-01-11 上午9:34
'''
import happybase

rowkey = "481c7ffffe97d816e870a114974d0000"
con = happybase.Connection("192.168.5.156")
con.open()
table = con.table("news")
ret = table.row(rowkey.decode("hex"))
con.close()
for i in ret:
    print i,ret[i]
import os
os.chdir("/home/eos/git/Mr.Jaryn/")
from db.mysql.SqlUtil import Mysql

db = Mysql("mysql5.106")
news = db.execute("select id,title,summary,rowkey,info_type from news where news_time>'2019-03-11' and info_type = 1 order by news_time desc","app_data")


title = "银保监会周亮：降低担保成本也要减少银行对担保的依赖"
content = '''3月11日，银保监会副主席周亮在政协经济界别联组讨论会后接受记者采访时表示，尽量要把担保成本降低，目前国家成立的融资担保公司已经在助力降低成本。他强调，更重要的是，以后银行信贷融资要更多地看企业本身信用、第一还款能力，而不能过多依赖担保机构，不能因为有了担保就不审核企业真实流动性、企业效益、产品市场前景，这样才能从根本上更大程度地减少对担保的依赖。担保机构也要按照市场化收费，尽量减少对实体经济的负担，这是监管政策的导向。'''
context = title+"\t"+content
print "".join(jieba.cut(context))

from jieba import posseg
pos = posseg.cut(context,HMM=True)
for i in pos:
    print i.word+"/"+i.flag,

