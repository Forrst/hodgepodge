#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-10-19 上午9:13
'''
from bank.db.mysql import Mysql

mysql5_106 = Mysql("mysql5.106")
rules = mysql5_106.execute("select * from news_title_rules_by_time",'app_data')
classification = mysql5_106.execute("select * from news_time_classification",'app_data')
classification_dict = {}
for i in classification:
    code = i[1]
    type = i[3]
    classification_dict[code] = type


# for i in rules:
#     code = i[1]
#     if classification_dict[code]
#     type = 6
#     news_time_start
#     news_time_stop
#     title_absolute_rule
#     title_keywords
#     action
#     title_not_contain