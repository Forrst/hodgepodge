#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-09-19 上午10:51
'''
from bank.db.mysql import Mysql
import pandas as pd

db5_106 = Mysql("mysql5.106")



# Ambiguous
ambiguous = db5_106.execute("select keyword from ambiguous","trade_data")
pd.DataFrame(list(ambiguous),columns=['keyword']).to_csv("ambiguous.csv",encoding="utf8")

#Financial
financial = db5_106.execute("select keyword from financial","trade_data")
pd.DataFrame(list(financial),columns=['keyword']).to_csv("financial.csv",encoding="utf8")

#positive
positive = db5_106.execute("select keyword from positive","trade_data")
pd.DataFrame(list(positive),columns=['keyword']).to_csv("positive.csv",encoding="utf8")


#negative
negative = db5_106.execute("select keyword from negative","trade_data")
pd.DataFrame(list(negative),columns=['keyword']).to_csv("negative.csv",encoding="utf8")


#news_tag_word
news_tag_word = db5_106.execute("select exchange,name,synonyms from news_tag_word","app_data")
pd.DataFrame(list(news_tag_word),columns=['exchange','name','synonyms']).to_csv("negative.csv",encoding="utf8")


#concept_hk
concept_hk = db5_106.execute("select name,alias,synonyms from concept_hk","app_data")
pd.DataFrame(list(concept_hk),columns=['name','alias','synonyms']).to_csv("concept_hk.csv",encoding="utf8")


#concept
concept = db5_106.execute("select name,alias,synonyms from concept","app_data")
pd.DataFrame(list(concept),columns=['name','alias','synonyms']).to_csv("concept.csv",encoding="utf8")


#sentimentwords
sentimentwords = db5_106.execute("select keyword,sentiment_score from sentimentwords","trade_data")
pd.DataFrame(list(sentimentwords),columns=['keyword','sentiment_score']).to_csv("sentimentwords.csv",encoding="utf8")

#industry
industry = db5_106.execute("select name,synonyms from industry","app_data")
pd.DataFrame(list(industry),columns=['name','synonyms']).to_csv("industry.csv",encoding="utf8")


#industry_hk
industry_hk = db5_106.execute("select name,synonyms from industry_hk","app_data")
pd.DataFrame(list(industry_hk),columns=['name','synonyms']).to_csv("industry_hk.csv",encoding="utf8")

#region
region = db5_106.execute("select name, related_stocks from region","app_data")
pd.DataFrame(list(region),columns=['name','related_stocks']).to_csv("region.csv",encoding="utf8")

#china concept stock
china_concept_stock = db5_106.execute("select code,name,chname,synonyms,type,te_flag from us_data.stock_info_us Left JOIN china_concept_stock on stock_info_us.exchange = china_concept_stock.stock_exchange and stock_info_us.`code` = china_concept_stock.stock_code","us_data")
pd.DataFrame(list(china_concept_stock),columns=['code','name','chname','synonyms','type','te_flag']).to_csv("china_concept_stock.csv",encoding="utf8")



#stock_info
stock_info = db5_106.execute("select exchange,code,name,synonyms,type from stock_info","app_data")
pd.DataFrame(list(stock_info),columns=['exchange','code','name','synonyms','type']).to_csv("stock_info.csv",encoding="utf8")


#stock_info_hk
stock_info_hk = db5_106.execute("select exchange,code,name,synonyms,type from stock_info_hk","app_data")
pd.DataFrame(list(stock_info_hk),columns=['exchange','code','name','synonyms','type']).to_csv("stock_info_hk.csv",encoding="utf8")


#macro
macro = db5_106.execute("select name,synonyms from macro","app_data")
pd.DataFrame(list(macro),columns=['name','synonyms']).to_csv("macro.csv",encoding="utf8")


#stop_word
stop_word = db5_106.execute("select keyword from stop_words","trade_data")
pd.DataFrame(list(stop_word),columns=['keyword']).to_csv("stop_word.csv",encoding="utf8")


#stock_info_us
stock_info_us = db5_106.execute("select exchange,code,name,chname,synonyms,type from stock_info_us","us_data")
pd.DataFrame(list(stock_info_us),columns=['exchange','code','name','chname','synonyms','type']).to_csv("stock_info_us.csv",encoding="utf8")


#b
b = db5_106.execute("select name from b","app_data")
pd.DataFrame(list(b),columns=['name']).to_csv("b.csv",encoding="utf8")

#beifeng
beifeng = db5_106.execute("select name from beifen","app_data")
pd.DataFrame(list(beifeng),columns=['name']).to_csv("beifeng.csv",encoding="utf8")


#beifeng1
beifeng1 = db5_106.execute("select name from beifen1","app_data")
pd.DataFrame(list(beifeng1),columns=['name']).to_csv("beifeng1.csv",encoding="utf8")

