#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-09-30 下午3:19
'''

# import NewsSelector
import re
from hashlib import md5
from collections import defaultdict
import numpy as np
import jieba
jieba.load_userdict("")

# one = table.row("58f17ffffe9a2a2133c2ea57401b0000".decode("hex"))


class word:
    def __init__(self):
        self.text = ""
        self.left
        self.right
        self.frequency
        self.entropy
        self.left_entropy
        self.right_entropy
        self.aggregation
    def word(self,text):
        self.text=text
    def increaseFrequency(self):
        pass
    def computeEntropy(self):
        pass
    def computeProbabilityEntropy(self):
        pass
    def computeAggregation(self):
        pass


def texts(news):
    texts_set = set()
    for text in news:
        if not isinstance(news_list[0],unicode):
            text = text.decode("utf-8")
        if md5(text.encode('utf-8')) in texts_set:
            continue
        else:
            texts_set.add(md5(text.encode('utf-8')))
        for t in re.split(u'[^\u4e00-\u9fa50-9a-zA-Z|%]+', text):
            if t:
                yield t
    print u'原始文章总数%s,最终计算了%s篇文章'%(len(news),len(texts_set))

if __name__=="__main__":
    # con = happybase.Connection("192.168.5.151")
    # con.open()
    # table = con.table("news")
    # cnNewsSelector = NewsSelector.newsFilter(server='mysql5.106', start="2018-10-01 00:00:00", end="2018-10-08 00:00:00")
    # cnNewsSelector.loadRowkeys()
    # news = cnNewsSelector.news
    # texts_set = set()
    news_list = open("/home/eos/data/other/title",'r').read().split("\n")
    # for i in news:
    #     rowkey = i[1]
    #     newsTable = table.row(rowkey.decode("hex"))
    #     title = newsTable['info:title']
    #     content = newsTable['info:title']
    #     context = title+"\n"+content
    #     news_list.append([context])
    # lines = texts(news_list)
    import pandas as pd
    import math
    n = 4
    min_count = 128
    ngrams = defaultdict(int)
    #获取词频信息
    for t in texts(news_list):
        for i in range(len(t)):
            for j in range(1, n+1):
                if i+j <= len(t):
                    word = t[i:i+j]
                    ngrams[word] += 1
# words_count_list = [j for i,j in ngrams.iteritems() if j>1]
# mean = 1.*sum(words_count_list)/len(words_count_list)
# df = pd.DataFrame.from_dict(ngrams,'index')
# df.columns = ['count']
# dfdesc = df.sort_values('count',ascending=False)
    #过滤词频阈值低的词
    ngrams = {i:j for i,j in ngrams.iteritems() if j >= min_count}
    #统计单个词的个数
    total = 1.*sum([j for i,j in ngrams.iteritems() if len(i) == 1])
    #设定2个词，3个词,4个词对应的凝固度p(ab)/(p(a)*p(b))的值
    min_proba = {2:5, 3:25, 4:125}

    def is_keep(s, min_proba):
        if len(s) >= 2:
            score = min([total*ngrams[s]/(ngrams[s[:i+1]]*ngrams[s[i+1:]]) for i in range(len(s)-1)])
            if score > min_proba[len(s)]:
                return True
        else:
            return False
    #过滤低于凝固度的词
    ngrams_ = set(i for i,j in ngrams.iteritems() if is_keep(i, min_proba))

    #单句的切分
    def cut(s):
        r = np.array([0]*(len(s)-1))
        for i in range(len(s)-1):
            for j in range(2, n+1):
                if s[i:i+j] in ngrams_:
                    r[i:i+j-1] += 1
        w = [s[0]]
        for i in range(1, len(s)):
            if r[i-1] > 0:
                w[-1] += s[i]
            else:
                w.append(s[i])
        return w

    #建立新词的词库
    words = defaultdict(int)
    for t in texts(news_list):
        for i in cut(t):
            wor = i
            words[i] += 1
    #过滤低于词频阈值的词
    words = {i:j for i,j in words.iteritems() if j >= min_count}

    def is_real(s):
        if len(s) >= 3:
            for i in range(3, n+1):
                for j in range(len(s)-i+1):
                    if s[j:j+i] not in ngrams_:
                        return False
            return True
        else:
            return True

    w = {i:j for i,j in words.iteritems() if is_real(i)}
###################################################################for words
import jieba
jieba.load_userdict("/home/eos/data/dictionary/dictionary")
def texts(news):
    texts_set = set()
    for text in news:
        if not isinstance(news_list[0],unicode):
            text = text.decode("utf-8")
        if md5(text.encode('utf-8')) in texts_set:
            continue
        else:
            texts_set.add(md5(text.encode('utf-8')))
        for t in re.split(u'[^\u4e00-\u9fa50-9a-zA-Z]+', text):

            if t:
                seg = jieba.cut(t)
                wordList = [i for i in seg]
                yield wordList
    print u'原始文章总数%s,最终计算了%s篇文章'%(len(news),len(texts_set))

n = 4
min_count = 128
ngrams = defaultdict(int)
#获取词频信息
news_list = open("/home/eos/data/other/title",'r').read().split("\n")
for t in texts(news_list):
    for i in range(len(t)):
        for j in range(1, n+1):
            if i+j <= len(t):
                word = t[i:i+j]
                ngrams[word] += 1
# words_count_list = [j for i,j in ngrams.iteritems() if j>1]
# mean = 1.*sum(words_count_list)/len(words_count_list)
# df = pd.DataFrame.from_dict(ngrams,'index')
# df.columns = ['count']
# dfdesc = df.sort_values('count',ascending=False)
#过滤词频阈值低的词
ngrams = {i:j for i,j in ngrams.iteritems() if j >= min_count}
#统计单个词的个数
total = 1.*sum([j for i,j in ngrams.iteritems() if len(i) == 1])
#设定2个词，3个词,4个词对应的凝固度p(ab)/(p(a)*p(b))的值
min_proba = {2:5, 3:25, 4:125}

def is_keep(s, min_proba):
    if len(s) >= 2:
        score = min([total*ngrams[s]/(ngrams[s[:i+1]]*ngrams[s[i+1:]]) for i in range(len(s)-1)])
        if score > min_proba[len(s)]:
            return True
    else:
        return False
#过滤低于凝固度的词
ngrams_ = set(i for i,j in ngrams.iteritems() if is_keep(i, min_proba))

#单句的切分
def cut(s):
    r = np.array([0]*(len(s)-1))
    for i in range(len(s)-1):
        for j in range(2, n+1):
            if s[i:i+j] in ngrams_:
                r[i:i+j-1] += 1
    w = [s[0]]
    for i in range(1, len(s)):
        if r[i-1] > 0:
            w[-1] += s[i]
        else:
            w.append(s[i])
    return w

#建立新词的词库
words = defaultdict(int)
for t in texts(news_list):
    for i in cut(t):
        wor = i
        words[i] += 1
#过滤低于词频阈值的词
words = {i:j for i,j in words.iteritems() if j >= min_count}

def is_real(s):
    if len(s) >= 3:
        for i in range(3, n+1):
            for j in range(len(s)-i+1):
                if s[j:j+i] not in ngrams_:
                    return False
        return True
    else:
        return True

w = {i:j for i,j in words.iteritems() if is_real(i)}
