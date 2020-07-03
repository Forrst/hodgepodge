#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2019-02-14 上午11:10
'''

import re
import jieba
import pandas as pd
def split_context(context):
    text = context
    if not isinstance(context,unicode):
        text = context.decode("utf-8")
    sentences = re.split(u'[^\u4e00-\u9fa50-9a-zA-Z]+', text)
    return sentences
gram_max_length = 4


class term:
    def __init__(self):
        self.word_name = ""
        self.word_count = 0
        self.word_right = {}
        self.word_left = {}
    def toString(self):
        word_right_str = ""
        for i in self.word_right:
            word_right_str+="{}:{}".format(i,self.word_right[i])+" "
        return "word_name:{}\tword_count:{}\tword_right:{}".format(self.word_name,self.word_count,word_right_str)
import happybase
con = happybase.Connection("192.168.5.156")
con.open()
table = con.table("news")

r= table.row('6bda7ffffe8e7d42abc8e7f838580000'.decode("hex"))
for i in r:
    print(i,r[i])
title = r['info:title']
content = r['info:content']
context = title+"\t"+content

sentences = split_context(context)
term_dict = {}

for sentence in sentences:
    if sentence.strip() == "":
        continue
    words = jieba.cut(sentence)
    words = [word for word in words]
    if words[0] == sentence:
        continue
    for i in xrange(len(words)):
        word_name = words[i].encode("utf8")
        tm = term_dict[word_name] if word_name in term_dict else term()
        tm.word_name = word_name
        tm.word_count = term_dict[tm.word_name].word_count+1 if tm.word_name in term_dict else 1
        term_dict[tm.word_name] = tm
        for j in xrange(min(len(words)-i,gram_max_length)):
            new_word = "".join(words[i:i+j+1]).encode("utf8")
            right_dict = term_dict[word_name].word_right if word_name in term_dict else {}
            right_dict[new_word] = right_dict[new_word]+1 if new_word in right_dict else 1
            term_dict[tm.word_name].word_right = right_dict
            left_index = max(i-j-1,0)
            left_dict = term_dict[word_name].word_left if word_name in term_dict else {}
            left_dict[new_word] = left_dict[new_word]+1 if new_word in left_dict else 1
            term_dict[tm.word_name].word_left = left_dict