#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-03-28 下午4:33
'''
import re
import os
os.chdir("/home/eos/git/Mr.Jaryn")
from db.mysql.SqlUtil import Mysql

db = Mysql("mysql2.231")
# ret = []
# for year in range(2008,2019):
#     # year = 2018
#     sql = "select id,content,date from finra where date like '%{}%'".format(year)
#     ret_ = db.execute(sql,"app_data")
#     for j in ret_:
#         ret.append(j)
year = 2018
#regex = re.compile(".*?(?=FINRA Case)FINRA Case.*?(?=\r\n)",re.DOTALL)
sql = "select id,content,date from finra where date like '%{}%'".format(year)
ret = db.execute(sql,"app_data")
regex = re.compile('(?=CRD.*\)\s{0,}\r\n).*?(?=\(FINRA)',re.DOTALL)
case_regex = re.compile("(?=\)\r\n|\)\s\r\n).*",re.DOTALL)

filter_regex = re.compile("Disciplinary[\t| ]and[\t| ]Other[\t| ]FINRA[\t| ]Actions[\t| ].*?\d{4}",re.DOTALL)


crd_regex = re.compile("CRD.*\d{1,}")
fira_regex = re.compile("FINRA Case.*\d{5,}",re.DOTALL)


usefull_sentence = [u"Without admitting or denying the findings,"]
usefull_sentence.append(u"the firm consented to the sanctions and to the entry of findings that")
usefull_sentence.append(u"consented to the sanction and to the entry of findings that she ")
usefull_sentence.append(u"consented to the sanction and to the entry of findings that he ")
usefull_sentence.append(u"consented to the sanctions and to the entry of findings that he ")
usefull_sentence.append(u"consented to the sanctions and to the entry of findings that she ")
def isFindings(sentence):
    if "findings" in sentence.lower() or "finra rules" in sentence.lower():
        return True
    else:
        return False

def filter_usefull_sentence(text):
    s = text
    for i in usefull_sentence:
        s = s.replace(i,"")
    return s

def getCase(monthly_text):
    cases_ = []
    cases = regex.findall(monthly_text)
    for case in cases:
        case_context = ""
        t_case = case_regex.findall(case)
        if len(t_case)==0:
            continue
        else:
            case_context = t_case[0]
        c = "CRD"
        # f = "FINRA Case"
        crd = crd_regex.findall(case)
        # fira = fira_regex.findall(case)
        if len(crd) >0:
            c = crd[0]
        # if len(fira)>0:
        #     f = fira[0]
        # findings = []
        # sentences = case.split(".")
        # for sentence in sentences:
        #     if isFindings(sentence):
        #         findings.append(sentence.replace("\r\n",u"").replace("\t",u"").replace(c,u"").replace(f,u"").replace(u"\xae",u""))
        # if len(findings)==0:
        #     continue
        #
        # context = ",".join(findings)
        # contex_ = filter_usefull_sentence(context)
        filter_string = filter_regex.findall(case_context)
        if len(filter_string)>0:
            case_context = case_context.replace(filter_string[0],"")
        context_ = case_context.replace(")\r\n","").replace("\r\n","")
        context_ = context_.lower()
        context_true = context_
        consent_1 = "the findings stated that"
        content_2 = "consented to the sanctions and to the entry of findings that"
        content_3 = "consented to the sanction and to the entry of findings that"
        end_1 = "the suspension is in effect from"
        flag = False
        if consent_1 in context_:
            index = context_.index(consent_1)
            index = index+25
            context_ = context_[index:]
            flag = True
        if content_2 in context_:
            index = context_.index(content_2)
            index = index+61
            context_ = context_[index:]
            flag = True
        if content_3 in context_:
            index = context_.index(content_3)
            index = index+60
            context_ = context_[index:]
            flag = True
        if end_1 in context_:
            index = context_.index(end_1)
            context_ = context_[:index]
        if not flag:
            print c,context_
            continue
        context_final = context_.replace("the findings also stated that","")
        cases_.append([c,context_final.strip()])
    return cases_

records = []
for i in ret:
    finra_id = i[0]
    date = i[2]
    content = i[1]
    rcase = getCase(content)
    for j in rcase:
        crds =j[0]
        # firas = j[1]
        # findings = j[1]
        case = j[1]
        records.append([finra_id,date,crds,case])

db.executeMany("insert into finra_case",["finra_id","date","crd","findings"],records)

import jieba
from sklearn.feature_extraction.text import  TfidfVectorizer
from sklearn.cluster import KMeans

text_list = [i[3] for i in records]

def jieba_tokenize(text):
    return jieba.lcut(text)

tfidf_vectorizer = TfidfVectorizer(tokenizer=jieba_tokenize, lowercase=False)
tfidf_matrix = tfidf_vectorizer.fit_transform(text_list)
num_clusters = 30

from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import pandas as pd
SSE = []
Scores = []  # 存放轮廓系数
for k in range(50,70):
    estimator = KMeans(n_clusters=k, max_iter=300, n_init=40, init='k-means++',n_jobs=-1)
    estimator.fit(tfidf_matrix)
    SSE.append(estimator.inertia_)
    Scores.append(silhouette_score(tfidf_matrix,estimator.labels_,metric='euclidean'))

X = range(2,50)
plt.xlabel('k')
plt.ylabel('SSE')
plt.plot(X,SSE,'o-')
plt.show()

X = range(50,70)
plt.xlabel('k')
plt.ylabel('silhouette coeffient')
plt.plot(X,Scores,'o-')
plt.show()


km_cluster = KMeans(n_clusters=68, max_iter=300, n_init=40, init='k-means++',n_jobs=-1)
result = km_cluster.fit_predict(tfidf_matrix)
result_dict = {}
for i,j in enumerate(result):
    result_dict[i] = j

df = pd.DataFrame(records,columns= ['finra_id','date','crd_id','content'])
df['classification'] = df.index.map(lambda x:result_dict[x])
df.to_csv("/home/eos/data/finra/content_kmeans_68.csv",encoding='utf-8')


# coding=UTF-8
import nltk
from nltk.corpus import brown

# This is a fast and simple noun phrase extractor (based on NLTK)
# Feel free to use it, just keep a link back to this post
# http://thetokenizer.com/2013/05/09/efficient-way-to-extract-the-main-topics-of-a-sentence/
# Create by Shlomi Babluki
# May, 2013


# This is our fast Part of Speech tagger
#############################################################################
brown_train = brown.tagged_sents(categories='news')
regexp_tagger = nltk.RegexpTagger(
    [(r'^-?[0-9]+(.[0-9]+)?$', 'CD'),
     (r'(-|:|;)$', ':'),
     (r'\'*$', 'MD'),
     (r'(The|the|A|a|An|an)$', 'AT'),
     (r'.*able$', 'JJ'),
     (r'^[A-Z].*$', 'NNP'),
     (r'.*ness$', 'NN'),
     (r'.*ly$', 'RB'),
     (r'.*s$', 'NNS'),
     (r'.*ing$', 'VBG'),
     (r'.*ed$', 'VBD'),
     (r'.*', 'NN')
     ])
unigram_tagger = nltk.UnigramTagger(brown_train, backoff=regexp_tagger)
bigram_tagger = nltk.BigramTagger(brown_train, backoff=unigram_tagger)
#############################################################################


# This is our semi-CFG; Extend it according to your own needs
#############################################################################
cfg = {}
cfg["NNP+NNP"] = "NNP"
cfg["NN+NN"] = "NNI"
cfg["NNI+NN"] = "NNI"
cfg["JJ+JJ"] = "JJ"
cfg["JJ+NN"] = "NNI"


#############################################################################


class NPExtractor(object):
    def __init__(self, sentence):
        self.sentence = sentence

    # Split the sentence into singlw words/tokens
    def tokenize_sentence(self, sentence):
        tokens = nltk.word_tokenize(sentence)
        return tokens

    # Normalize brown corpus' tags ("NN", "NN-PL", "NNS" > "NN")
    def normalize_tags(self, tagged):
        n_tagged = []
        for t in tagged:
            if t[1] == "NP-TL" or t[1] == "NP":
                n_tagged.append((t[0], "NNP"))
                continue
            if t[1].endswith("-TL"):
                n_tagged.append((t[0], t[1][:-3]))
                continue
            if t[1].endswith("S"):
                n_tagged.append((t[0], t[1][:-1]))
                continue
            n_tagged.append((t[0], t[1]))
        return n_tagged

    # Extract the main topics from the sentence
    def extract(self):

        tokens = self.tokenize_sentence(self.sentence)
        tags = self.normalize_tags(bigram_tagger.tag(tokens))

        merge = True
        while merge:
            merge = False
            for x in range(0, len(tags) - 1):
                t1 = tags[x]
                t2 = tags[x + 1]
                key = "%s+%s" % (t1[1], t2[1])
                value = cfg.get(key, '')
                if value:
                    merge = True
                    tags.pop(x)
                    tags.pop(x)
                    match = "%s %s" % (t1[0], t2[0])
                    pos = value
                    tags.insert(x, (match, pos))
                    break

        matches = []
        for t in tags:
            if t[1] == "NNP" or t[1] == "NNI":
                # if t[1] == "NNP" or t[1] == "NNI" or t[1] == "NN":
                matches.append(t[0])
        return matches


# Main method, just run "python np_extractor.py"
def main():
    sentence = "Swayy is a beautiful new dashboard for discovering and curating online content."
    np_extractor = NPExtractor(sentence)
    result = np_extractor.extract()
    print("This sentence is about: %s" % ", ".join(result))


if __name__ == '__main__':
    main()