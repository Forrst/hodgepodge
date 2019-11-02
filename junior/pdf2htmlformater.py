#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-08-08 下午12:26
'''
import logging
import os
import re

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as bsoup


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger = logging.getLogger("[pdf2html.py]")
# os.chdir("/root/jzhou/pdf2html")
# htmlpath = "/apps/www/brokerhtml/"

os.chdir("/home/eos/git/hodgepodge")
htmlpath = "/home/eos/data/pdf2html/data/"

from db.mysql.SqlUtil import Mysql
db = Mysql("mysql2.231")
sep = " "

# reformatHtml = "/home/eos/data/pdf2html/firm_7059_python.html"


topregex = re.compile("(?<=top:).*?(?=pt)")
leftregex = re.compile("(?<=left:).*?(?=pt)")
selfregulatoryregex = re.compile(".*?(?=\s)\s[0-1][0-9]/[0-3][0-9]/\d{4}")
businessregex = re.compile("(?<=This firm currently conducts )\d{1,}?(?= types of businesses.)")
disclosureregex = re.compile("Disclosure \d{1,} of \d{1,}")
numberregex = re.compile("\d{1,}")

def save_clearing_arrangements(pages,crd):
    flag = False
    over_flag = False
    block = [crd]
    ret = []
    for page in pages[4:]:
        divs = page.findAll("div")
        lrtext = ""
        try:
            lrtext = get_datarame(divs)
        except Exception,e:
            continue
        for i in range(len(lrtext)):
            x = lrtext.ix[i,'text_x']
            y = lrtext.ix[i,'text_y']
            if x == u"Clearing Arrangements":
                flag = True
            if x == u"Industry Arrangements" or x ==u"Organization Affiliates":
                flag = False
                over_flag = True
                break
            if flag == True:
                if x == u"Name:":
                    block.append(y)
                if x == u'CRD #:':
                    block.append(y)
                if x == u'Business Address:':
                    block.append(y)
                if x == u'Effective Date:':
                    block.append(y)
                if x == u'Description:' and len(block)==5:
                    block.append(y)
                    ret.append(block)
                    block = [crd]
        if over_flag == True:
            break
    return ret

def save_clearing_arrangements_tomysql(ret):
    db.executeMany("insert into brocker_introducing_arrangements ",columns=['crd','name','arrangement_crd','address','date','description'],data=ret,db="app_data")


def saveFile(filename, crd):
    def getpage(x):
        return get_datarame(pages[x + 1].findAll("div"))

    def savefile(x, doeo):
        df = getpage(x)
        header = df.ix[1, 'text_x']
        if header == "Direct Owners and Executive Officers" or header == "Direct Owners and Executive Officers (continued)":
            for i in range(len(df))[2:]:
                key = df.ix[i, 'text_x']
                value = df.ix[i, 'text_y']
                if key is not np.nan and value is not np.nan:
                    doeo.append([key, value])
        if header == "Indirect Owners" or header == "Indirect Owners (continued)":
            if len(index) == 0:
                index.append(len(doeo))
            for i in range(len(df))[2:]:
                key = df.ix[i, 'text_x']
                value = df.ix[i, 'text_y']
                if key is not np.nan and value is not np.nan:
                    doeo.append([key, value])
        if header == "Organization Affiliates" or header == "Organization Affiliates (continued)":
            if len(index) == 1:
                index.append(len(doeo))
            for i in range(len(df))[2:]:
                key = df.ix[i, 'text_x']
                value = df.ix[i, 'text_y']
                if key is not np.nan and value is not np.nan:
                    doeo.append([key, value])

    f = open(filename, "r")
    html = f.read()
    HTML = "<html "
    source_html = bsoup(html, 'lxml')
    html_tag = source_html.find("html").attrs
    HTML += "xmlns=" + html_tag['xmlns'] + ">\n"
    head = source_html.find("head")
    HTML += str(head) + "\n"
    HTML += "<body>"
    pages = source_html.findAll("div", {"class": "page"})
    doeo = []
    index = []

    for x in range(len(pages)):
        try:
            savefile(x + 1, doeo)
        except Exception, e:
            print e.message
    index_1 = []
    index_2 = []
    index_3 = []
    if len(index) == 1:
        index_1 = doeo[:index[0]]
        index_2 = doeo[index[0]:]
    if len(index) == 2:
        index_1 = doeo[:index[0]]
        index_2 = doeo[index[0]:index[1]]
        index_3 = doeo[index[1]:]

    if len(index_1) >= 1:
        key1 = "Legal Name & CRD# (if any):"
        key2 = "Is this a domestic or foreign entity or an individual?"
        key3 = "Position"
        key4 = "Position Start Date"
        key5 = "Percentage of Ownership"
        key6 = "Does this owner direct the management or policies of the firm?"
        key7 = "Is this a public reporting company?"

        ret = []
        k = []
        v = [crd]
        for i in index_1:
            if not i[0] in k:
                k.append(i[0])
                v.append(i[1])
            if len(k) == 7 and k == [key1, key2, key3, key4, key5, key6, key7]:
                ret.append(v)
                k = []
                v = [crd]
        db.executeMany(sql="insert into brocker_direct_owner",
                       columns=["crd", "legalname", "type", "position", "date", "percentage", "management", "public"],
                       data=ret,
                       db="app_data")

    if len(index_2) >= 1:
        key1 = "Legal Name & CRD# (if any):"
        key2 = "Is this a domestic or foreign entity or an individual?"
        key3 = "Company through which indirect ownership is established"
        key4 = "Relationship to Direct Owner"
        key5 = "Relationship Established"
        key6 = "Percentage of Ownership"
        key7 = "Does this owner direct the management or policies of the firm?"
        key8 = "Is this a public reporting company?"
        ret = []

        k = []
        v = [crd]
        for i in index_2:
            if not i[0] in k:
                k.append(i[0])
                v.append(i[1])
            if len(k) == 8 and k == [key1, key2, key3, key4, key5, key6, key7, key8]:
                ret.append(v)
                k = []
                v = [crd]
        db.executeMany(sql="insert into brocker_indirect_owner",
                       columns=["crd", "legalname", "type", "indirectway", "relationship", "date", "percentage",
                                "management",
                                "public"], data=ret, db="app_data")
    if len(index_3) >= 1:
        key1 = "Business Address:"
        key2 = "Effective Date:"
        key3 = "Foreign Entity:"
        key4 = "Country:"
        key5 = "Securities Activities:"
        key6 = "Investment Advisory Activities:"
        key7 = "Description:"
        ret = []
        k = []
        v = [crd]

        for i in index_3:
            if not i[0] in k:
                if i[0] == "Foreign Entity: Country:":
                    k.append("Foreign Entity:")
                    v.append(i[1])
                    k.append("Country:")
                    v.append("")
                else:
                    k.append(i[0])
                    v.append(i[1])
            if len(k) == 7 and k == [key1, key2, key3, key4, key5, key6, key7]:
                ret.append(v)
                k = []
                v = [crd]
            if len(k) == 7 and k != [key1, key2, key3, key4, key5, key6, key7]:
                k.pop(0)
                v.pop(1)
        db.executeMany(sql="insert into brocker_organization_affiliates",
                       columns=["crd", "address", "date", "foreign_entity", "country", "securities", "investment",
                                "description"], data=ret, db="app_data")


def get_datarame(divs):
    div_dict = {}
    div_id_dict = {}
    div_left_dict = {}
    div_width_dict = {}
    for div in divs:
        class_tag = div.attrs['class']
        if class_tag == ['r']:
            continue
        style = div.attrs['style']
        id = div.attrs['id']
        style_attrs = style.split(";")
        div_key_list = []
        left = ""
        width = ""
        for style_attr in style_attrs:
            if style_attr == "":
                continue
            style_attr_list = style_attr.split(":")
            attr = style_attr_list[0]
            value = style_attr_list[1]
            if attr == "left":
                left = value
            elif attr == "width":
                width = value
            else:
                div_key_list.append(style_attr)
        div_key_str = ";".join(div_key_list)
        if div_key_str not in div_dict:
            text = []
            text.append(div.text)
            div_dict[div_key_str] = text
            div_left_dict[div_key_str] = left
            div_width_dict[div_key_str] = width
            div_id_dict[div_key_str] = id
        else:
            # if div_dict[div_key_str] is None or div_key_str is None:
            #     print div_key_str
            div_value_list = div_dict[div_key_str]
            div_value_list.append(div.text)
            div_dict[div_key_str] = div_value_list
    div_att = []
    for key in div_dict:
        id_ = div_id_dict[key]
        key_ = key
        left_ = div_left_dict[key]
        width_ = div_width_dict[key]
        text_ = sep.join(div_dict[key])
        line = '''<div class="p" id="{}" style="{};left:{};width{};">{}</div>
            '''.format(id_, key_, left_, width_, text_.encode("utf-8"))
        top = float(topregex.findall(line)[0])
        left = float(left_[:-2])
        div_att.append([top, left, line])
    df = pd.DataFrame(div_att, columns=['top', 'left', 'line'])
    dfs = df.sort_values(by=['top', 'left'], ascending=True)
    dataframe = get_dataframe_from_divs(dfs)
    return dataframe


def get_self_regulator(pages):
    selfregulatory = []
    regestration = []
    business = []
    business_counter = 0
    self_regulatory_flag = False
    regestration_flag = False
    business_flag = False
    business_real_flag = False
    business_continue_flag = False
    r_tag_flag = False
    for page in pages:
        divs = page.findAll("div")
        div_dict = {}
        div_id_dict = {}
        div_left_dict = {}
        div_width_dict = {}
        div_class_dict = {}
        for div in divs:
            class_tag = div.attrs['class'][0]
            # if class_tag == ['r']:
            #     continue
            id = None
            style = div.attrs['style']
            if class_tag != "r":
                id = div.attrs['id']
            else:
                id = None
            style_attrs = style.split(";")
            div_key_list = []
            left = ""
            width = ""
            for style_attr in style_attrs:
                if style_attr == "":
                    continue
                style_attr_list = style_attr.split(":")
                attr = style_attr_list[0]
                value = style_attr_list[1]
                if attr == "left":
                    left = value
                elif attr == "width":
                    width = value
                else:
                    div_key_list.append(style_attr)
            div_key_str = ";".join(div_key_list)
            div_class_dict[div_key_str] = class_tag
            if div_key_str not in div_dict:
                text = []
                text.append(div.text)
                div_dict[div_key_str] = text
                div_left_dict[div_key_str] = left
                div_width_dict[div_key_str] = width
                div_id_dict[div_key_str] = id
            else:
                # if div_dict[div_key_str] is None or div_key_str is None:
                #     print div_key_str
                div_value_list = div_dict[div_key_str]
                div_value_list.append(div.text)
                div_dict[div_key_str] = div_value_list
        div_att = []
        for key in div_dict:
            id_ = div_id_dict[key]
            class_ = div_class_dict[key]
            key_ = key
            left_ = div_left_dict[key]
            width_ = div_width_dict[key]
            text_ = sep.join(div_dict[key])
            id_str = ""
            if id_ is not None:
                id_str = "id='{}'".format(id_)
            line = '''<div class="{}" {} style="{};left:{};width{};">{}</div>
                    '''.format(class_, id_str, key_, left_, width_, text_.encode("utf-8"))
            top = float(topregex.findall(line)[0])
            left = float(left_[:-2])
            div_att.append([top, left, line])
        df = pd.DataFrame(div_att, columns=['top', 'left', 'line'])
        dfs = df.sort_values(by=['top', 'left'], ascending=True)
        dfs.index = range(len(dfs))
        last_class_type = ""
        for i in range(len(dfs)):
            tagline = dfs.ix[i, 'line']
            taglinesoup = bsoup(tagline, 'lxml')
            class_type = taglinesoup.find("div").attrs['class'][0]
            if r_tag_flag and i > 0:
                taglastline = dfs.ix[i - 1, 'line']
                taglastlinesoup = bsoup(taglastline, 'lxml')
                last_class_type = taglastlinesoup.find("div").attrs['class'][0]
            if r_tag_flag and i < len(dfs) - 1:
                tagnextline = dfs.ix[i + 1, 'line']
                tagnextlinesoup = bsoup(tagnextline, 'lxml')
                next_class_type = tagnextlinesoup.find("div").attrs['class'][0]
            if r_tag_flag == False and class_type == "r":
                continue
            line = taglinesoup.text.strip()
            if business_continue_flag == True and len(business) < business_counter:
                business.append(line)
                continue
            if u'2019 FINRA. All rights reserved. Report about ' in line or line == u'www.finra.org/brokercheck User Guidance\n':
                continue
            if "Self-Regulatory Organization Status Date Effective" in line:
                self_regulatory_flag = True
                continue
            if self_regulatory_flag == True:
                records = selfregulatoryregex.findall(line)
                for record in records: selfregulatory.append(record.encode("utf-8").strip())
            if "U.S. States & Status Date Effective" in line:
                self_regulatory_flag = False
                regestration_flag = True
                continue
            if line == "Other Types of Business" or line == "ClearingArrangements":
                break
            if regestration_flag == True:
                records = selfregulatoryregex.findall(line)
                for record in records: regestration.append(record.encode("utf-8").strip())
            business_len = businessregex.findall(line)
            if len(business_len) > 0:
                business_len = int(business_len[0])
                business_counter = business_len
                business_flag = True
            if line == "Types of Business" and business_flag:
                business_real_flag = True
                r_tag_flag = True
                continue
            if business_real_flag == True and len(business) <= business_counter and class_type == "p":
                if last_class_type == "p":
                    tline = business[len(business) - 1]
                    tline += " " + line.encode("utf-8").strip()
                    business.pop(-1)
                    business.append(tline)
                else:
                    business.append(line.encode("utf-8").strip())
            if len(business) >= business_counter and len(business) != 0 and next_class_type == "r":
                break
        if len(business) < business_counter:
            business_continue_flag = True
        elif len(business) == business_counter and len(business) != 0:
            break
    return selfregulatory, regestration, business


def get_dataframe_from_divs(dfs):
    dfs.index = range(len(dfs))
    keyvalue = dfs.groupby('left').count().sort_values('top', ascending=False).top
    maxleft = keyvalue.max()
    leftseries = keyvalue / maxleft
    leftserie = list(leftseries[leftseries > 0.5].keys())
    if len(leftserie) < 2:
        leftserie.append(leftseries.index[1])
    leftserie.sort()
    leftdict = {}
    for k in range(len(dfs)):
        i = dfs.ix[k, :]
        toplast = i[0]
        if k > 0:
            toplast = dfs.ix[k - 1, :][0]
        top = i[0]
        if abs(top - toplast) < 0.05:
            top = toplast
        left = i[1]
        if abs(i[1] - leftserie[0]) < 0.05:
            left = leftserie[0]
        elif abs(i[1] - leftserie[1]) < 0.05:
            left = leftserie[1]
        line = i[2]
        if left == leftserie[0] and not "font-weight:bold" in line:
            continue
        soup = bsoup(line, "lxml")
        text = soup.text.strip()
        if left in leftserie:
            index = leftserie.index(left)
            if index in leftdict:
                leftdict[index].append([top, left, text])
            else:
                tlist = []
                tlist.append([top, left, text])
                leftdict[index] = tlist
    leftdf = pd.DataFrame(leftdict[0], columns=['top', 'left', 'text'])
    rightdf = pd.DataFrame(leftdict[1], columns=['top', 'left', 'text'])
    leftdf = leftdf.sort_values(by='top', ascending=True)
    rightdf = rightdf.sort_values(by='top', ascending=True)

    lrdfs = pd.merge(leftdf, rightdf, left_on='top', right_on='top', how='outer')
    lrdfs = lrdfs.sort_values("top", ascending=True)

    lrdfs.index = range(len(lrdfs))
    number_start = []
    for i in range(len(lrdfs) - 1):
        xt = lrdfs.ix[i, 'text_x']
        yt = lrdfs.ix[i, 'text_y']
        xtl = lrdfs.ix[i + 1, 'text_x']
        ytl = lrdfs.ix[i + 1, 'text_y']
        if xt is not np.nan and yt is not np.nan:
            number_start.append(i)
        if xt is np.nan and yt is not np.nan and xtl is not np.nan and ytl is np.nan:
            number_start.append(i + 1)
    number_start.append(len(lrdfs))
    line_remove = []
    for i in range(len(lrdfs)):
        if i in number_start:
            next_index = number_start.index(i) + 1
            next_i = i
            if next_index < len(number_start):
                next_i = number_start[next_index]
            if i + 1 == len(lrdfs):
                continue
            xtn = lrdfs.ix[i + 1, 'text_x']
            ytn = lrdfs.ix[i + 1, 'text_y']
            t = i
            if xtn is np.nan and ytn is not np.nan:
                while t < next_i - 1:
                    t = t + 1
                    lrdfs.loc[i, 'text_y'] = lrdfs.ix[i, 'text_y'].encode("utf-8") + " " + lrdfs.ix[t, 'text_y'].encode(
                        "utf-8")
                    line_remove.append(t)
            if ytn is np.nan and xtn is not np.nan:
                while t < next_i - 1:
                    t = t + 1
                    lrdfs.loc[i, 'text_x'] = lrdfs.ix[i, 'text_x'].encode("utf-8") + " " + lrdfs.ix[t, 'text_x'].encode(
                        "utf-8")
                    line_remove.append(t)
    lrdfs = lrdfs.drop(line_remove)
    lrtext = lrdfs[['text_x', 'text_y']]
    lrtext.index = range(len(lrtext))
    return lrtext


def saveabc(a, b, c):
    aret = []
    bret = []
    cret = []
    for i in a:
        aline = i.split(" ")
        selforganization = " ".join(aline[:-2])
        status = aline[-2]
        date = aline[-1]
        if selforganization == "" or status == "" or date == "":
            print aline
        aret.append([crd, selforganization, status, date])
    for i in b:
        aline = i.split(" ")
        selforganization = " ".join(aline[:-2])
        status = aline[-2]
        date = aline[-1]
        if selforganization == "" or status == "" or date == "":
            print aline
        bret.append([crd, selforganization, status, date])
    for i in c:
        cret.append([crd, i])
    db.executeMany(sql="insert into brocker_self_regulatory_organization",
                   columns=["crd", "self_regulatory_organization", "status", "date"], data=aret, db="app_data")
    db.executeMany(sql="insert into brocker_registrations_states",
                   columns=["crd", "states_territories", "status", "date"], data=bret, db="app_data")
    db.executeMany(sql="insert into brocker_bussiness_types",
                   columns=["crd", "type_of_bussiness"], data=cret, db="app_data")


# def thread_self_regulatory(filename):
#     logger.info("======= processing file:{} =======".format(i))
#     crd = filename.split("_")[1].replace(".html","")
#     filename = htmlpath + i
#     f = open(filename, "r")
#     html = f.read()
#     HTML = "<html "
#     source_html = bsoup(html, 'lxml')
#     html_tag = source_html.find("html").attrs
#     HTML += "xmlns=" + html_tag['xmlns'] + ">\n"
#     head = source_html.find("head")
#     HTML += str(head) + "\n"
#     HTML += "<body>"
#     pages = source_html.findAll("div", {"class": "page"})
#     a,b,c = get_self_regulator(pages[4:])
#     saveabc(a,b,c)



def get_events(pages):
    disclosure_regulatory = []
    disclosure_civil = []
    disclosure_award = []

    disclosure_regulatory_all = []
    disclosure_civil_all = []
    disclosure_award_all = []

    disclosure_regulatory_flag = False
    disclosure_regulatory_real_flag = False
    disclosure_civil_flag = False
    disclosure_civil_real_flag = False
    disclosure_award_flag = False
    disclosure_award_real_flag = False

    disclosure_award_counter = 0
    for page in pages:
        divs = page.findAll("div")
        div_dict = {}
        div_id_dict = {}
        div_left_dict = {}
        div_width_dict = {}
        div_class_dict = {}
        for div in divs:
            class_tag = div.attrs['class'][0]
            if class_tag == 'r':
                continue
            id = None
            style = div.attrs['style']
            if class_tag != "r":
                id = div.attrs['id']
            else:
                id = None
            style_attrs = style.split(";")
            div_key_list = []
            left = ""
            width = ""
            for style_attr in style_attrs:
                if style_attr == "":
                    continue
                style_attr_list = style_attr.split(":")
                attr = style_attr_list[0]
                value = style_attr_list[1]
                if attr == "left":
                    left = value
                elif attr == "width":
                    width = value
                else:
                    div_key_list.append(style_attr)
            div_key_str = ";".join(div_key_list)
            div_class_dict[div_key_str] = class_tag
            if div_key_str not in div_dict:
                text = []
                text.append(div.text)
                div_dict[div_key_str] = text
                div_left_dict[div_key_str] = left
                div_width_dict[div_key_str] = width
                div_id_dict[div_key_str] = id
            else:
                # if div_dict[div_key_str] is None or div_key_str is None:
                #     print div_key_str
                div_value_list = div_dict[div_key_str]
                div_value_list.append(div.text)
                div_dict[div_key_str] = div_value_list
        div_att = []
        for key in div_dict:
            id_ = div_id_dict[key]
            class_ = div_class_dict[key]
            key_ = key
            left_ = div_left_dict[key]
            width_ = div_width_dict[key]
            text_ = sep.join(div_dict[key])
            id_str = ""
            if id_ is not None:
                id_str = "id='{}'".format(id_)
            line = '''<div class="{}" {} style="{};left:{};width{};">{}</div>
                    '''.format(class_, id_str, key_, left_, width_, text_.encode("utf-8"))
            top = float(topregex.findall(line)[0])
            left = float(left_[:-2])
            div_att.append([top, left, line])
        df = pd.DataFrame(div_att, columns=['top', 'left', 'line'])
        dfs = df.sort_values(by=['top', 'left'], ascending=True)
        dfs.index = range(len(dfs))
        for i in range(len(dfs)):
            tagline = dfs.ix[i, 'line']
            left = dfs.ix[i, 'left']
            left_next = 0
            taglinesoup = bsoup(tagline, 'lxml')
            taglinesoup = taglinesoup.find("div")
            taglinesoup.attrs['style']
            if i < len(dfs) - 1:
                tagnextline = dfs.ix[i + 1, 'line']
                left_next = dfs.ix[i + 1, 'left']
                tagnextlinesoup = bsoup(tagnextline, 'lxml')
                tagnextlinesoup = tagnextlinesoup.find("div")
                line_next = tagnextlinesoup.text.strip()
            line = taglinesoup.text.strip()
            if u'2019 FINRA. All rights reserved. Report about ' in line or line == u'www.finra.org/brokercheck User Guidance\n':
                continue
            if u'There may be a non-monetary award associated with this arbitration' in line or u'Please select the Case Number above to view more detailed information' in line:
                continue

            if line == "Regulatory - Final":
                disclosure_regulatory_flag = True
                continue

            if line == "Civil - Final":
                disclosure_regulatory_flag = False
                disclosure_civil_flag = True
                continue

            if line == "Arbitration Award - Award / Judgment":
                disclosure_regulatory_flag = False
                disclosure_civil_real_flag = False
                disclosure_civil_flag = False
                disclosure_award_flag = True
                continue
            if line == u'Civil Bond':
                disclosure_regulatory_flag = False
                disclosure_civil_real_flag = False
                disclosure_award_real_flag = False

            if line == "i":
                continue

            disclosure_list = disclosureregex.findall(line)
            if len(disclosure_list) > 0 and disclosure_regulatory_flag == True:
                index = disclosure_list[0].index("of")
                # logger.info("================Regulatory - Final:{}===================".format(line))
                if len(disclosure_regulatory)>1:
                    numbers = int(disclosure_list[0][:index].replace("Disclosure","").strip())
                    disclosure_regulatory_all.append([numbers-1,disclosure_regulatory])
                    disclosure_regulatory = []
                disclosure_regulatory_real_flag = True
                continue

            if len(disclosure_list) > 0 and disclosure_civil_flag == True:
                index = disclosure_list[0].index("of")
                # logger.info("================Civil - Final:{}===================".format(line))
                if len(disclosure_civil)>1:
                    numbers = int(disclosure_list[0][:index].replace("Disclosure","").strip())
                    disclosure_civil_all.append([numbers-1,disclosure_civil])
                    disclosure_civil = []
                disclosure_regulatory_real_flag = False
                disclosure_civil_real_flag = True
                continue

            if len(disclosure_list) > 0 and disclosure_award_flag == True:
                index = disclosure_list[0].index("of")
                # logger.info("================award - Final:{}===================".format(line))
                if len(disclosure_award)>1:
                    numbers = int(disclosure_list[0][:index].replace("Disclosure","").strip())
                    disclosure_award_all.append([numbers-1,disclosure_award])
                    disclosure_award = []
                disclosure_regulatory_flag = False
                disclosure_civil_real_flag = False
                disclosure_civil_flag = False
                disclosure_regulatory_real_flag = False
                disclosure_award_real_flag = True
                continue

            if disclosure_regulatory_real_flag == True:
                if abs(left-19.81)<0.1 and abs(left_next - 177.346)<0.1:
                    disclosure_regulatory.append([line,line_next])
                if abs(left-177.346)<0.1 and abs(left_next - 177.346)<0.1:
                    tmpx = disclosure_regulatory[-1][0]
                    tmpy = disclosure_regulatory[-1][1]+u" "+line_next
                    disclosure_regulatory.pop(-1)
                    disclosure_regulatory.append([tmpx,tmpy])
                    if len(disclosure_regulatory)>2 and disclosure_regulatory[-1] == disclosure_regulatory[-2]:
                        disclosure_regulatory.pop(-1)
                if abs(left-177.346)<0.1 and abs(left_next-19.81)<0.1:
                    continue
                if abs(left-19.81)<0.1 and abs(left_next-19.81)<0.1:
                    if len(disclosure_regulatory)>1 and not ":" in disclosure_regulatory[-1][0] and not "?" in disclosure_regulatory[-1][0]:
                        tmpx = disclosure_regulatory[-1][0]+u" "+line
                        tmpy = disclosure_regulatory[-1][1]
                        disclosure_regulatory.pop(-1)
                        disclosure_regulatory.append([tmpx,tmpy])
                    elif ":" in line or "?" in line:
                        if line in disclosure_regulatory[-1][0]:
                            continue
                        else:
                            disclosure_regulatory.append([line,""])
                    elif ":" not in line and "?" not in line:
                        disclosure_regulatory.append([line+u" "+line_next,""])
            if len(disclosure_regulatory)>=2 and disclosure_regulatory[-1] == disclosure_regulatory[-2]:
                disclosure_regulatory.pop(-1)

            if disclosure_civil_real_flag == True:
                if abs(left-19.81)<0.1 and abs(left_next - 177.346)<0.1:
                    disclosure_civil.append([line,line_next])
                if abs(left-177.346)<0.1 and abs(left_next - 177.346)<0.1:
                    tmpx = disclosure_civil[-1][0]
                    tmpy = disclosure_civil[-1][1]+u" "+line_next
                    disclosure_civil.pop(-1)
                    disclosure_civil.append([tmpx,tmpy])
                    if len(disclosure_civil)>2 and disclosure_civil[-1] == disclosure_civil[-2]:
                        disclosure_civil.pop(-1)
                if abs(left-177.346)<0.1 and abs(left_next-19.81)<0.1:
                    continue
                if abs(left-19.81)<0.1 and abs(left_next-19.81)<0.1:
                    if len(disclosure_civil)>1 and not ":" in disclosure_civil[-1][0] and not "?" in disclosure_civil[-1][0]:
                        tmpx = disclosure_civil[-1][0]+u" "+line
                        tmpy = disclosure_civil[-1][1]
                        disclosure_civil.pop(-1)
                        disclosure_civil.append([tmpx,tmpy])
                    elif ":" in line or "?" in line:
                        if line in disclosure_civil[-1][0]:
                            continue
                        else:
                            disclosure_civil.append([line,""])
                    elif ":" not in line and "?" not in line:
                        disclosure_civil.append([line+u" "+line_next,""])
            if len(disclosure_civil)>=2 and disclosure_civil[-1] == disclosure_civil[-2]:
                disclosure_civil.pop(-1)
            if disclosure_award_real_flag == True:
                if abs(left-19.81)<0.1 and abs(left_next - 177.346)<0.1:
                    disclosure_award.append([line,line_next])
                if abs(left-177.346)<0.1 and abs(left_next - 177.346)<0.1:
                    tmpx = disclosure_award[-1][0]
                    tmpy = disclosure_award[-1][1]+u" "+line_next
                    disclosure_award.pop(-1)
                    disclosure_award.append([tmpx,tmpy])
                    if len(disclosure_award)>2 and disclosure_award[-1] == disclosure_award[-2]:
                        disclosure_award.pop(-1)
                if abs(left-177.346)<0.1 and abs(left_next-19.81)<0.1:
                    continue
                if abs(left-19.81)<0.1 and abs(left_next-19.81)<0.1:
                    if len(disclosure_award)>1 and not ":" in disclosure_award[-1][0] and not "?" in disclosure_award[-1][0]:
                        tmpx = disclosure_award[-1][0]+u" "+line
                        tmpy = disclosure_award[-1][1]
                        disclosure_award.pop(-1)
                        disclosure_award.append([tmpx,tmpy])
                    elif ":" in line or "?" in line:
                        if line in disclosure_award[-1][0]:
                            continue
                        else:
                            disclosure_award.append([line,""])
                    elif ":" not in line and "?" not in line:
                        disclosure_award.append([line+u" "+line_next,""])
            if len(disclosure_award)>=2 and disclosure_award[-1] == disclosure_award[-2]:
                disclosure_award.pop(-1)
    if len(disclosure_regulatory_all)!=0:
        lastnumbers1 = disclosure_regulatory_all[-1][0]+1
        disclosure_regulatory_all.append([lastnumbers1,disclosure_regulatory])
    else:
        disclosure_regulatory_all.append([1,disclosure_regulatory])
    if len(disclosure_civil_all)!=0:
        lastnumbers2 = disclosure_civil_all[-1][0]+1
        disclosure_civil_all.append([lastnumbers2,disclosure_civil])
    else:
        disclosure_civil_all.append([1,disclosure_civil])

    if len(disclosure_award_all)!=0:
        lastnumbers3 = disclosure_award_all[-1][0]+1
        disclosure_award_all.append([lastnumbers3,disclosure_award])
    else:
        disclosure_award_all.append([1,disclosure_award])
    return disclosure_regulatory_all, disclosure_civil_all, disclosure_award_all

def saveDisclosure(a,b,c,crd):
    disclosure_event = []
    disclosure_civil = []
    disclosure_award = []
    event_key_list = [u'Reporting Source:',
                      u'Current Status:',
                      u'Allegations:',
                      u'Initiated By:',
                      u'Date Initiated:',
                      u'Docket/Case Number:',
                      u'Principal Product Type:',
                      u'Other Product Type(s):',
                      u'Principal Sanction(s)/Relief Sought:',
                      u'Other Sanction(s)/Relief Sought:',
                      u'Resolution:',
                      u'Resolution Date:',
                      u'Does the order constitute a final order based on violations of any laws or regulations that prohibit fraudulent, manipulative, or deceptive conduct?',
                      u'Sanctions Ordered:',
                      u'Other Sanctions Ordered:',
                      u'Sanction Details:',
                      u'Firm Statement']
    civil_key_list = [
                    u'Reporting Source:',
                    u'Current Status:',
                    u'Allegations:',
                    u'Initiated By:',
                    u'Court Details:',
                    u'Date Court Action Filed:',
                    u'Principal Product Type:',
                    u'Other Product Types:',
                    u'Relief Sought:',
                    u'Other Relief Sought:',
                    u'Resolution:',
                    u'Resolution Date:',
                    u'Sanctions Ordered or Relief Granted:',
                    u'Other Sanctions',
                    u'Sanction Details:',
                    u'Regulator Statement',
                    u'Firm Statement'
                ]
    award_key_list = [
                    u'Reporting Source:',
                    u'Type of Event:',
                    u'Allegations:',
                    u'Arbitration Forum:',
                    u'Case Initiated:',
                    u'Case Number:',
                    u'Disputed Product Type:',
                    u'Sum of All Relief Requested:',
                    u'Disposition:',
                    u'Disposition Date:',
                    u'Sum of All Relief Awarded:'
                ]
    for index,i in enumerate(a):
        key_index = i[0]
        i = a[index][1]
        flag = False
        line = [crd,key_index]
        tmpline = []
        eventkv = {u'Reporting Source:':"",
                                    u'Current Status:':"",
                                    u'Allegations:':"",
                                    u'Initiated By:':"",
                                    u'Date Initiated:':"",
                                    u'Docket/Case Number:':"",
                                    u'Principal Product Type:':"",
                                    u'Other Product Type(s):':"",
                                    u'Principal Sanction(s)/Relief Sought:':"",
                                    u'Other Sanction(s)/Relief Sought:':"",
                                    u'Resolution:':"",
                                    u'Resolution Date:':"",
                                    u'Does the order constitute a final order based on violations of any laws or regulations that prohibit fraudulent, manipulative, or deceptive conduct?':"",
                                    u'Sanctions Ordered:':"",
                                    u'Other Sanctions Ordered:':"",
                                    u'Sanction Details:':"",
                                    u'Firm Statement':""}
        for k,j in enumerate(i):
            if j[0] in eventkv.keys():
                if not (eventkv["Reporting Source:"]!="" and j[0] == "Reporting Source:"):
                    eventkv[j[0]] = j[1]
                    if j[0] in eventkv:
                        tmpline.append(j[1])
            if (j[0] == u'Reporting Source:' and k>0) or k == len(i)-1:
                if len(tmpline)>=5:
                    for key in event_key_list:
                        line.append(eventkv[key])
                    disclosure_event.append(line)
                    flag = True
                    eventkv = {
                        u'Reporting Source:':"",
                        u'Current Status:':"",
                        u'Allegations:':"",
                        u'Initiated By:':"",
                        u'Date Initiated:':"",
                        u'Docket/Case Number:':"",
                        u'Principal Product Type:':"",
                        u'Other Product Type(s):':"",
                        u'Principal Sanction(s)/Relief Sought:':"",
                        u'Other Sanction(s)/Relief Sought:':"",
                        u'Resolution:':"",
                        u'Resolution Date:':"",
                        u'Does the order constitute a final order based on violations of any laws or regulations that prohibit fraudulent, manipulative, or deceptive conduct?':"",
                        u'Sanctions Ordered:':"",
                        u'Other Sanctions Ordered:':"",
                        u'Sanction Details:':"",
                        u'Firm Statement':""
                    }
                    line = [crd,key_index]
                    tmpline = []
                    eventkv[j[0]] = j[1]
                    tmpline.append(j[1])
                else:
                    line = [crd,key_index]
                    tmpline = []
        if flag ==False:
            print index
    for index,i in enumerate(b):
        key_index = i[0]
        i = b[index][1]
        flag = False
        line = [crd,key_index]
        tmpline = []
        civilkv = {
            u'Reporting Source:':'',
            u'Current Status:': '',
            u'Allegations:': '',
            u'Initiated By:': '',
            u'Court Details:': '',
            u'Date Court Action Filed:': '',
            u'Principal Product Type:': '',
            u'Other Product Types:': '',
            u'Relief Sought:': '',
            u'Other Relief Sought:': '',
            u'Resolution:': '',
            u'Resolution Date:': '',
            u'Sanctions Ordered or Relief Granted:': '',
            u'Other Sanctions': '',
            u'Sanction Details:': '',
            u'Regulator Statement': '',
            u'Firm Statement':''
        }
        for k,j in enumerate(i):
            if k< len(i)-1:
                if j[0] == "Sanctions Ordered or Relief" and i[k+1][0] == "Granted:":
                    j[0] = j[0]+u" "+i[k+1][0]
                    j[1] = i[k][1]+u" "+i[k+1][1]
            if j[0] in civilkv.keys():
                if not (civilkv["Reporting Source:"]!="" and j[0] == "Reporting Source:"):
                    civilkv[j[0]] = j[1]
                    if j[0] in civilkv:
                        tmpline.append(j[1])
            if (j[0] == u'Reporting Source:' and k>0) or k == len(i)-1:
                if len(tmpline)>=5:
                    for key in civil_key_list:
                        line.append(civilkv[key])
                    disclosure_civil.append(line)
                    flag = True
                    civilkv = {
                        u'Reporting Source:':'',
                        u'Current Status:': '',
                        u'Allegations:': '',
                        u'Initiated By:': '',
                        u'Court Details:': '',
                        u'Date Court Action Filed:': '',
                        u'Principal Product Type:': '',
                        u'Other Product Types:': '',
                        u'Relief Sought:': '',
                        u'Other Relief Sought:': '',
                        u'Resolution:': '',
                        u'Resolution Date:': '',
                        u'Sanctions Ordered or Relief Granted:': '',
                        u'Other Sanctions': '',
                        u'Sanction Details:': '',
                        u'Regulator Statement': '',
                        u'Firm Statement':''
                    }
                    line = [crd,key_index]
                    tmpline = []
                    civilkv[j[0]] = j[1]
                    tmpline.append(j[1])
                else:
                    line = [crd,key_index]
                    tmpline = []
        if flag ==False:
            print index
    for index,i in enumerate(c):
        key_index = i[0]
        i = c[index][1]
        flag = False
        line = [crd,key_index]
        tmpline = []
        awardkv = {
            u'Reporting Source:':'',
            u'Type of Event:':'',
            u'Allegations:': '',
            u'Arbitration Forum:': '',
            u'Case Initiated:': '',
            u'Case Number:': '',
            u'Disputed Product Type:':'',
            u'Sum of All Relief Requested:': '',
            u'Disposition:': '',
            u'Disposition Date:': '',
            u'Sum of All Relief Awarded:': ''
        }
        for k,j in enumerate(i):
            if j[0] == u'There may be a non-monetary award associated with this arbitration. Please select the Case Number above to view more detailed information. Please select the Case Number above to view more detailed information.':
                continue
            if j[0] in awardkv.keys():
                if not (awardkv["Reporting Source:"]!="" and j[0] == "Reporting Source:"):
                    awardkv[j[0]] = j[1]
                    if j[0] in awardkv:
                        tmpline.append(j[1])
            if (j[0] == u'Reporting Source:' and k>0) or k == len(i)-1:
                if len(tmpline)>=5:
                    for key in award_key_list:
                        line.append(awardkv[key])
                    disclosure_award.append(line)
                    # if len([xgt for xgt in line if xgt!=""]) == 5:
                    #     print index
                    flag = True
                    awardkv = {
                        u'Reporting Source:':'',
                        u'Type of Event:':'',
                        u'Allegations:': '',
                        u'Arbitration Forum:': '',
                        u'Case Initiated:': '',
                        u'Case Number:': '',
                        u'Disputed Product Type:':'',
                        u'Sum of All Relief Requested:': '',
                        u'Disposition:': '',
                        u'Disposition Date:': '',
                        u'Sum of All Relief Awarded:': ''
                    }
                    line = [crd,key_index]
                    tmpline = []
                    awardkv[j[0]] = j[1]
                    tmpline.append(j[1])
                else:
                    line = [crd,key_index]
                    tmpline = []
        if flag ==False:
            print index
    for i in range(len(disclosure_event)/400+1):
        data1 = disclosure_event[400*i:400*i+400]
        db.executeMany(sql="insert into brocker_disclosure_event ",columns=["crd","disclosure_index","source", "status", "allegations", "initiate","date","number","type","other_type","sought","other_sought","resolution","resolution_date","consititute","sanction","other_sanction","sanction_detail","firm_statement"], data=data1, db="app_data")
    for i in range(len(disclosure_civil)/400+1):
        data2 = disclosure_civil[400*i:400*i+400]
        db.executeMany(sql="insert into brocker_disclosure_civil ",columns=["crd","disclosure_index","source", "status", "allegations", "initiate","court_detail","date_filed","type","other_type","sought","other_sought","resolution","resolution_date","sanction","other_sanction","sanction_detail","regulator_statement","firm_statement"], data=data2, db="app_data")
    for i in range(len(disclosure_award)/400+1):
        data3 = disclosure_award[400*i:400*i+400]
        db.executeMany(sql="insert into brocker_disclosure_award ",columns=["crd","disclosure_index","source", "event_type", "allegations", "arbitration","case_initiated","number","product_type","sum_requested","disposition","disposition_date","sum_awarded"], data=data3, db="app_data")



if __name__ == "__main__":
#获取 events
    # filenames = os.listdir(htmlpath)
    # for i in filenames:
    #     try:
    #         logger.info("======= processing file:{} =======".format(i))
    #         crd = i.split("_")[1].replace(".html", "")
    #         filename = htmlpath + i
    #         f = open(filename, "r")
    #         html = f.read()
    #         HTML = "<html "
    #         source_html = bsoup(html, 'lxml')
    #         html_tag = source_html.find("html").attrs
    #         HTML += "xmlns=" + html_tag['xmlns'] + ">\n"
    #         head = source_html.find("head")
    #         HTML += str(head) + "\n"
    #         HTML += "<body>"
    #         pages = source_html.findAll("div", {"class": "page"})
    #         ret = save_clearing_arrangements(pages,crd)
    #         save_clearing_arrangements_tomysql(ret)
    #     except Exception, e:
    #         logger.error(e, exc_info=True)
    #         continue
#获取 events
    filenames = os.listdir(htmlpath)
    for i in filenames:
        try:
            logger.info("======= processing file:{} =======".format(i))
            crd = i.split("_")[1].replace(".html", "")
            filename = htmlpath + i
            f = open(filename, "r")
            html = f.read()
            HTML = "<html "
            source_html = bsoup(html, 'lxml')
            html_tag = source_html.find("html").attrs
            HTML += "xmlns=" + html_tag['xmlns'] + ">\n"
            head = source_html.find("head")
            HTML += str(head) + "\n"
            HTML += "<body>"
            pages = source_html.findAll("div", {"class": "page"})
            a, b, c = get_events(pages)
            saveDisclosure(a,b,c,crd)
        except Exception, e:
            logger.error(e, exc_info=True)
            continue



'''
    # 处理 self-regulatory registration and bussiness type
    filenames = os.listdir(htmlpath)
    for i in filenames:
        try:
            logger.info("======= processing file:{} =======".format(i))
            crd = i.split("_")[1].replace(".html", "")
            filename = htmlpath + i
            f = open(filename, "r")
            html = f.read()
            HTML = "<html "
            source_html = bsoup(html, 'lxml')
            html_tag = source_html.find("html").attrs
            HTML += "xmlns=" + html_tag['xmlns'] + ">\n"
            head = source_html.find("head")
            HTML += str(head) + "\n"
            HTML += "<body>"
            pages = source_html.findAll("div", {"class": "page"})
            a, b, c = get_self_regulator(pages[4:])
            saveabc(a, b, c)
        except Exception, e:
            logger.error(e, exc_info=True)
            continue

    # saveFile("/home/eos/data/pdf2html/firm_132007.html","132007")
'''
    # 处理 direct owner indirect owner and organization_affiliates
    # filenames = os.listdir(htmlpath)
    # for i in filenames:
    #     try:
    #         logger.info("======= processing file:{} =======".format(i))
    #         crd = i.split("_")[1].replace(".html","")
    #         filename = htmlpath + i
    #         saveFile(filename, crd)
    #     except Exception, e:
    #         logger.error(e,exc_info=True)
    #         continue
# for index,page in enumerate(pages):
#     # if index == 2:
#     #     break
#     page_info = "<div"
#     for i in page.attrs:
#         attr = i
#         value = page.attrs[i]
#         if i == "class":
#             value = value[0]
#         page_info+=" {}='{}' ".format(attr,value)
#     HTML+=page_info+">"
#     divs = page.findAll("div")
#     divs_formated = reformatDiv(divs)
#     HTML+=divs_formated+"</div>"
# HTML+="</body></html>"
