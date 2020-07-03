#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-10 下午2:35
'''
import requests
from bs4 import BeautifulSoup as soup
import datetime
import re
import os
os.chdir("/home/eos/git/hodgepodge")
from db.mysql.SqlUtil import Mysql
import pandas as pd

regex = re.compile("\d{1,}\sof\s\d{1,}")


apikeytoken = "3QM3QQ7FQIMC9C1I8CTKMDT2FXAGBBMUSX"


# skr token contract_address
contract_address = "0x26587f4d672876e61a91b887f83ced591be1cba4"

#获取一个地址的以太坊账户余额
# etherBalance = "https://api.etherscan.io/api?module=account&action=balance&address={}&tag=latest&apikey={}".format(address,apikeytoken)
#获取多个地址的以太坊账户余额


'''
url = "https://api.etherscan.io/api?module=stats&action=tokensupply&contractaddress={}&apikey={}".format(contract_address,apikeytoken)

url = "http://api.etherscan.io/api?module=account&action=txlist&address={}&startblock=0&endblock=99999999&sort=asc&apikey={}".format(address,apikeytoken)

javascript:move('generic-tokentxns2?contractAddress=0x26587f4d672876e61a91b887f83ced591be1cba4&mode=&a=0x1cfca010814474e29bda43930a7990070a7a3f54&m=normal&p=3')
'''


#skr 持币地址排行

url = "https://etherscan.io/token/generic-tokenholders2?m=normal&a=0x26587f4d672876e61a91b887f83ced591be1cba4&s=5000000000000000000&p=1";
r = requests.get(url)

html = r.text
source = soup(html,"lxml")
table = source.find("table",{"class":"table table-md-text-normal table-hover"})
trs = table.findAll("tr")
listTop100 = []
for tr in trs[1:]:
    topaddress = tr.find("a").text
    tds = tr.findAll("td")
    amount = float(tds[-2].text.replace(",",""))
    percent = float(tds[-1].text.replace("%",""))*1.0/100
    listTop100.append([topaddress,amount,percent])
        
addressTimes = []
transactions = {}
for addr in listTop100:
    total_pages = 2
    page = 1
    flow = []
    while page<= total_pages:
        url = "https://etherscan.io/token/generic-tokentxns2?contractAddress={}&mode=&a={}&m=normal&p={}".format(contract_address,addr[0],page)
        print url
        r = requests.get(url)
        source = soup(r.text,"lxml")
        div = source.find("div",{"id":"maindiv"})
        trs = div.findAll("tr")
        for tr in trs[1:]:
            tds = tr.findAll("td")
            _from = tds[2].text
            _to = tds[4].text
            _time = str(datetime.datetime.strptime(tds[1].find("span")['title'],"%b-%d-%Y %H:%M:%S %p"))
            _flow = tds[3].text.replace("\xc2\xa0","")
            _amount = float(tds[5].text.replace(",",""))
            flow.append([_from,_to,_time,_flow,_amount])
        if page == total_pages:
            page+=1
            continue
        page+=1
        pages = source.find("div",{"class":"d-inline-block"})
        pages_text = pages.text
        page_list = regex.findall(pages_text)
        if len(page_list)==0:
            total_pages = 1
        else:
            total_pages = int(page_list[0][page_list[0].index('of')+3:])
    transactions[addr[0]] = flow
    times = len(flow)
    addressTimes.append([addr[0],times])

###############################################
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib as mpl
DG = nx.DiGraph()
edges = []
nodes = []
# for key in transactions['']:
for transaction in transactions['0xe965a0e9f5880aed0178186225c2961fc1dd7f65']:
    i = transaction
    DG.add_node(i[0])
    DG.add_node(i[1])
    DG.add_edge(i[0],i[1],weight = i[4]/1000000)
pos = nx.layout.spring_layout(DG)

node_sizes = len(DG.nodes())
M = DG.number_of_edges()
edge_colors = range(2, M + 2)
edge_alphas = [(5 + i) / (M + 4) for i in range(M)]

nodes = nx.draw_networkx_nodes(DG, pos, node_size=node_sizes, node_color='blue')
edges = nx.draw_networkx_edges(DG, pos, node_size=node_sizes, arrowstyle='->',
                               arrowsize=10, edge_color=edge_colors,
                               edge_cmap=plt.cm.Blues, width=2)
# for i in range(M):
#     edges[i].set_alpha(edge_alphas[i])

pc = mpl.collections.PatchCollection(edges, cmap=plt.cm.Blues)
pc.set_array(edge_colors)
plt.colorbar(pc)

ax = plt.gca()
ax.set_axis_off()
plt.show()

#################################################
#获取转账记录
#mxc交易所转账记录skr
contract_address = "0x26587f4d672876e61a91b887f83ced591be1cba4"
exchange_address = "0x0211f3cedbef3143223d3acf0e589747933e8527"
total_page = 288
account_balance = {}
tag_address_dict = {}
def get_transactions_from_exchange(contract_address,total_page):
    flow = []
    for page_index in range(total_page+1)[::-1][:-1]:
        # url = "https://etherscan.io/token/generic-tokentxns2?contractAddress={}&mode=&a={}&m=normal&p={}".format(contract_address,exchange_address,page_index)
        url = "https://etherscan.io/token/generic-tokentxns2?contractAddress={}&amp;mode=&amp;m=normal&amp;p={}".format(contract_address,page_index)
        print url
        proxies = {
            "http": "socks5://127.0.0.1:1080",
            'https': 'socks5://127.0.0.1:1080'
        }

        header = {
            "Host": "etherscan.io",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:67.0) Gecko/20100101 Firefox/67.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "deflate",
            "Connection": "keep-alive",
            "Cookie": "__cfduid=d4cfb8f747a633259f8ee9f193ebb690a1571985379; ASP.NET_SessionId=erdi1dpdyhhwqobxkqh0gcjw; __cflb=1041149169",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "TE": "Trailers"
        }
        r = requests.get(url,proxies,headers = header)
        source = soup(r.text,"lxml")
        div = source.find("div",{"id":"maindiv"})
        trs = div.findAll("tr")
        for tr in trs[1:]:
            tds = tr.findAll("td")
            from_a = tds[2].find("a")
            if "title" in from_a.attrs:
                text = from_a.attrs['title']
                tagline = text.split("\n")
                tag = tagline[0]
                address = tagline[1].replace("(","").replace(")","")
                tag_address_dict[tag] = address
            _from = tds[2].text

            _to = tds[4].text
            _time = str(datetime.datetime.strptime(tds[1].find("span")['title'],"%b-%d-%Y %H:%M:%S %p"))
            _flow = tds[3].text.replace("\xc2\xa0","")
            _amount = float(tds[5].text.replace(",",""))
            if _from not in account_balance:
                account_balance[_from] = _amount*-1
            elif _from in account_balance:
                account_balance[_from] += _amount
            if _to not in account_balance:
                account_balance[_to] = _amount
            elif _to in account_balance:
                account_balance[_to]+= _amount
            flow.append([_from,_to,_time,_flow,_amount])
    return flow

ret = []
for i in flow:
    fro = i[0]
    to = i[1]
    date =  i[2]
    type = i[3]
    amount = i[4]
    ret.append([fro,to,date,type,amount])

db = Mysql("mysqllocalhost")
db.executeMany("insert into skr ",columns=['from_address','to_address','datetime','type','amount'],data=flow,db="erc20")
url = "https://etherscan.io/token/generic-tokentxns2?contractAddress=0x26587f4d672876e61a91b887f83ced591be1cba4&amp;mode=&amp;m=normal&amp;p=288"


total = 50000000000
def circo(x):
    from_ = x[0]
    to_ = x[1]
    amount = round(x[4]/total*100000000,2)
    small_from = from_[2:6]+from_[-4:]
    smal_to = to_[2:6]+to_[-4:]
    line = '''"{}"->"{}" [label="{}"];\n'''.format(small_from,smal_to,amount)
    return line

header = '''
digraph G
{
layout="dot";
'''
tailer = '}'

middler = ""
for i in a:
    middler+=circo(i)

graph = header+middler+tailer
print graph

