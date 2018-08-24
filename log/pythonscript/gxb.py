#!/usr/bin/env python
#-*- coding:utf-8 -*-
import requests
import json
import time

headers = {
    "Host":"walletgateway.gxb.io",
    "Origin": "https://blockcity.gxb.io",
    "Accept-Encoding ": " br, gzip, deflate",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 (4404189184)",
    "Authorization":"aFpid3hySmZ0VzBldjI4Yk5iQjA1MjEzODUxMzc6OTQ4OFRDWVI4TEcyWEFkenJXWEVLTGs2OTgw",
    "Referer": "https://blockcity.gxb.io/",
    "Accept-Language": "zh-CN",
    }

def steal(ids):
    steal_url=' https://walletgateway.gxb.io/miner/steal/%s/mine/list'%(ids)
    steal_data = None  

    steal_r = requests.get(steal_url, headers = steal_headers)  
    steal_coins = steal_r.json()
    for coin in steal_coins:
        if coin['canSteal'] == True:
            print "Stealing coin: ........",
            mineId = coin['mineId']
            receive(ids,mineId)
            time.sleep(1)
    
def receive(ids,mineId):
    receive_url=' https://walletgateway.gxb.io/miner/steal/%s/mine/%s'%(ids,mineId)
    print ids,mineId
    receive_data = None  
    receive_headers = {
    "Host":"walletgateway.gxb.io",
    "Content-Type": "application/json;charset=utf-8",
    "Origin": "https://blockcity.gxb.io",
    "Accept-Encoding ": " br, gzip, deflate",
    "Content-Length": "0",
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 (4404189184)",
    "Authorization":"aFpid3hySmZ0VzBldjI4Yk5iQjA1MjEzODUxMzc6OTQ4OFRDWVI4TEcyWEFkenJXWEVLTGs2OTgw",
    "Referer": "https://blockcity.gxb.io/",
    "Accept-Language": "zh-CN",
    }
    receive_req = urllib2.Request(receive_url, receive_data, receive_headers)  
    receive_response = urllib2.urlopen(receive_req) 

counter = 0
while True:
    #try:
    url = ""
    if counter ==0:
        url = 'https://walletgateway.gxb.io/miner/steal/user/list?change=false&hasLocation=true'
    else:
        url = 'https://walletgateway.gxb.io/miner/steal/user/list?change=true&hasLocation=true'
headers ={
"Host": "walletgateway.gxb.io",
"Origin": "https://blockcity.gxb.io",
"Accept-Encoding": "br, gzip, deflate",
"Connection": "keep-alive",
"Accept": "application/json, text/plain, */*",
"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 (5737884160)",
"Authorization":"aFpid3hySmZ0VzBldjI4Yk5iQjA1MjEzODUxMzc6OTQ4OFRDWVI4TEcyWEFkenJXWEVLTGs2OTgw",
"Referer": "https://blockcity.gxb.io/",
"Accept-Language": "zh-CN"
}
r = None
if counter == 0:
    r = requests.get(url,headers = headers)
else:
    r = requests.options(url,headers = headers)
persons =  r.json()
if len(persons)==0:
    time.sleep(60*20)
    continue
for i in persons:
    if i['canSteal'] == True:
        print "Stealing：..........",i['nickName']
        ids = i['userId']
        steal(ids)
        time.sleep(60)
except Exception,e:
    print e
    print "Error time ",time.gmtime()

