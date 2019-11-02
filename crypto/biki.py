#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-31 下午4:19
'''
import os
os.chdir("/home/eos/git/open-api/sdk/biki-python-sdk-api/")

import biki.rest_api as restapi

if __name__ == '__main__':

    api_key = "ef07c51040c5708082255f833c7073e1"
    secretkey = "2f87e3594f57db261e34503aa224c758"

    # REST API
    restAPI = restapi.RestAPI(api_key, secretkey)

    # Public
    # result = restAPI.get_symbols()
    # result = restAPI.get_all_ticker()
    # result = restAPI.get_market()
    result = restAPI.get_ticker('aiwusdt')['data']
    # result = restAPI.get_trades('btcusdt')
    # result = restAPI.get_records('btcusdt', 30)
    # result = restAPI.get_market_dept('btcusdt', 'step0')

    # Privite
    # result = restAPI.get_account()
    # result = restAPI.get_new_order('btcusdt')
    # result = restAPI.get_all_order('btcusdt')
    # result = restAPI.get_all_trade('btcusdt')
    # result = restAPI.get_order_info(30, 'btcusdt')
    # result = restAPI.create_order('btcusdt', 'limit', 'buy', 1, 9000)
    # result = restAPI.create_order('btcusdt', 'market', 'sell', 3.333)
    # result = restAPI.cancel_order(39,'btcusdt')
    # result = restAPI.cancel_order_all('btcusdt')
    # result = restAPI.create_and_cancel_mass_orders('btcusdt', create_orders=[{'side':'buy', 'type':'market', 'volume':1000},{'side':'buy', 'type':'limit', 'volume':1.5, 'price':10000}], cancel_orders=[100001, 100002])
    # result = restAPI.create_and_cancel_mass_orders('btcusdt', create_orders=[{'side': 'buy', 'type': 'limit', 'volume': 2,'price': 10000},{'side': 'buy', 'type': 'limit', 'volume': 1.5,'price': 10000}])
    # result = restAPI.create_and_cancel_mass_orders('btcusdt', cancel_orders=[1000001, 1000002])

    print(result)


