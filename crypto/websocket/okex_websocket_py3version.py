#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-12-13 上午10:17
'''
import zlib

import websockets
import asyncio
import logging
import json
import os

os.environ["http_proxy"] ="127.0.0.1:8118"
os.environ["https_proxy"] = "127.0.0.1:8118"
#export http_proxy="127.0.0.1:8118"

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger(" OKEX ")

def inflate(data):
    decompress = zlib.decompressobj(
        -zlib.MAX_WBITS
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


uri = "wss://real.okex.com:8443/ws/v3"
# websocket = websockets.connect(uri)
# paras = json.dumps({"op": "subscribe", "args": ["swap/candle1h:EOS-USD-SWAP"]})
# logger.info(paras)
# websocket.send(paras)
# message = websocket.recv()
# logger.info(inflate(message))



async def okex(uri):
    async with websockets.connect(uri) as websocket:
        paras = json.dumps({"op": "subscribe", "args": ["swap/candle1h:EOS-USD-SWAP"]})
        logger.info(f"-->   {paras}")
        await websocket.send(paras)
        message = await websocket.recv()
        logger.info(f"<--   {inflate(message)}")

asyncio.get_event_loop().run_until_complete(
    okex(uri))