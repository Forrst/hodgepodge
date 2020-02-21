#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-11-07 上午10:54
'''
import zlib
import json
import websocket
import threading
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger(__file__)

class okex(websocket.WebSocketApp):
    def __init__(self):
        self.ws = None

    def on_message(self, message):
        logger.info(self.inflate(message))

    def on_error(self, error):
        logger.info(error)

    def pong(self):
        self.ws.send(u"pong")
        logger.info(u"pong")

    def on_close(self):
        logger.info("### closed ###")

    def on_open(self):
        # paras = json.dumps({"op": "subscribe", "args": ["swap/ticker:BTC-USD-SWAP"]})
        paras = json.dumps({"op": "subscribe", "args": ["spot/candle14400s:ONT-USDT"]})
        # paras = json.dumps({"op": "subscribe", "args": ["spot/depth:EOS-USDT"]})
        logger.info(U"send: {}".format(paras.decode("utf-8")))
        self.ws.send(paras)
        # data = self.ws.recv()
        # print("receive:{}".format(data))

    def run(self):
        t = threading.Thread(target=self._run,args=())
        t.run()

    def _run(self):
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://real.okex.com:8443/ws/v3",
                                         on_message = self.on_message,
                                         on_error = self.on_error,
                                         on_close = self.on_close,
                                         on_open= self.on_open)
        self.ws.run_forever(ping_interval=10,ping_timeout=8,http_proxy_host="localhost",http_proxy_port=8118)

    def inflate(self,data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated


if __name__ == "__main__":
    ok = okex()
    ok.run()

#nice 666