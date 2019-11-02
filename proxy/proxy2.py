#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-10-16 上午11:09
'''
# encoding:utf-8
import socket
import thread
import re
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')

def getAddr(d):
    a = re.search("Host: (.*)\r\n", d)
    host = a.group(1)
    a = host.split(":")
    if len(a) == 1:
        return (a[0], 80)
    else:
        return (a[0], int(a[1]))

def client(conn, caddr):
    while 1:
        try:
            data = conn.recv(4096)
            if len(data)==0 or data is None or data == "":
                continue
            logging.info("request data:::::::::::::::::::::::::::{}".format(data))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            addr = getAddr(data)
            # print "目的服务器：",addr
            s.connect(addr)
            # print '发给目的服务器数据：',data
            s.sendall(data)#将请求数据发给目的服务器
            d = s.recv(40960)#接收目的服务器发过来的数据
            logging.info("response data::::::::::::::::::::::::::".format(d))
            s.close()#断开与目的服务器的连接
            conn.sendall(d)#发送给代理的客户端
        except Exception, e:
            logging.error(e,exc_info=True)
            # print '代理的客户端异常：%s, ERROR:%s'%(caddr,e)
            conn.close()
            break

def serve(PORT = 8083):
    # 创建
    IP = "192.168.3.96"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((IP, PORT))
    s.listen(10)
    print 'proxy start...'
    while True:
        conn, addr = s.accept()
        # print 'conn:', conn
        # print "addr:", addr
        thread.start_new_thread(client, (conn, addr))
try:
    serve()
except Exception as e:
    logging.error(e,exc_info=True)
    # print '代理服务器异常',e

print 'server end!!!'