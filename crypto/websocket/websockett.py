#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-11-05 下午5:47
'''
import socket,base64,hashlib
import websocket

ws = websocket.WebSocketApp()





def get_headers(data):
    '''将请求头转换为字典'''
    header_dict = {}
    data = str(data,encoding="utf-8")

    header,body = data.split("\r\n\r\n",1)
    header_list = header.split("\r\n")
    for i in range(0,len(header_list)):
        if i == 0:
            if len(header_list[0].split(" ")) == 3:
                header_dict['method'],header_dict['url'],header_dict['protocol'] = header_list[0].split(" ")
        else:
            k,v=header_list[i].split(":",1)
            header_dict[k]=v.strip()
    return header_dict

sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock.bind(("127.0.0.1",8080))
sock.listen(5)

#等待用户连接
conn,addr = sock.accept()
print("conn from ",conn,addr)
#获取握手消息，magic string ,sha1加密
#发送给客户端
#握手消息
data = conn.recv(8096)
headers = get_headers(data)

# 对请求头中的sec-websocket-key进行加密
response_tpl = "HTTP/1.1 101 Switching Protocols\r\n" \
               "Upgrade:websocket\r\n" \
               "Connection: Upgrade\r\n" \
               "Sec-WebSocket-Accept: %s\r\n" \
               "WebSocket-Location: ws://%s%s\r\n\r\n"

magic_string = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

value = headers['Sec-WebSocket-Key'] + magic_string
ac = base64.b64encode(hashlib.sha1(value.encode('utf-8')).digest())

response_str = response_tpl % (ac.decode('utf-8'), headers['Host'], headers['url'])

# 响应【握手】信息
conn.send(bytes(response_str, encoding='utf-8'))
