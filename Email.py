#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-12-05 下午2:38
'''
from __future__ import absolute_import
import sys
print sys.path

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
import smtplib

_user = "295627520@qq.com"
_pwd = "pxbgmgnifwurbied"
_geter = "jia.zhou@aliyun.com"

msg = MIMEMultipart()
msg['Subject'] = '测试'
msg['From'] = _user
msg['To'] = _geter

puretext = MIMEText('这是一封测试邮件')
msg.attach(puretext)

path = "/root/env/public/"
# mp3part = MIMEApplication(open(path+'movie.mp4', 'rb').read())
mp3part = MIMEApplication(open("/home/eos/CHATS12180802BKR03183.png", 'rb').read())

mp3part.add_header('Content-Disposition', 'attachment', filename='movie.mp4')
msg.attach(mp3part)

server = smtplib.SMTP_SSL("smtp.qq.com", 465)
server.login(_user,_pwd)
server.sendmail(_user,_geter,msg.as_string())
server.quit()