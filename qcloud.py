#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-15 下午2:23
'''
from qcloud_image import auth
import ConfigParser
import requests
import base64

conf = ConfigParser.ConfigParser()
conf.readfp(open('db/config/mysql.cfg'))

appid = conf.get('qcloud',"appid")
secret_id = conf.get('qcloud',"secret_id")
secret_key = conf.get('qcloud',"secret_key")
bucket = 'BUCKET'

authorization = auth.Auth(appid,secret_id,secret_key).get_sign(bucket)

# fimage = open()
# image = base64.b64encode(fimage.read())
# image = "data:image/jpg;base64,"+image
# fimage.close()

files = {"file":("winglung.jpg",open("/home/eos/data/winglung.jpg","rb"),"image/jpeg")}
url = "http://recognition.image.myqcloud.com/ocr/general"
headers={'host': 'recognition.image.myqcloud.com',
         'content-Type': 'application/json',
         'authorization': authorization,
         }

data = {'appid': appid,
        }
d=requests.post(url,data=data,headers=headers)

print(d)