#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-16 上午9:27
'''

import ConfigParser
import os
import time
import requests
from aip import AipOcr
import logging
#os.chdir("/home/eos/git/auto_account/")


class BaiduOCR(object):
    def __init__(self):
        '''
        :param host:
        '''
        self.confPath = "db/config/ocr.cfg"
        self.orconfig = self.getConfig()

    def getConfig(self):
        '''
        :return:
        '''
        conf = ConfigParser.ConfigParser()
        conf.readfp(open(self.confPath))
        APP_ID = conf.get('baiduaip', "appid")
        API_KEY = conf.get('baiduaip', "apikey")
        SECRET_KEY = conf.get('baiduaip', "secretkey")
        return {"APP_ID": APP_ID, "API_KEY": API_KEY, "SECRET_KEY": SECRET_KEY}

    def getHttpContent(self, file_path):

        ret = ""
        """ 读取图片 """
        # def get_file_content(filePath):
        #     with open(filePath, 'rb') as fp:
        #         return fp.read()

        host = "http://10.10.1.51:8086/miningaccount_manager/file/"
        # host_ = "http://117.121.21.173:8086/file"
        urls = []
        if "," in file_path:
            file_paths = file_path.split(",")
            for i in file_paths:
                url = host + i
                urls.append(url)
        else:
            urls.append(host + file_path)
        for url in urls:
            logging.info("request url is:"+str(url))
            try:
                r = requests.get(url)
            except Exception,e:
                logging.error(e, exc_info=True)
                continue
            image = r.content
            ret = ret + self.getContext(image) + "\n"
        return ret

    def getContext(self, image):
        ret = ""
        """ 调用通用文字识别（高精度版） """
        client = AipOcr(self.orconfig['APP_ID'], self.orconfig['API_KEY'], self.orconfig['SECRET_KEY'])
        """ 如果有可选参数 """
        options = {}
        options["detect_direction"] = "true"
        options["probability"] = "true"

        """ 带参数调用通用文字识别（高精度版） """
        # 高级版
        result = None
        words_result = {'word':""}
        try:
            result = client.basicAccurate(image, options)
        except Exception,e:
            logging.error(e,exc_info=True)
            logging.info("百度ocr获取失败，重试中...")
            time.sleep(3)
            result = client.basicAccurate(image, options)
        # 普通版
        # result = client.basicGeneral(image,options)
        try:
            words_result = result['words_result']
        except KeyError,e:
            logging.error(e,exc_info=True)
        for i in words_result:
            ret = ret + i['words'] + "\n"
        return ret

    def getImage(self, file_path):
        ret = ""
        image = open(file_path,'rb+').read()
        ret = ret + self.getContext(image) + "\n"
        return ret


if __name__ == '__main__':
    ocr = BaiduOCR()
    # path = "/accountimage/depositcert/2018/10/31/05C36169FAA5550316.jpg"
    path = "/home/eos/文档/tcyl.png"
    ret = ocr.getImage(path)
    # ret = ocr.getHttpContent(path)
    print ret
    print path + "\n" + ret
