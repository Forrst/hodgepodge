#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-16 上午9:27
'''

from aip import AipOcr
import ConfigParser
import requests


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
        APP_ID = conf.get('baiduaip',"appid")
        API_KEY = conf.get('baiduaip',"apikey")
        SECRET_KEY = conf.get('baiduaip',"secretkey")
        return {"APP_ID":APP_ID,"API_KEY":API_KEY,"SECRET_KEY":SECRET_KEY}



    def getHttpContent(self,file_path):

        ret = ""
        """ 读取图片 """
        # def get_file_content(filePath):
        #     with open(filePath, 'rb') as fp:
        #         return fp.read()

        host = "http://117.121.21.173:8086/miningaccount_manager/file/"
        urls = []
        if "," in file_path:
            file_paths = file_path.split(",")
            for i in file_paths:
                url = host+i
                urls.append(url)
        else:
            urls.append(host+file_path)
        for url in urls:
            r = requests.get(url)
            image = r.content
            ret = ret+self.getContext(image)+"\n"
        return ret



    def getContext(self,image):
        ret = ""
        """ 调用通用文字识别（高精度版） """
        client = AipOcr(self.orconfig['APP_ID'], self.orconfig['API_KEY'], self.orconfig['SECRET_KEY'])
        """ 如果有可选参数 """
        options = {}
        options["detect_direction"] = "true"
        options["probability"] = "true"

        """ 带参数调用通用文字识别（高精度版） """
        result = client.basicAccurate(image, options)
        # result = client.basicGeneral(image,options)
        words_result = result['words_result']
        for i in words_result:
            ret=ret+i['words']+"\n"
        return ret

if __name__=='__main__':
    ocr = BaiduOCR()
    path = "/home/eos/图片/微信图片_20191018142028.png"
    f = open(path,'r')
    m = f.read()
    f.close()
    ret = ocr.getContext(m)
    print ret
    print path+"\n"+ret