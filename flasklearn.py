#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-08-29 上午11:43
'''

from flask import Flask,render_template
app = Flask(__name__)

@app.route('/')
def index():
    name = ["jaryn","zhou"]
    return render_template('/home/eos/git/Mr.Jaryn/restfulapi/index.html', name=name)



if __name__ == '__main__':
    app.run(debug=True,host='192.168.3.168',port=8080)