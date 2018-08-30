#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-23 下午6:09
'''
import datetime
import os
import sys

sys.path.append("../")
from flask import Flask, send_from_directory
from flask_restful import reqparse, Api, Resource
from db.mysql import SqlUtil as Mysql

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('task')


def getDetail(deposite_id):
    if deposite_id == "favicon.ico":
        return send_from_directory(os.path.join(app.root_path, 'static'),
                                   'favicon.ico', mimetype='image/vnd.microsoft.icon')
    sql = Mysql.Mysql("mysql2.231")
    ret = sql.execute("select bank_id,bank_name from in_account where deposit_id = %s limit 1" % deposite_id, "bank")
    if len(ret) == 0:
        return "Deposit_id:%s does not exist!" % deposite_id
    else:
        bank_id, bank_name = ret[0][0], ret[0][1]
        result = sql.execute("select * from %s where id = %s limit 1" % (bank_name, bank_id), "bank")
        return getJson(bank_name, result[0])


# Todo
# shows a single todo item and lets you delete a todo item
class Todo(Resource):
    def get(self, todo_id):
        # abort_if_todo_doesnt_exist(todo_id)
        return getDetail(todo_id)


def getJson(bank_name, result):
    ret = {}
    col = []
    row = []
    path = ""
    sql = Mysql.Mysql("mysql2.231")
    columns = sql.getDBColumns("bank", bank_name)
    column = columns[0]
    columnlist = column[0].split(",")
    for i, j in enumerate(columnlist):
        col.append(str(j))
        x = result[i]
        ret[str(j)] = result[i]
        if isinstance(result[i], datetime.datetime):
            x = str(result[i])
            ret[str(j)] = str(result[i])
        if isinstance(result[i], unicode):
            ret[str(j)] = result[i].encode("utf8")
            x = result[i].encode("utf8")
        row.append(x)
    if bank_name.lower() == "icbc":
        path = "http://192.168.2.232:9999" + "/icbc/" + ret['img_name']
        url = "<a href='%s'>图片网址</a>" % path
        col.append("url")
        row.append(url)
        col.append("bank")
        row.append("工银亚洲")

    elif bank_name.lower() == "wlb":
        path = "http://192.168.2.232:9999" + "/WLB/" + ret['dealTime'].replace("-", "") + "/" + ret['imgName']
        url = "<a href='%s'>图片网址</a>" % path
        col.append("url")
        row.append(url)
        col.append("bank")
        row.append("永隆银行")
    elif bank_name.lower() == "bankofchinahk":
        path = "http://192.168.2.232:9999" + "/bankofchinaHK/" + ret['effectDate'].replace("/", "") + "/" + ret[
            'imgName']
        url = "<a href='%s'>图片网址</a>" % path
        col.append("url")
        row.append(url)
        col.append("bank")
        row.append("中银香港")
    return getHtml(col, row) +"<p >\n\n</p>"+ "<img src = '%s'>" % path
    # return json.dumps(ret,ensure_ascii=False)


def getHtml(col, row):
    html = "<table border='1'>"
    th = "<tr>"
    for i in col:
        th = th + "<th>" + str(i) + "</th>"
    th = th + "</tr><tr>"
    for i in row:
        th = th + "<td>" + str(i) + "</td>"
    html = html + th + "</tr></table>"
    return html


@app.route('/<deposit_id>')
def index(deposit_id):
    return getDetail(deposit_id)


# @app.route('/')
# def index():
#     html = "<input>"
#     return url_for(deposit_id)
# TodoList
# shows a list of all todos, and lets you POST to add new tasks
# class TodoList(Resource):
#     def get(self):
#         return TODOS

# def post(self):
#     args = parser.parse_args()
#     todo_id = int(max(TODOS.keys()).lstrip('todo')) + 1
#     todo_id = 'todo%i' % todo_id
#     TODOS[todo_id] = {'task': args['task']}
#     return TODOS[todo_id], 201


##
## Actually setup the Api resource routing here
##
# api.add_resource(TodoList, '/all')
# api.add_resource(Todo, '/<todo_id>')

if __name__ == '__main__':
    app.run(debug=True, host='192.168.2.119', port=8989)
