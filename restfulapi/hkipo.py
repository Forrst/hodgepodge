#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-23 下午6:09
'''
import sys
import datetime
import os
sys.path.append("../")
from flask import Flask,url_for,send_from_directory
from flask_restful import reqparse, abort, Api, Resource
from db.mysql import SqlUtil as Mysql

app = Flask(__name__)
api = Api(app)




parser = reqparse.RequestParser()
parser.add_argument('task')

def getDetail(deposite_id):
    if deposite_id == "favicon.ico":
        return send_from_directory(os.path.join(app.root_path, 'static'),
                                   'favicon.ico', mimetype='image/vnd.microsoft.icon')
    sql =  Mysql.Mysql("mysql2.231")
    ret = sql.execute("select bank_id,bank_name from in_account where deposit_id = %s limit 1"%deposite_id,"bank")
    if len(ret) == 0:
        return "Deposit_id:%s does not exist!"%deposite_id
    else:
        bank_id,bank_name = ret[0][0],ret[0][1]
        result = sql.execute("select * from %s where id = %s limit 1"%(bank_name,bank_id),"bank")
        return getJson(bank_name,result[0])
# Todo
# shows a single todo item and lets you delete a todo item
class Todo(Resource):
    def get(self, todo_id):
        # abort_if_todo_doesnt_exist(todo_id)
        return getDetail(todo_id)

def getJson(bank_name,result):
    ret = {}
    sql =  Mysql.Mysql("mysql2.231")
    columns = sql.getDBColumns("bank",bank_name)
    column = columns[0]
    columnlist = column[0].split(",")
    for i,j in enumerate(columnlist):
        ret[str(j)] = result[i]
        if isinstance(result[i],datetime.datetime):
            ret[str(j)] = str(result[i])
        if isinstance(result[i],unicode):
            ret[str(j)] = result[i].encode("utf8")
    return ret



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
api.add_resource(Todo, '/<todo_id>')

if __name__ == '__main__':
    app.run(debug=True)