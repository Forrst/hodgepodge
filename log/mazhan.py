import MySQLdb
import happybase

code = []
code.append("61800009")
code.append("61800001")

con = MySQLdb.connect(host='192.168.5.106',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "select * from hk_bills where code in %s"%code
sql = sql.replace("[","(").replace("]",")")
cursor.execute(sql)
result = cursor.fetchall()
cursor.close()
con.close()

insert = []
for i in result:
    insert.append([i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10]])


con = MySQLdb.connect(host='192.168.2.231',user='root',passwd='zunjiazichan123',db='app_data',charset='utf8')
cursor = con.cursor()
sql = "insert into hk_bills(name,rowkey,code,bill_type,bill_date,png_location,pdf_location,email) values(%s,%s,%s,%s,%s,%s,%s,%s)"
cursor.executemany(sql,insert)
cursor.close()
con.commit()
con.close()


connection1 = happybase.Connection("192.168.2.232")
connection1.open()
table1 = connection1.table("hk_bills")


connection2 = happybase.Connection("192.168.5.151")
connection2.open()
table2 = connection2.table("hk_bills")
for i in insert:
    rowkey = i[1].decode("hex")
    data = table2.row(rowkey,columns=['info:pdf','info:png'])
    pdf = data['info:pdf']
    png = data['info:png']
    table1.put(rowkey,{'info:pdf':pdf,'info:png':png})

connection1.close()
connection2.close()
