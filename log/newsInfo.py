import happybase
#线下数据库http://192.168.2.231/pma/index.php
#  192.168.2.231 root/zunjiazichan123

con = happybase.Connection("192.168.2.232")
con.open()
table = con.table("news")
data = table.row(bytes.fromhex("6bda7ffffe8e3620e3fbf25b8e6a0000"))
for i in data:
    print(f"key{i}\tvalue{data[i]}")