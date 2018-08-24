import happybase
#线下数据库http://192.168.2.231/pma/index.php
#  192.168.2.231 root/zunjiazichan123

con = happybase.Connection("192.168.2.232")
con.open()
table = con.table("news")
data = table.row("d0df7ffffe9ade954b59f830053f0000".decode("hex"))
