import MySQLdb
import pandas as pd

sql = "select * from jcbms.account_charge_rule where charge_code = 'US_JC_FEE:US' and waive = 'Y'"
con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")

df = pd.read_sql(sql, con=con)
