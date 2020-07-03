import MySQLdb
import pandas as pd

df = pd.read_csv("rfm_recall.csv",encoding="gbk")
                 # , columns=['account','time','recency','frequency','fequency_month','monetary','value','recency1_level','frequency1_level','monetary1_level','customer1_level'])


def getTotalAssets(accountlist):
    con = MySQLdb.connect(host="192.168.5.105", user="root", passwd="zunjiazichan123", db="jcbms", charset="utf8")
    sql = f'''
            SELECT
                a.account_id as account,
                (market_value + trade_balance + ipo_frozen_before_close)*b.exchange_rate AS total
            from (select *FROM
                account_balance
            WHERE
                account_id in {str(accountlist).replace("[", "(").replace("]", ")")}
            AND process_date = '20200228') a left join currency_history b on a.currency = b.currency and a.process_date = b.process_date    
        '''
    result = pd.read_sql(sql, con)
    return result

accountlist = df.account.values
data = getTotalAssets(list(accountlist))
# rfm_data = pd.merge(df,data,how="left")
df.to_csv("df.csv")
data.to_csv("data.csv")