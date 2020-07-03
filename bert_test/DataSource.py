#此脚本用于获取币世界快讯

import requests
cn_headers = {'Host': 'iapi.bishijie.com', 'Connection': 'keep-alive', 'packetBit': 'pc', 'DNT': '1', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36', 'Accept': 'application/json, text/plain, */*', 'lang': 'zh-cn', 'uuid': '71FAEF4CDD21738C0108B79917E82964', 'timeZone': '+08:00', 'Session-Token': '', 'version': '1.0', 'Origin': 'https://www.bishijie.com', 'Sec-Fetch-Site': 'same-site', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Dest': 'empty', 'Referer': 'https://www.bishijie.com/kuaixun', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7'}
en_headers ={'Host': 'iapi.bishijie.com', 'Connection': 'keep-alive', 'packetBit': 'pc', 'DNT': '1', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36', 'Accept': 'application/json, text/plain, */*', 'lang': 'en', 'uuid': '71FAEF4CDD21738C0108B79917E82964', 'timeZone': '+08:00', 'Session-Token': '', 'version': '1.0', 'Origin': 'https://www.bishijie.com', 'Sec-Fetch-Site': 'same-site', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Dest': 'empty', 'Referer': 'https://www.bishijie.com/kuaixun', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7'}

import datetime
import time

#币世界资讯起始时间2017-10-19 14:26:14
end_time_str = "2020-10-19 14:30:14"
now = int(time.time())

end_datetime = datetime.datetime.strptime(end_time_str,'%Y-%m-%d %H:%M:%S')
end_timestamp = int(end_datetime.timestamp())
import hashlib
m = hashlib.md5()
str_souce = f"{now}b3290f72866a06136674b380a92446fetimestamp={end_timestamp}&size=50&client=M"
m.update(str_souce.encode("utf-8"))

signature = m.hexdigest()

url = f"https://iapi.bishijie.com/v3/newsFlash?timestamp={end_timestamp}&size=50&client=M&signature={signature}&ts={now}"
data = requests.get(url,headers=cn_headers)
js = data.json()

datalist = js['data']

list_message = datalist[0]

top = list_message['top']
date = list_message['date']
real_data = list_message['buttom']
print(real_data[0])
print(datetime.datetime.fromtimestamp(real_data[0]['issue_time']))
il|z|6#H*$qP