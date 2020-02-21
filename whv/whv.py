#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2019-11-05 下午2:11
'''
import json
from email.mime.text import MIMEText

import requests
import threading
import logging

logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(process)d:%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s')
logger =logging.getLogger("whv")

#获取点击预约的地方
'''
GET https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg=&_ga=2.109607557.145996771.1573444497-1106728563.1573444497 HTTP/1.1
Host: workandholiday.vfsglobal.com
Connection: keep-alive
Cache-Control: max-age=0
DNT: 1
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36
Sec-Fetch-User: ?1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
Sec-Fetch-Site: cross-site
Sec-Fetch-Mode: navigate
Referer: https://www.vfsglobal.cn/Australia/China/schedule_an_appointment.html
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7
Cookie: _hjid=391aa627-017c-4888-8d85-bb36e3d8086a; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe; _ga=GA1.2.1106728563.1573444497; _gid=GA1.2.145996771.1573444497
'''




'''
点击预约的按钮
POST https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.109607557.145996771.1573444497-1106728563.1573444497 HTTP/1.1
Host: workandholiday.vfsglobal.com
Connection: keep-alive
Content-Length: 943
Cache-Control: max-age=0
Origin: https://workandholiday.vfsglobal.com
Upgrade-Insecure-Requests: 1
DNT: 1
Content-Type: application/x-www-form-urlencoded
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36
Sec-Fetch-User: ?1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: navigate
Referer: https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg=&_ga=2.109607557.145996771.1573444497-1106728563.1573444497
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7
Cookie: _hjid=391aa627-017c-4888-8d85-bb36e3d8086a; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe; _ga=GA1.2.1106728563.1573444497; _gid=GA1.2.145996771.1573444497; ASP.NET_SessionId=gi4f0vawjqdliawgjj15rrsj

__EVENTTARGET=ctl00%24plhMain%24lnkSchApp&__EVENTARGUMENT=&__VIEWSTATE=ckdNk%2FeaL3xXf3r8Wl%2FMq1gc1TI8CUAZhmvhWi0JqFlVTGkgYbmX%2FAh4QlmVjL2YW%2FbxlgkQCGd2n1QftfKU3e2QFxioj%2BbA5%2Bz3oK3yo3aqqULJjc5bd4LWOg6umMQlLFh%2BMHiFhFwvs9QTJMPF7mAJNRwkFsOIHXAh%2BzP40c%2FzBd8cfFZ7wYBOW%2FEvxopEgqsxvT9bBVm%2BG%2Flbxr8s1St3v5LYfb3y0TYD%2BoVVO4vrJEvFtspwUqbBpGcCoOImTP%2FxYT4Bwz%2Bg9GZeLGsw9bzk2%2FCbit8O70C2hQlja8x9ybuM%2Fw8hlNFE7RLAD8abB2WWYFIbGmKpq88NdlpJwwtlwz0oZKzikShRwZItlwhWEsKeFvsUMh9XhvjLl6v5%2FeDalSsEe1NqXyr%2FTXQwEy1PXW6qsG8rwKtFgrVRXiuMIUImXEzUXoM9frcnLZOqiJaNbqTyknsE1HOqJRrj9JRWBpz9XgUjhnhFYI0e2i6kIQecLZknhglqiHJVJ00jS%2BkZsner2G%2B993Mhj4ladRldErVef7CrfQvxGR9sG3x7ZC%2BA&____Ticket=1&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=E31uPqXmoWdSaQJNcQ6tCEIRiWXrTRb1IDoVDryM%2BiMWOZkvmaS1QX419UHZD6GYKT8HG9oosnsYMYRQfbW%2FmqmXCV8yDnpqF%2B1cAR88yt%2FLA7s9dwFa9ocUpPv8BInrkUs6ipNxKTyabtJcRob9Qib%2B0MX61kiNdOjtUJ0XSxGDXYC5srm9SAUvynUDAm7ub4n6Gw%3D%3D
'''






status_code = 0
while status_code != 200:
    try:
        url = "https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg="
        data = {
            "Host": "workandholiday.vfsglobal.com",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
            "Sec-Fetch-User": "?1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "navigate",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Cookie": "_hjid=391aa627-017c-4888-8d85-bb36e3d8086a; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe; _ga=GA1.2.1106728563.1573444497; _gid=GA1.2.145996771.1573444497"
        }
        r = requests.get(url,data)
        status_code = r.status_code
        logger.info(r.text)
        if r.status_code == 200:
            logger.info("*********** SUCCESS POST ***********")
            break
    except Exception,e:
        logger.error(e,exc_info = True)
        continue

#发送城市的地址
status_code = 0
while status_code != 200:
    try:
        url = "https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?"
        header = {
            'Host': 'workandholiday.vfsglobal.com',
            'Connection': 'keep-alive',
            'Content-Length': '919',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Origin': 'https://workandholiday.vfsglobal.com',
            'Upgrade-Insecure-Requests': '1',
            'DNT': "1",
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
            'Sec-Fetch-User': '?1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Referer': 'https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppWelcome.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg=&_ga=2.237263808.1314286643.1573436000-1998338386.1572931695',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
            "Cookie": "_hjid=391aa627-017c-4888-8d85-bb36e3d8086a; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe; _ga=GA1.2.1106728563.1573444497; _gid=GA1.2.145996771.1573444497"
        }
        data = '''__EVENTTARGET=ctl00%24plhMain%24lnkSchApp&__EVENTARGUMENT=&__VIEWSTATE=BfetTtFVMXTXr1h0y%2Fu2LLm4USoXl4NiNrcjuTLnngsP6BgGzA%2B%2B%2B6BsTRWjBdG64vHmFgxrDT5FlYlhxR6SJqmhm702LJ5n9DJSVeaY7j7m8BfYggDguLIWmv3TIACHxSCU7uEwVLFbHK%2FhK3vog66I8WDkRzPCq3r9pLwQqXx5cDtUWWOd79EtZed4NmTYc6Gcc0WNf8iyqRegctrK%2F599uV3XVB1bd3IJsZ0sFqITKBqsj278PP%2F%2FkZFsruJSATxyJ8KkiH%2B8xUQ6cneHxEkFWLpJh5J2I6k5C6zCCsTpQOuZFXt4Ib5A7VsHTtlDuKMJ%2B03ATnrWwCDe6zxsJcUFsDNuDqhvEgJwpWMkI8FiDJKCv0sEExi4VsOxRe6hmlAAwlChuF6nWhH2wQarYGxPkTOoodP18SzfCHdClQ1YKmwUfS3HWdps%2BIAKUMP9b5e8gmcSurGe0cwmme7S92jtSKLZo6Yth3ThBo7%2F3yVFc2IHR60ZXDBK8xetigNTvCIutwUqfbwnsxqR%2B6YbHn5eyqbNTmV1nwk6crLVKGVVX0FY&____Ticket=1&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=p6XfjojjmgUV6FPIGgj8a2opU1oz3SaoZi7OUqytiiFTu9g9BH7XC3yvsm4autnAe%2BeWFO%2B3mB9X9rdSSKdJnFDNmNRQI9Brnkx3mbcGwZUV1NmOYY9GQTT1PfLec8VFyYdeNUxdRDaeqHBeGyINVwOA6CzKPsV1SF9ZbmeKQot3UNZGY9aRPEZleqNq3Y0lAcny7A%3D%3D'''
        r = requests.post(url,headers=header,data=data)
        logger.info(r.text)
        status_code = r.status_code
        if r.status_code == 200:
            logger.info("*********** SUCCESS POST ***********")
            break
    except Exception,e:
        logger.error(e,exc_info = True)
        continue

# POST https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.133641934.1314286643.1573436000-1998338386.1572931695 HTTP/1.1


'''
POST https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.237263808.1314286643.1573436000-1998338386.1572931695 HTTP/1.1
Host: workandholiday.vfsglobal.com
Connection: keep-alive
Content-Length: 1832
Cache-Control: max-age=0
Origin: https://workandholiday.vfsglobal.com
Upgrade-Insecure-Requests: 1
DNT: 1
Content-Type: application/x-www-form-urlencoded
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36
Sec-Fetch-User: ?1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: navigate
Referer: https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.237263808.1314286643.1573436000-1998338386.1572931695
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7
Cookie: _ga=GA1.2.1998338386.1572931695; _hjid=391aa627-017c-4888-8d85-bb36e3d8086a; _gid=GA1.2.1314286643.1573436000; ASP.NET_SessionId=wlixh0j1rtb3ul2gbfmcvkcn; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe

__VIEWSTATE=6b7dv9oIg1pON0Duot4zu3Q6hgbWNJBCPlt8%2BX15UoTZkJdiQwGDs3xf9m7gUjDa%2FBSe7ffkDS39tatg9c6TV6ks3W1t2nowa67aR2p4gds9%2FeObHSIH8svz%2BwDOQ8bXXmhBGet2KMrfpFqLtUJxkCo0MABH1ITNk3AI%2Fyo%2Bub374RhCi7xZ%2FOCfQrFioTWxFPBU27m6EtBXiDieKSI8yRxMPRZHywuzw5DoEMQVfnONR5WRoOnj%2FUG2Fcb8n%2FyRUaq9uei%2BTBt3iWfgMIw%2Be0e0gqG5zmFEw%2Basg%2F8GEhJOdfU8U073XGtb75eFttUj0E8FBgPrqYI8W2Y0FZSG7P6t4d2JM%2F4Sr3iCeBTFnfHe5LLRhz30gKrg16AKjupQnfOWPf%2F%2Bi4%2FqZ59Spc%2BuSgAzYl9PeqLNqSXiQnosl36BWR2qV7Nyl30zs9ADJc9uPPc1RANeu%2B6g439KDGmI%2F8vYjvgaeeKUnfTWLP%2BxmkXOQkUsLMJLCFLUzCxpMmvNng9SQuz9frYYXoB1%2FoL71jcPLwM%3D&____Ticket=3&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=kpHBR7T5QZp48ZUbnalz6sFsTankySZvxq7llTQk4F%2Bi4zxrlSoESM8WUU15U7qEpbKw8mrr5CIO%2B5PavzS5fHbXQxX1qUyELr5zmjVLWB9hwdlzyG38VSoKUWtle7Ssxdg3ebEA0mJASqhw6fZu8fm46R4syFrz7TmbvhjMewLTsIanCF5ov3gspl6lJYxFAOEJDDQah8OFUuMU%2Fw3lZrisykUHF7Bp8vkV6no3rLE1eOX6NKiczFsK3xOw9spd1WRGG%2FOpnzUrAUxRyEohZU759UqwFqz8NUaRv%2B1%2BoYCvIaDj&ctl00%24plhMain%24cboVAC=28&g-recaptcha-response=03AOLTBLRgs8z4kS58cXpoFF9rZEShyUkZBlbQuICQlDWXUmqQWY6zAyhQ1af-CiyMCAroJ1juli136GnuYySlaTatAp2zhujdI3vx2HqtR4hWnduWwqjsQD_fZ-k1CHAhdhFjRxxJUPxhPPVPc67wFGsTTg_oAZLlAPGx5VQpQUK7qGvhex3m3Fuq5wkyN47PovXYVy7VzOFVHpAbw-XpAEydIxJExOtk_oDSFnF2_Y6nV5v14REwspczNIdyneHQxVy6K5waqyyJUwIzZUB_gkVSEx7eOA-nceQmwgiIrdEJLw4TMWf2o2fGiBR-KhjOI8G3LA8t0L9ZfqXc7Ck8sp2PejGsPm2COPkjkUQDhgyJfVZbB8nwbNts3B9aQAtN7wQNWfC2zT6FMxJGl_0fDK1KLxuRplz1qU3ePHbTe2Za-AzV5L5Z75crVyZy6fGVCnBSPm-PbLAkgeMMehWXx4oG5tPXupLBxCxsGnSoz6Ez1OOIGNWKM5L82Ial5xruak1fObY3WsbdtCZZ6ZnVDAzCWFX4IbD9-g&ctl00%24plhMain%24btnSubmit=%E6%8F%90%E4%BA%A4&ctl00%24plhMain%24hdnValidation1=%E8%AF%B7%E9%80%89%E6%8B%A9%EF%BC%9A&ctl00%24plhMain%24hdnValidation2=%E7%AD%BE%E8%AF%81%E7%94%B3%E8%AF%B7%E4%B8%AD%E5%BF%83&ctl00%24plhMain%24hdnValidation3=%E5%B1%85%E4%BD%8F%E5%9B%BD
'''

'''
POST https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppSchedulingGetInfo.aspx HTTP/1.1
Host: workandholiday.vfsglobal.com
Connection: keep-alive
Content-Length: 1684
Cache-Control: max-age=0
Origin: https://workandholiday.vfsglobal.com
Upgrade-Insecure-Requests: 1
DNT: 1
Content-Type: application/x-www-form-urlencoded
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36
Sec-Fetch-User: ?1
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: navigate
Referer: https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.237263808.1314286643.1573436000-1998338386.1572931695
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7
Cookie: _ga=GA1.2.1998338386.1572931695; _hjid=391aa627-017c-4888-8d85-bb36e3d8086a; _gid=GA1.2.1314286643.1573436000; ASP.NET_SessionId=wlixh0j1rtb3ul2gbfmcvkcn; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe

__EVENTTARGET=ctl00%24plhMain%24cboVisaCategory&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=6WVmdhCjoO7JwfQ%2FoRSlcIoDfjnlfcOUt4EQwf5ryaXBDW1%2FDWW8v2jIFedzQ3DcxT7tJ5ubMxt7oI%2BflHzDojfIw0kNhB10niNcEqPbtTBs%2FaF1f6o82mJTv03i9G4yRojzUjfbV7RG4mkP5UXW15Z3nkGMkhPPTjGwMx975MYqd4%2FrlcYcaI84VyJA4NkUxmChA0Sh%2FamdEPOsJSpSIpRj4Ty1WZTffCDyvqWlOml3Wjn552okei%2BrHV%2FXf%2BLbfv8DhkaofHqu5HYpQypG877IOmyMslf5f0qIZ8byOvyNMVtQxjxEqIOUs7RGFpaK8TTbRlhB3jeY%2BrYpP21EtUcEPTb2sM2ikWg1QxDgoShz8teGDFdFc6acBRhlIrnkKTxV92oCRwDIoXlu9Bem2VtEZYEGwQrWOMD7zxMU0zJJVYcpNp3FkUfAdpraNdFTNdESCXbF7nkhPTCLfL1Fpa6lUoO1x%2BH32vS%2FddB7%2FHA93r3lpwxLMs2XlL3O9EhQ8BodcNnOvk17UoGRLpVBHoiVq593yJfU9Ckx%2F56aOyisdE62OcVfz1mFDmtJFsLsgEtX3ggos6vHZnqtOgR%2Bs69r58HTZno%2BhZYVmKWmxGyVAaUMuVvT4apW5Q12uQy0ic4WeI3M9vhXZuh2%2ByTxB3ccVMaAsSdh2F1oI9a9k5lzRd%2F%2F&____Ticket=4&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=lZMEv%2BPvZU9SyqdHy7H8uT%2FcLCqasDFPsGBIJf%2FkqMUs7wxSjCcQ1ewbR4dsQ8A9f9IUtMi9EjnCQWGHpI7IBjTjHI%2F%2F%2FF7Rv1Rv7U%2FV6jQDI%2BzvCR3RZBjkR3wozZQ96k5xbu4EU3YmaQqwLUWKQXAmhANSWauSxdC6KrROub0JZ5KQk6zzqh49rsacojDWCS9I5Jmd4qjezXr0XD4IMzasd8lTKXLUWNnP4VSKyjs74MGd7zI%2BTNWYSZznZ0qHFxQYzOqQEeYNj%2BmWTEG1Cr8sfoc46gN8lLXN1XrtZiebfxIfQmfTKzxDUk%2FAgv24pixoGg%3D%3D&ctl00%24plhMain%24tbxNumOfApplicants=1&ctl00%24plhMain%24cboVisaCategory=17&ctl00%24plhMain%24btnSubmit=%E6%8F%90%E4%BA%A4&ctl00%24plhMain%24hdnValidation1=%E8%AF%B7%E8%BE%93%E5%85%A5%EF%BC%9A&ctl00%24plhMain%24hdnValidation2=%E6%9C%89%E6%95%88%E4%BA%BA%E6%95%B0%E3%80%82&ctl00%24plhMain%24hdnValidation3=%E6%8A%A5%E5%90%8D%E4%BA%BA%E6%95%B0%E5%BF%85%E9%A1%BB%E4%BB%8B%E4%BA%8E1%E5%92%8C++&ctl00%24plhMain%24hdnValidation4=%E7%AD%BE%E8%AF%81%E7%B1%BB%E5%88%AB
'''

'''
填写有效申请人数
'''

status_code = 0
while status_code != 200:
    try:
        url = "https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.133641934.1314286643.1573436000-1998338386.1572931695"
        header = {
            'Host': 'workandholiday.vfsglobal.com',
            'Connection': 'keep-alive',
            'Content-Length': '1684',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
            'Sec-Fetch-User': '?1',
            'Origin': 'https://workandholiday.vfsglobal.com',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Referer': 'https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.133641934.1314286643.1573436000-1998338386.1572931695',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
            'Cookie': '_ga=GA1.2.1998338386.1572931695; _hjid=391aa627-017c-4888-8d85-bb36e3d8086a; _gid=GA1.2.1314286643.1573436000; ASP.NET_SessionId=wlixh0j1rtb3ul2gbfmcvkcn; sess_map=be4e34bf-a934-4a28-a876-a87c068fd1fe'
        }
        data = '''__EVENTTARGET=ctl00%24plhMain%24cboVisaCategory&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=6WVmdhCjoO7JwfQ%2FoRSlcIoDfjnlfcOUt4EQwf5ryaXBDW1%2FDWW8v2jIFedzQ3DcxT7tJ5ubMxt7oI%2BflHzDojfIw0kNhB10niNcEqPbtTBs%2FaF1f6o82mJTv03i9G4yRojzUjfbV7RG4mkP5UXW15Z3nkGMkhPPTjGwMx975MYqd4%2FrlcYcaI84VyJA4NkUxmChA0Sh%2FamdEPOsJSpSIpRj4Ty1WZTffCDyvqWlOml3Wjn552okei%2BrHV%2FXf%2BLbfv8DhkaofHqu5HYpQypG877IOmyMslf5f0qIZ8byOvyNMVtQxjxEqIOUs7RGFpaK8TTbRlhB3jeY%2BrYpP21EtUcEPTb2sM2ikWg1QxDgoShz8teGDFdFc6acBRhlIrnkKTxV92oCRwDIoXlu9Bem2VtEZYEGwQrWOMD7zxMU0zJJVYcpNp3FkUfAdpraNdFTNdESCXbF7nkhPTCLfL1Fpa6lUoO1x%2BH32vS%2FddB7%2FHA93r3lpwxLMs2XlL3O9EhQ8BodcNnOvk17UoGRLpVBHoiVq593yJfU9Ckx%2F56aOyisdE62OcVfz1mFDmtJFsLsgEtX3ggos6vHZnqtOgR%2Bs69r58HTZno%2BhZYVmKWmxGyVAaUMuVvT4apW5Q12uQy0ic4WeI3M9vhXZuh2%2ByTxB3ccVMaAsSdh2F1oI9a9k5lzRd%2F%2F&____Ticket=4&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=lZMEv%2BPvZU9SyqdHy7H8uT%2FcLCqasDFPsGBIJf%2FkqMUs7wxSjCcQ1ewbR4dsQ8A9f9IUtMi9EjnCQWGHpI7IBjTjHI%2F%2F%2FF7Rv1Rv7U%2FV6jQDI%2BzvCR3RZBjkR3wozZQ96k5xbu4EU3YmaQqwLUWKQXAmhANSWauSxdC6KrROub0JZ5KQk6zzqh49rsacojDWCS9I5Jmd4qjezXr0XD4IMzasd8lTKXLUWNnP4VSKyjs74MGd7zI%2BTNWYSZznZ0qHFxQYzOqQEeYNj%2BmWTEG1Cr8sfoc46gN8lLXN1XrtZiebfxIfQmfTKzxDUk%2FAgv24pixoGg%3D%3D&ctl00%24plhMain%24tbxNumOfApplicants=1&ctl00%24plhMain%24cboVisaCategory=17&ctl00%24plhMain%24btnSubmit=%E6%8F%90%E4%BA%A4&ctl00%24plhMain%24hdnValidation1=%E8%AF%B7%E8%BE%93%E5%85%A5%EF%BC%9A&ctl00%24plhMain%24hdnValidation2=%E6%9C%89%E6%95%88%E4%BA%BA%E6%95%B0%E3%80%82&ctl00%24plhMain%24hdnValidation3=%E6%8A%A5%E5%90%8D%E4%BA%BA%E6%95%B0%E5%BF%85%E9%A1%BB%E4%BB%8B%E4%BA%8E1%E5%92%8C++&ctl00%24plhMain%24hdnValidation4=%E7%AD%BE%E8%AF%81%E7%B1%BB%E5%88%AB'''
        r = requests.post(url,headers=header)
        logger.info(r.text)
        status_code = r.status_code
        if r.status_code == 200:
            logger.info("*********** SUCCESS POST ***********")
            break
    except Exception,e:
        logger.error(e,exc_info = True)
        continue


from email.mime.multipart import MIMEMultipart
import smtplib

_user = "295627520@qq.com"
_pwd = "pxbgmgnifwurbied"
_geter = "jia.zhou@aliyun.com"

msg = MIMEMultipart()
msg['Subject'] = '发送成功'
msg['From'] = _user
msg['To'] = _geter
# mp3part = MIMEApplication(open("/home/eos/test.csv", 'rb').read())
# # mp3part = MIMEApplication(open("/root/multi_cased_L-12_H-768_A-12.zip", 'rb').read())
#
# mp3part.add_header('Content-Disposition', 'attachment', filename='test.csv')
# msg.attach(mp3part)





import json
status_code = 0
while status_code!=200:
    url = 'https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppSchedulingGetInfo.aspx'
    headers = {u'Origin': u'https://workandholiday.vfsglobal.com', u'Content-Length': u'1684', u'Sec-Fetch-User': u'?1', u'Accept-Encoding': u'gzip, deflate, br', u'Sec-Fetch-Site': u'same-origin', u'Connection': u'keep-alive', u'Accept': u'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3', u'Upgrade-Insecure-Requests': u'1', u'DNT': u'1', u'Host': u'workandholiday.vfsglobal.com', u'Referer': u'https://workandholiday.vfsglobal.com/WNH_Appointments_11Nov19/AppScheduling/AppScheduling.aspx?p=Gta39GFZnstZVCxNVy83zTlkvzrXE95fkjmft28XjNg%3d&_ga=2.237263808.1314286643.1573436000-1998338386.1572931695', u'Sec-Fetch-Mode': u'navigate', u'Cache-Control': u'max-age=0', u'Accept-Language': u'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7', u'User-Agent': u'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36', u'Content-Type': u'application/x-www-form-urlencoded'}
    cookies = {u'ASP.NET_SessionId': u'wlixh0j1rtb3ul2gbfmcvkcn', u'_hjid': u'391aa627-017c-4888-8d85-bb36e3d8086a', u'_ga': u'GA1.2.1998338386.1572931695', u'_gid': u'GA1.2.1314286643.1573436000', u'sess_map': u'be4e34bf-a934-4a28-a876-a87c068fd1fe'}
    data = {'__EVENTARGUMENT': '',
            '__EVENTTARGET': 'ctl00%24plhMain%24cboVisaCategory',
            '__EVENTVALIDATION': 'lZMEv%2BPvZU9SyqdHy7H8uT%2FcLCqasDFPsGBIJf%2FkqMUs7wxSjCcQ1ewbR4dsQ8A9f9IUtMi9EjnCQWGHpI7IBjTjHI%2F%2F%2FF7Rv1Rv7U%2FV6jQDI%2BzvCR3RZBjkR3wozZQ96k5xbu4EU3YmaQqwLUWKQXAmhANSWauSxdC6KrROub0JZ5KQk6zzqh49rsacojDWCS9I5Jmd4qjezXr0XD4IMzasd8lTKXLUWNnP4VSKyjs74MGd7zI%2BTNWYSZznZ0qHFxQYzOqQEeYNj%2BmWTEG1Cr8sfoc46gN8lLXN1XrtZiebfxIfQmfTKzxDUk%2FAgv24pixoGg%3D%3D',
            '__LASTFOCUS': '',
            '__VIEWSTATE': '6WVmdhCjoO7JwfQ%2FoRSlcIoDfjnlfcOUt4EQwf5ryaXBDW1%2FDWW8v2jIFedzQ3DcxT7tJ5ubMxt7oI%2BflHzDojfIw0kNhB10niNcEqPbtTBs%2FaF1f6o82mJTv03i9G4yRojzUjfbV7RG4mkP5UXW15Z3nkGMkhPPTjGwMx975MYqd4%2FrlcYcaI84VyJA4NkUxmChA0Sh%2FamdEPOsJSpSIpRj4Ty1WZTffCDyvqWlOml3Wjn552okei%2BrHV%2FXf%2BLbfv8DhkaofHqu5HYpQypG877IOmyMslf5f0qIZ8byOvyNMVtQxjxEqIOUs7RGFpaK8TTbRlhB3jeY%2BrYpP21EtUcEPTb2sM2ikWg1QxDgoShz8teGDFdFc6acBRhlIrnkKTxV92oCRwDIoXlu9Bem2VtEZYEGwQrWOMD7zxMU0zJJVYcpNp3FkUfAdpraNdFTNdESCXbF7nkhPTCLfL1Fpa6lUoO1x%2BH32vS%2FddB7%2FHA93r3lpwxLMs2XlL3O9EhQ8BodcNnOvk17UoGRLpVBHoiVq593yJfU9Ckx%2F56aOyisdE62OcVfz1mFDmtJFsLsgEtX3ggos6vHZnqtOgR%2Bs69r58HTZno%2BhZYVmKWmxGyVAaUMuVvT4apW5Q12uQy0ic4WeI3M9vhXZuh2%2ByTxB3ccVMaAsSdh2F1oI9a9k5lzRd%2F%2F',
            '__VIEWSTATEENCRYPTED': '',
            '____Ticket': '4',
            'ctl00%24plhMain%24btnSubmit': '%E6%8F%90%E4%BA%A4',
            'ctl00%24plhMain%24cboVisaCategory': '17',
            'ctl00%24plhMain%24hdnValidation1': '%E8%AF%B7%E8%BE%93%E5%85%A5%EF%BC%9A',
            'ctl00%24plhMain%24hdnValidation2': '%E6%9C%89%E6%95%88%E4%BA%BA%E6%95%B0%E3%80%82',
            'ctl00%24plhMain%24hdnValidation3': '%E6%8A%A5%E5%90%8D%E4%BA%BA%E6%95%B0%E5%BF%85%E9%A1%BB%E4%BB%8B%E4%BA%8E1%E5%92%8C++',
            'ctl00%24plhMain%24hdnValidation4': '%E7%AD%BE%E8%AF%81%E7%B1%BB%E5%88%AB',
            'ctl00%24plhMain%24tbxNumOfApplicants': '1'}

    html = requests.post(url, headers=headers, verify=False, cookies=cookies, data=json.dumps(data))
    status_code = html.status_code
    print(len(html.text))
    htmlsource = html.text
    print(html.text)
    if status_code == 200:
        msg.attach(MIMEText(htmlsource, 'plain', 'utf-8'))
        server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        server.login(_user,_pwd)
        server.sendmail(_user,_geter,msg.as_string())
        server.quit()