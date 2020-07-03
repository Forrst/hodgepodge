#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-05-18 上午11:32
'''
#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2019-07-22 下午2:35

'''
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr


_user = "cs@juniorchina.com"
_from = "尊嘉金融 <cs@juniorchina.com>"
_pwd = "Today123"
_geter = ["295627520@qq.com","jia.zhou@juniorchina.com","kelvin.wang@zinvestglobal.com"]
def _format_addr(s):
    name, addr = parseaddr(s)
    # 将邮件的name转换成utf-8格式，addr如果是unicode，则转换utf-8输出，否则直接输出addr
    return formataddr((Header(name), addr))

html = '''
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>追收保证金通知</title>
</head>
<body>
<table width="780" border="0" align="center" cellpadding="0" cellspacing="0" >
	<tr><td height="40">&nbsp;</td></tr>
	<tr>
		<td><img src="http://t.financialdatamining.com/static/images/header.png" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
	</tr>

	<tr>
		<td style="background: #fff;">
			<table width="100%" border="0" align="center" cellpadding="0" cellspacing="0">
				<tr>
					<td>
						<table width="692" border="0" align="center" cellpadding="0" cellspacing="0">
							<tr><td height="75">&nbsp;</td></tr>
							<tr>
								<td height="46" valign="top" style="font-size:18px; line-height: 25px; color: #252D36; text-align: left; font-weight:bold;">尊敬的客户您好：</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:16px; color: #000; line-height: 20px; text-align: left;">
                                <p>&nbsp;&nbsp;&nbsp;&nbsp;经我司风控部监测，您账户的风险值过高，请于2个工作日内尽快补交部分欠款，或出售部分股票，以降低风险值达到账户的保证金最低要求，否则我司可能会为您强制平仓。如您有任何疑问，请致电400-039-0319。</p>
                                </td>
							</tr>
							<tr><td height="26">&nbsp;</td></tr>
							<tr><td height="60">&nbsp;</td></tr>
							<tr>
								<td  valign="top" style="font-size:16px;font-weight:bold; color: #252D36; line-height: 30px;">谢谢<br />尊嘉证券国际有限公司</td>
							</tr>

						</table>
					</td>
				</tr>
				<tr><td height="56">&nbsp;</td></tr>
				<tr>
					<td style=" background: #FAFAFA;">
						<table width="692" border="0" align="center" cellpadding="0" cellspacing="0">
							<tr><td colspan="4" height="32">&nbsp;</td></tr>
							<tr>
								<td width="435">
									<table width="100%" border="0" align="center" cellpadding="0" cellspacing="0" style="font-size:14px; line-height: 16px; text-align: left; color: #7f8286;">
										<tr><td>如有任何问题，可以与我们联系，我们会尽快为您解答。</td></tr>
										<tr><td height="35">&nbsp;</td></tr>
										<tr><td>香港咨询：852-31690319</td></tr>
										<tr><td style=" line-height: 38px;">大陆咨询：400-039-0319</td></tr>
										<tr><td>公司官网：<a style="color: #7f8286; text-decoration: none;" href="https://www.zinvestglobal.com" target="_blank">www.zinvestglobal.com</a></td></tr>
									</table>
								</td>
								<td width="112"><img src="http://t.financialdatamining.com/static/images/xz_erweima.png" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
								<td width="30">&nbsp;</td>
								<td width="126"><img src="http://t.financialdatamining.com/static/images/gz_erweima.png" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
							</tr>
							<tr><td colspan="4" >&nbsp;</td></tr>
							<tr><td colspan="4" align="center" style="color: #7f8286; font-size: 12px; line-height: 14px;">此邮件为系统邮件，无需回复 &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;©2017尊嘉证券国际有限公司   版权所有</td></tr>
							<tr><td colspan="4" >&nbsp;</td></tr>
						</table>
					</td>
				</tr>
				
			</table>
		</td>
	</tr>
	<tr><td height="45" >&nbsp;</td></tr>

</table>
</body>
</html>
'''



# dbs = Mysql("mysql5.105")
# sql = '''
# SELECT
# 	funds_account,email
# FROM
# 	miningaccount.account_info b
# LEFT JOIN miningaccount.id_info c ON b.account_id = c.account_id
# WHERE c.email IS NOT NULL
#     '''
# emails = dbs.execute(sql,db="miningaccount")
# _geter = []
# for i in emails:
#     if i[0] in b:
#         _geter.append(i[1])
# _geter = [email[0] for email in emails]
# _geter = ["295627520@qq.com"]


subject = "追收保证金通知"
server = smtplib.SMTP_SSL("mail.juniorchina.com", 465)
server.login(_user, _pwd)
for user in _geter:
    try:
        msgRoot = MIMEMultipart('related')
        msgRoot['Subject'] = subject
        msgRoot['From'] = Header(_from,"utf-8")
        msgRoot['To'] = user
        msgText = MIMEText(html, 'html', 'utf-8')
        msgRoot.attach(msgText)
        server.sendmail(_user, user, msgRoot.as_string())
        print("send email to user {} success".format(user))
    except Exception as e:
        print(e)
        print("error email:",user)
server.quit()

# if __name__ == "__main__":
#     job()