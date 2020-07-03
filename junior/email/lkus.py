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
# _geter = ["jia.zhou@juniorchina.com","kelvin.wang@zinvestglobal.com"]
def _format_addr(s):
    name, addr = parseaddr(s)
    # 将邮件的name转换成utf-8格式，addr如果是unicode，则转换utf-8输出，否则直接输出addr
    return formataddr((Header(name), addr))
emails = '''
settlement@huatongzq.com
443571074@qq.com
hillwong@qq.com
2574095@qq.com
694129133@qq.com
1015761563@qq.com
631695154@qq.com
zhangyaonick@163.com
958104336@qq.com
1625925277@qq.com
1059887625@qq.com
orzlollipop@outlook.com
ligs2001@163.com
65183210@qq.com
Ldj123668@163.com
867766860@qq.com
zfx0906@qq.com
chenqian_cd@163.com
553882335@qq.com
1429303165@qq.com
383995100@qq.com
jesonwayne@foxmail.com
369152880@qq.com
sunsi426@163.com
455896805@qq.com
514498144@qq.com
zfx0906@163.com
vipfoon@163.com
jeffdeen@qq.com
582625956@qq.com
729730@163.com
286882298@qq.com
122342387@qq.com
583816522@qq.com
15154627377@139.com
651449765@qq.com
stephenyao0822@163.com
13815860097@139.con
13810935923@139.com
caoyaran@yeah.net
825644665@qq.com
mdfmdf@126.com
settlement@usmarthk.com; hkdealing@youxin.com'''

_geter = emails.split("\n")
html = '''
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>瑞幸咖啡复牌通知</title>
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
								<td height="46" valign="top" style="font-size:18px; line-height: 25px; color: #252D36; text-align: left; font-weight:bold;">尊敬的客户：</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:16px; color: #000; line-height: 20px; text-align: left;">
                                <p>&nbsp;&nbsp;&nbsp;&nbsp;您好，您持有的瑞幸咖啡LK:US，将于今晚复牌，该股有退市风险，敬请留意。</p>
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
subject = "瑞幸咖啡复牌通知"
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
