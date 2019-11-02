#!/usr/bin/python
# -*- coding:utf-8 -*-
'''
作者:jia.zhou@aliyun.com
创建时间:2019-06-28 上午10:41
'''
import os
from email.header import Header
from email.mime.application import MIMEApplication
from email.utils import parseaddr, formataddr

from db.mysql.SqlUtil import Mysql
from email.mime.image import MIMEImage

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import datetime
import subprocess
_user = "cs@juniorchina.com"
_from = "尊嘉金融分析研究部 <cs@juniorchina.com>"
_pwd = "Today123"
_geter = ["jia.zhou@juniorchina.com"]
# _geter = ["qingtao.kong@juniorchina.com","wei.li@juniorchina.com","linyun.zhu@juniorchina.com"]
# _geter = ["qingtao.kong@juniorchina.com","jia.zhou@juniorchina.com","wei.sun@juniorchina.com","jia.zhou@aliyun.com","295627520@qq.com"]
def _format_addr(s):
    name,addr = parseaddr(s)
    #将邮件的name转换成utf-8格式，addr如果是unicode，则转换utf-8输出，否则直接输出addr
    return formataddr((Header(name,'utf-8').encode(), addr.encode("utf-8") if isinstance(addr,unicode) else addr))


# 获取邮箱列表
# sql = "select email from search_subscription where status = 0"
# emails = Mysql.execute(sql,db="miningstock")
# _geter = [i[0] for i in emails]

weekday = {0:"一",1:"二",2:"三",3:"四",4:"五",5:"六",6:"日"}
now = datetime.datetime.now()-datetime.timedelta(days=1)

src = "https://t.financialdatamining.com/news/19/china_concept_{}.html".format(now.strftime("%Y%m%d"))
des = os.getcwd()+"/"+'尊嘉金融–{}年{}月{}日（周{}）美股市场分析报告.pdf'.format(now.year,now.month,now.day,weekday[now.weekday()])
cmd = "/usr/local/bin/wkhtmltopdf --margin-top 2cm --margin-right 2cm --margin-left 2cm --margin-bottom 2cm --page-size  A3 --header-spacing 5 --footer-right [page]/[topage] {} {}".format(src,des)

sub = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE)
sub.wait()

attachment = MIMEApplication(open(des, 'rb').read())
filename = "尊嘉金融–{}年{}月{}日（周{}）美股市场分析报告.pdf".format(now.year,now.month,now.day,weekday[now.weekday()])
attachment.add_header('Content-Disposition', 'attachment', filename=Header(filename,'utf-8').encode())

subject = "尊嘉金融 – {}年{}月{}日（周{}）美股市场分析报告".format(now.year,now.month,now.day,weekday[now.weekday()])


filelist = ["img/header.png","img/gz_erweima.png","img/xz_erweima.png"]


title = "{}年{}月{}日美股开市汇报".format(now.year,now.month,now.day)
title_en = "US Stock Market Report of {}".format(now.strftime("%Y/%m/%d"))
report_time = "报告时间：{}年{}月{}日{}:{}:{}".format(now.year,now.month,now.day,now.hour,now.minute,now.second)
report_time_en = "Time of Report：{}".format(now.strftime("%Y/%m/%d %H:%M:%S"))
url = "https://t.financialdatamining.com/news/19/china_concept_{}.html".format(now.strftime("%Y%m%d"))
server = smtplib.SMTP_SSL("mail.juniorchina.com", 465)
server.login(_user,_pwd)
for user in _geter:
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject.decode("utf-8")
    msgRoot['From'] = _format_addr(_from)
    msgRoot['To'] = user
    msgRoot.attach(attachment)
    index = 0
    for file in filelist:
        p = os.getcwd()
        fp = open(p+"/"+file, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()
        msgImage.add_header('Content-ID', '<'+str(index)+'>')
        index += 1
        msgRoot.attach(msgImage)
    unsubscribe = "http://192.168.2.218:7501/unsubscribe?email={}".format(user)
    html ='''
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
	<title>邮件模板</title>
</head>
<body>
<table width="780" border="0" align="center" cellpadding="0" cellspacing="0" >
	<tr><td height="40">&nbsp;</td></tr>
	<tr>
		<td><img src="cid:0" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
	</tr>

	<tr>
		<td style="border-left: 1px solid #efefef;border-right: 1px solid #efefef;border-bottom: 1px solid #efefef;background: #fff;">
			<table border="0" align="center" cellpadding="0" cellspacing="0">
				<tr>
					<td>
						<table width="672" border="0" align="center" cellpadding="0" cellspacing="0" >
							<tr><td height="54">&nbsp;</td></tr>
							<tr>
								<td valign="top" style="font-size:24px; line-height: 26px; color: #02A4D2; text-align: left; font-weight:500;">{}</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:14px; line-height: 30px; color: #7F8286; text-align: left; font-weight:400;">{}</td>
							</tr>
							<tr><td height="60">&nbsp;</td></tr>
							<td valign="top" style="font-size:16px; line-height: 22px; color: #252D36; text-align: left; font-weight:400;">{}</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:14px; line-height: 30px; color: #7F8286; text-align: left; font-weight:400;">{}</td>
							</tr>
							<tr>
								<td>
									<table  border="0" align="center" cellpadding="0" cellspacing="0">
										<tr>
											<td height="16"></td>
										</tr>
									</table>
								</td>
							</tr>				
							<tr>
								<td valign="top" style="font-size:16px; line-height: 22px; color: #252D36; text-align: left; font-weight:400;">报告类型：市场分析</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:14px; line-height: 30px; color: #7F8286; text-align: left; font-weight:400;">Type of Report：Market Analysis</td>
							</tr>
							<tr>
								<td>
									<table  border="0" align="center" cellpadding="0" cellspacing="0">
										<tr>
											<td height="10"></td>
										</tr>
									</table>
								</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:16px; line-height: 40px; color: #252D36; text-align: left; font-weight:400;">报告详情：点击链接<a href="{}" style="color: #02A4D2; text-decoration: none;">美股行情回顾与后市策略</a>，或下载附件PDF文档查看。</td>
							</tr>
							<tr>
								<td valign="top" style="font-size:14px; line-height: 18px; color: #7F8286; text-align: left; font-weight:400;">Details of Report：click the following link <a href="{}" style="color: #02A4D2;text-decoration: none;">US Stock Market Review & Trading Strategy for Reference</a>, or download the attached PDF file for details.</td>
							</tr>
						</table>
					</td>
				</tr>
				<tr><td height="140">&nbsp;</td></tr>
				<tr>
					<td>
						<table width="692" border="0" align="center" cellpadding="0" cellspacing="0">
							<tr>
								<td height="1" style="background: #efefef;"></td>
							</tr>
						</table>
					</td>
				</tr>
				<tr>
					<td>
						<table width="692" border="0" align="center" cellpadding="0" cellspacing="0">
							<tr><td colspan="4" height="32">&nbsp;</td></tr>
							<tr>
								<td width="405">
									<table width="405" border="0" align="center" cellpadding="0" cellspacing="0" style="font-size:14px; line-height: 16px; text-align: left; color: #7f8286;">
										<tr><td>如有任何问题，可以与我们联系，我们会尽快为您解答。</td></tr>
										<tr><td height="35">&nbsp;</td></tr>
										<tr><td>香港咨询：852-31690319</td></tr><tr><td style=" line-height: 38px;">大陆咨询：400-039-0319</td></tr>
										<tr><td>公司官网：<a style="color: #7f8286; text-decoration: none;" href="https://www.juniorchina.com" target="_blank">www.juniorchina.com</a></td></tr>
									</table>
								</td>
								<td width="114"><img src="cid:2" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
								<td width="36">&nbsp;</td>
								<td width="129"><img src="cid:1" alt="尊嘉证券"/></td><!-- 图片路径使用绝对路径 -->
							</tr>
						</table>
					</td>
				</tr>
				<tr><td height="26">&nbsp;</td></tr>
			</table>
		</td>
	</tr>
	<tr><td height="45" >&nbsp;</td></tr>
	<tr>
		<td>
			<table border="0" align="center" cellpadding="0" cellspacing="0" width="684" style="font-size: 12px; color: #808080; line-height: 18px;">
				<tr>
					<td style="font-weight: bold;">免责声明：</td>
				</tr>
				<tr>
					<td>本邮件由从事证券及期货条例（香港法例第571章）中第4类（就证券提供意见）受规管活动，及第1类（证券交易）受规管活动之持牌法团尊嘉证券国际有限公司（以下简称“本公司”）发出。本邮件使用的数据均来源于互联网和本公司研究人员认为可信的数据源，本公司研究人员对这些信息的准确性和完整性不作任何保证；本报告中的资料、意见和预测均仅反映本报告发布时的资料、意见和预测，可能在随后会做出调整；本公司研究人员已力求报告内容的客观、公正，但文中的观点、结论和建议仅供参考，不构成投资者在投资等方面的最终操作建议，亦不对投资者最<br />终投资结果承担任何责任。证券价格可升可跌，甚至变成毫无价值。投资有风险，入市需谨慎。在任何情况下，本公司不对任何人因使用本邮件中的任何内容所引致的任何损失负任何责任。</td>
				</tr>
				<tr><td>&nbsp;</td></tr>
				<tr>
					<td>如您日后不希望接收任何有关本公司产品及服务的推广资讯，请<a href="{}" style="color: #20BAE6; text-decoration: none;">按此</a>取消订阅。在收到您的取消订阅请求后，我们会尽快停止向您发送此类邮件且不收取任何费用，在这之前您仍有机会收到本公司发出的推广资讯，如有不便，敬请谅解。 <br />
If you do not wish to receive any marketing materials regarding our products and services from us thereafter, please click <a href="{}" style="color: #20BAE6; text-decoration: none;">unsubscribe</a>. Accordingly, we shall handle your request as soon as possible to stop using your personal data for direct marketing purpose without charge. Since it may have time difference in handling your request, please accept our apology if you still receive similar email or promotion from us during the processing period.</td>
				</tr>
				<tr><td>&nbsp;</td></tr>
				<tr>
					<td>尊嘉证券国际有限公司 Zinvest Global Limited<br />
					香港金钟道89号力宝中心2座1702室 Room 1702, Tower 2, Lippo Centre, 89 Queensway, Hong Kong</td>
				</tr>
				<tr><td>&nbsp;</td></tr>
				<tr>
					<td>**此邮件为系统邮件，无需回复**<br />
					**This email is an automated notification. Please DO NOT reply to this email.**</td>
				</tr>
				<tr><td>&nbsp;</td></tr>
				<tr>
					<td>©  2019尊嘉证券国际有限公司  版权所有<br />
						Copyright © 2019 Zinvest Global Limited<br />
						All rights reserved
					</td>
				</tr>
			</table>
		</td>
	</tr>
	<tr><td height="45" >&nbsp;</td></tr>

</table>
</body>
</html>
'''.format(title,title_en,report_time,report_time_en,url,url,unsubscribe,unsubscribe)
    msgText = MIMEText(html, 'html', 'utf-8')
    msgRoot.attach(msgText)
    server.sendmail(_user,user,msgRoot.as_string())
server.quit()