#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2018-12-26 下午2:35
'''
from pyhanlp import HanLP
# HanLP = JClass('com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer')
# HanLP = JClass('com.hankcs.hanlp.HanLP')
hanlp = HanLP
title = "中信资本踩雷*ST凯迪，多只私募产品风险金额或超16亿元"
content = '''
近日，多位投资者及知情人向第一财经记者爆料，中信资本控股有限公司（下称“中信资本”）旗下子公司中信资本（深圳）资产管理有限公司（下称“中信资本深圳资管公司”）的多只私募产品踩雷*ST凯迪（000939.SZ，下称“凯迪生态”），涉及风险金额或超16亿元。

“有些已经到期，但无法兑付，只能发延期公告；有些还没有到期，但后续兑付存在极大的不确定性。”该知情人士说。

据了解，中信资本为中信系旗下一级子公司平台，官网显示其主攻另类投资，成立于2002年，总部位于香港，核心业务包括私募股权投资​、不动产投资、结构融资及​资产管理，管理资金达250亿美元，在香港、上海、北京、深圳、​东京和纽约等地设有子公司或办事处。

在第一财经记者获取的多份私募基金产品的宣传资料上，中信资本介绍称其拥有强大的股东背景，第一大股东为中信股份（00267.HK），其他主要股东均为各自领域的领军企业，包括腾讯控股、富邦人寿和卡塔尔控股公司。

根据中信资本深圳资管公司官网介绍，该公司为中信资本全资子公司，注册地址为深圳前海，目前主要围绕资本市场和房地产业开展夹层融资、并购基金、地产基金、机遇投资、创新金融等业务。

天眼查显示，中信资本深圳资管公司法定代表人为张渺，张渺同时也是中信资本高级董事总经理、结构融资部执行合伙人。

据知情人士透露，张渺工作地之一是香港，他在内地主要组建了两个团队，一是深圳团队，一是北京团队，以债权类业务为主，中信资本深圳资管公司正是其组建的深圳团队。

而中信资本深圳资管公司同时又是深圳信诚基金销售有限公司（下称“深圳信诚”）的股东，后者的法人代表为周文。周文此前曾在上海信诚资产管理有限公司（下称“上海信诚”）任董事（2018年3月26退出）。

据上述知情人士透露，中信资本深圳资管公司的大部分产品都是经由深圳信诚来进行销售的。

2017年9月，深圳信诚买下了深圳前海欧中联合基金的销售牌照，成为持牌机构。

第一财经记者也在深圳信诚官网找到了涉及牌照的相关报道：“我司因经营发展需要，自2017年10月1日起公司名称由原‘深圳前海欧中联合基金销售有限公司’变更为‘深圳信诚基金销售有限公司’。 ”

知情人士也透露，周文出身中信信托，原任职于中信信托旗下一家名为北京聚信恒融资产管理有限公司（现改名为“北京鼎信恒融资产管理有限公司”）的财富公司，该公司曾专门销售中信信托的产品，而后周文又参与组建上海信诚。

第一财经记者也从相关渠道获得此次中信资本深圳资管公司部分踩雷的凯迪生态项目产品信息。

比如，由中信资本深圳资管公司作为管理人成立了迪信系列私募投资基金：迪信1号/2号/3号/4号/5号/8号（备案编号：SN4621／SN4716／SN4717／SN8951／SR5936／ST3452），产品期限24个月，成立时间在2016年11月17日~2017年5月8日之间，总金额超10亿元。

第一财经记者同时获得的一份关于“迪信壹/贰号私募投资基金（契约型私募基金）”的介绍PPT中显示，期限24个月，封闭式运作，总规模约1.5亿元，业绩比较基准 8%/年。

另外，中信资本深圳资管公司还在2017年8月~12月之间成立了信阳1号/2号私募基金产品（备案编号：SW3308／SW3553），金额总计6.23亿元。

而根据记者获得的一份加盖“中信资本（深圳）资产管理有限公司”公章、名为《信阳贰号私募投资基金说明函》的文件，该系列基金最终投向为受让武汉凯迪电力工程有限公司所持有的凯迪生态作为最终支付义务的应收账款。

目前这两款产品均在运行中，而融资方凯迪生态已是身陷囹圄。自今年5月7日凯迪生态发生中票兑付违约以来，其大量债务逾期，资金、资产遭冻结，关联方资金占用等问题便接踵而至，股票曾连创24日跌停，目前已进入“1元股”行列。

公告显示，目前凯迪生态逾期债务共计56.23亿元，最近一期经审计的净资产为106.33亿元，逾期债务占最近一期经审计净资产的比例为52.88%，部分债权人根据债务违约情况已经采取提起诉讼、仲裁、冻结银行账户、冻结资产等措施。

比如，中铁信托申请查封了凯迪生态及其大股东阳光凯迪新能源集团有限公司（下称“阳光凯迪”）的财产；恒泰证券于2018年8月10日向中国国际经济贸易仲裁委员会申请财产保全，请求在2.1亿元限额范围内，对被申请人来凤凯迪绿色能源开发有限公司和凯迪生态予以财产保全。

深圳平安大华汇通财富管理有限公司也要求对其4亿元的财产采取查封、冻结或扣押。泰信基金管理有限公司则要求查封、扣押、冻结被申请人银行存款6895.89万元或相应价值的财产。

公开信息显示，中信资本深圳资管公司于2018年5月7日向法院申请诉前财产保全，请求对被申请人凯迪生态、武汉凯迪电力工程有限公司、阳光凯迪的财产采取保全措施，查封被申请人价值6.74亿元的财产。

据了解，目前中信资本深圳资管公司正在对该项目进行司法追索。根据第一财经记者拿到的一份反馈说明，管理人与湖北省高院持续沟通，促请法院尽快出具司法判决。

另外，凯迪生态持续与湖北省政府、凯迪生态当前管理层及融资团队进行沟通，了解包括债务重组、资产重组事项进展。 同时也与部分资产意向买方保持持续沟通，为后续资产处置做准备。

而对于广大的债权人来说，凯迪生态如今推进的“股权重组+资产处置+债务重构”的自救方案能否自救？还有待观察。

本文作者安卓，来源于第一财经，原标题《中信资本踩雷*ST凯迪，多只私募产品风险金额或超16亿元》
'''
context = title+"\t"+content
import re
sentences = re.split(u'[^\u4e00-\u9fa50-9a-zA-Z|%]+', context.decode("utf-8"))
sentences = [i for i in sentences if i.strip() !=""]

import jieba
sentence_words = ["|".join(jieba.cut(i)) for i in sentences]
seg = jieba.cut(sentences)
print " ".join(seg)
print hanlp.extractKeyword(context,20)
phrases = hanlp.extractPhrase(context,5)
newWords = hanlp.extractWords(context,10,True)
print(newWords)

