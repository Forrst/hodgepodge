#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-14 下午3:09
'''

from log.const import const,PI
import logging
logging.config.dictConfig(const.LOGGING)
logger = logging.getLogger('area.py')

# logging.basicConfig(level=logging.DEBUG,
#                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
#                    datefmt='%m-%d %H:%M',
#                    filename='/home/eos/log/python/mrjaryn/area.log',
#                    filemode='a')
# logger = logging.getLogger('area')
def calc_round_area(radius):
    try:
        a = 1/0
    except Exception,e:
        logger.error(e,exc_info=True)
    return PI * (radius ** 2)

def main():
    logger.info("")
    logger.info("round area: %d", calc_round_area(2))

main()