#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:eos
创建时间:2018-08-14 下午3:07
'''
import os
import datetime

PI = 3.14
class const():
    # handlers_console为True,表示日志输出在console而不是保存在本地

    def __init__(self):
        self.handlers_console  = True

        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.LOG_DIR = os.path.join(self.BASE_DIR, "logs")
        if not os.path.exists(self.LOG_DIR) and not self.handlers_console:
            os.makedirs(self.LOG_DIR)  # 创建路径

        self.LOG_FILE = datetime.datetime.now().strftime("%Y-%m-%d") + ".log"
        if self.handlers_console == True:
            self.LOG_FILE = ""
        self.LOGGING = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {
                    'format': '%(asctime)s [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s'
                },
                'standard': {
                    'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(levelname)s]- %(message)s'
                },
            },

            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "standard",
                    "stream": "ext://sys.stdout"
                },

                # "default": {
                #     "class": "logging.handlers.RotatingFileHandler",
                #     "level": "INFO",
                #     "formatter": "simple",
                #     # "filename": os.path.join(LOG_DIR, LOG_FILE),
                #     'mode': 'w+',
                #     "maxBytes": 1024*1024*5,  # 5 MB
                #     "backupCount": 20,
                #     "encoding": "utf8"
                # },
            },

            # "loggers": {
            #     "app_name": {
            #         "level": "INFO",
            #         "handlers": ["console"],
            #         "propagate": "no"
            #     }
            # },

            "root": {
                # 'handlers': ['console'] if handlers_console else ['default'],
                'handlers': ['console'],
                'level': "INFO",
                'propagate': False
            }
        }
        # logging.config.dictConfig(LOGGING)
        # logger = logging.getLogger(__file__)
