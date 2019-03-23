# -*- coding: utf-8 -*-

apiKey = ''
secret = ''

lightning_userid = ''
lightning_password = ''

# ロギング設定
def loggingConf(filename='bitbot.log'):
    return {
        'version': 1,
        'formatters':{
            'simpleFormatter':{
                'format': '%(asctime)s %(levelname)s:%(name)s:%(message)s',
                'datefmt': '%Y/%m/%d %H:%M:%S'}},
        'handlers': {
            'fileHandler': {
                'formatter':'simpleFormatter',
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'level': 'INFO',
                'filename': filename,
                'encoding': 'utf8',
                'when': 'D',
                'interval': 1,
                'backupCount': 5},
            'consoleHandler': {
                'formatter':'simpleFormatter',
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'stream': 'ext://sys.stderr'}},
        'root': {
            'level': 'INFO',
            'handlers': ['fileHandler', 'consoleHandler']},
        'disable_existing_loggers': False
    }
