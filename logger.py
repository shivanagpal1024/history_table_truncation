import logging
from logging.handlers import TimedRotatingFileHandler
import configparser as cp
import sys
import datetime 
import os

def get_logger(name):
    logger_path_1 = os.getenv('logger_path')
    #logger_path_1 = os.getcwd()
    timestamp1 = datetime.datetime.today().strftime('%H:%M:%S')
    log = logging.getLogger("ecap_db2." + name)
    logger_path = logger_path_1 + f'hist_tbl_trunc_t{timestamp1}.log'
    handler = TimedRotatingFileHandler(logger_path, when="midnight", interval=1)
    handler.suffix = "%Y%m%d"
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log
