import json
import time
import requests

from datetime import datetime
from functools import wraps
from bs4 import BeautifulSoup

from data.dbm import DBM
from util.logger import Logger

# servicekey config
# crawling 이후 전처리 코드
# region 광역시 만 추출
# 진행 경과 로깅
# day 별 트래픽(1000) 초과 시 로직 break 및 중단시점 기록 기능


class Crawler:
    def __init__(self, db_name):
        self.dbm = DBM(db_name=db_name)
        self.logger = Logger().get_logger(module_name='modules.data.crawler')
        
        with open('./config/openapi/openapi_configs.json', 'r') as config:
            self.api_config = json.load(config)