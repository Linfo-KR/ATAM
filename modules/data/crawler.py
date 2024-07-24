import re
import json
import time
import requests

from datetime import datetime
from bs4 import BeautifulSoup

from data.dbm import DBM
from util.logger import Logger
from util.utils import *

class Crawler:
    def __init__(self, db_name, import_tbl_name, start_year, end_year):
        self.dbm = DBM(db_name=db_name)
        self.import_tbl_name = import_tbl_name
        self.logger = Logger().get_logger(module_name='modules.data.crawler')
        self.start_year = start_year
        self.end_year = end_year
        self.district_code = self.dbm.import_data(tbl_name=self.import_tbl_name, call_cols=self.tbl_config[self.import_tbl_name]['list'])
        
        with open('./config/openapi/openapi_configs.json', 'r') as config:
            self.api_config = json.load(config)
            
        with open('./configs/db/table_configs.json', 'r') as config:
            self.tbl_config = json.load(config)
            
    def set_query_list(self):
        date_list = date_generator(self.start_year, self.end_year)
                
        query_cnt = 0
        query_list = []
        try:
            for district in range(len(self.district_code)):
                for date in range(len(date_list)):
                    if date_list[date][0:4] in ('2023'):
                        service_key = self.api_config['service_key'][0]
                    elif date_list[date][0:4] in ('2022'):
                        service_key = self.api_config['service_key'][1]
                    elif date_list[date][0:4] in ('2021'):
                        service_key = self.api_config['service_key'][2]
                        
                    query_params = (
                        self.api_config['service_url'] +
                        'LAWD_CD=' + str(self.district_code[district][0]) +
                        '&DEAL_YMD=' + str(date_list[date]) +
                        '&numOfRows=' + str(100) +
                        '&serviceKey=' + service_key
                    )
                    
                    query_cnt += 1
                    query_list.append(query_params)
                
                district_length = len(self.district_code)
                district_index = district + 1
                query_length = len(self.district_code) * len(date_list)
                self.logger.info(f"[{district_index} / {district_length}] {self.district_code[district][2]} Set query lists : [{query_cnt} / {query_length} Objects]")
            
            return query_list
        
        except Exception as e:
            self.logger.error(f"Error setting query lists : {str(e)}.")
            
    def api_pipeline(self, query):
        response = requests.get(query)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        item_list = soup.find_all('item')
        result_list = soup.find_all('header')
        
        time.sleep(0.1)
        
        insert_data_cnt = 0
        insert_list = []
        
        for result in result_list:
            result_code = result.find('resultCode').string.strip()
            result_msg = result.find('resultMsg').string.strip()
            self.logger.info(f"OpenAPI response code and msg : {result_code}, {result_msg}.")
            
            for item in range(len(item_list)):
                insert = self.preprocessing(item)
                insert_list.append(insert)
                insert_data_cnt += 1
                
        return insert_list, insert_data_cnt
    
    def preprocessing(self, item):
        year = int(item.find('년').string.strip())
        month = int(item.find('월').string.strip())
        day = int(item.find('일').string.strip())
        price = int(str(item.find('거래금액').string.strip()).replace(',', ''))
        area = round(float(item.find('전용면적').string.strip()), 0)
        code = item.find('지역코드').string.strip()
        dong_name = item.find('법정동').string.strip()
        jibun = item.find('지번').string.strip()
        con_year = int(item.find('건축년도').string.strip())
        apt_name = re.sub(r'\(.*?\)', '', str(item.find('아파트').string.strip()))
        floor = abs(int(item.find('층').string.strip()))
        apt_dong = int(item.find('동').string.strip())
        
        for idx in range(len(self.district_code)):
            if code == self.district_code[idx][0]:     
                sigungu = self.district_code[idx][2]
                addr_1 = self.district_code[idx][3]
                addr_2 = self.district_code[idx][4]
                
                region_code = code
                contract_dte = datetime(year, month, day).strftime('%Y-%m-%d')
                district = sigungu
                cd_district = addr_1
                con_year = con_year
                address = addr_2 + ' ' + dong_name + ' ' + jibun + ' ' + apt_name
                apt_name = apt_name
                apt_dong = apt_dong
                floor = floor
                area = area
                price = price
                price_unit = float(price) / 10000
                py = round((float(price) / area) * 3.3, 0)
                py_unit = py / 10000
                
                schemas = [
                    region_code, contract_dte, district, cd_district, con_year, address,
                    apt_name, apt_dong, floor, area, price, price_unit, py, py_unit
                ]
                
                return schemas
                
    def insert_to_db(self, tbl):
        query_list = self.set_query_list()
        observe_data_cnt = 0
        
        for idx, query in enumerate(query_list):
            query_length = len(query_length)
            query_index = idx + 1
            
            try:
                insert_list, insert_data_cnt = self.api_pipeline(query=query)
                self.dbm.insert_data(tbl_name=tbl, data_list=insert_list)
                observe_data_cnt += insert_data_cnt
                
                self.logger.info(f"Processing : [{query_index} / {query_length}] \t Inserted : [{insert_data_cnt}] \t Observed : [{observe_data_cnt}]")
            except Exception as e:
                if isinstance(e, AttributeError):
                    self.logger.error(f"{str(e)}.")
                elif isinstance(e, ValueError):
                    self.logger.error(f"{str(e)}.")
                else:
                    self.logger.error(f"Error inserting data to database : {str(e)}.")
            
if __name__ == '__main__':
    pass