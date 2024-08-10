import re
import os
import sys
import json
import time
import requests
import datetime as dt

from typing import List, Dict, Any, Tuple
from dataclasses import dataclass
from bs4 import BeautifulSoup

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from modules.util.logger import Logger
from modules.util.utils import *
from modules.data.dbm import DBM

@dataclass
class Config:
    DB_NAME: str
    IMPORT_TBL_NAME: str
    START_YEAR: int
    END_YEAR: int
    API_CONFIG_PATH: str = './config/openapi/openapi_configs.json'
    TBL_CONFIG_PATH: str = './config/db/table_configs.json'
    PROGRESS_FILE: str = './config/openapi/api_progress.json'

    def validate(self):
        assert os.path.exists(self.API_CONFIG_PATH), f"API config file not found: {self.API_CONFIG_PATH}"
        assert os.path.exists(self.TBL_CONFIG_PATH), f"Table config file not found: {self.TBL_CONFIG_PATH}"
        assert self.START_YEAR <= self.END_YEAR, f"Invalid year range: {self.START_YEAR} to {self.END_YEAR}"

class APIManager:
    def __init__(self, service_keys: List[str]):
        self.service_keys = service_keys
        self.key_usage = {key: 0 for key in self.service_keys}
        self.current_key_idx = 0
        self.last_reset_date = dt.datetime.now().date()
        self.logger = Logger().get_logger(module_name='modules.data.crawler')

    def get_next_service_key(self) -> str:
        self.current_key_idx = (self.current_key_idx + 1) % len(self.service_keys)
        return self.service_keys[self.current_key_idx]

    def reset_key_usage(self) -> None:
        today = dt.datetime.now().date()
        if today > self.last_reset_date:
            self.key_usage = {key: 0 for key in self.service_keys}
            self.last_reset_date = today

    def wait_next_day(self) -> None:
        tomorrow = dt.datetime.now() + dt.timedelta(days=1)
        wait_time = (tomorrow.replace(hour=0, minute=0, second=0, microsecond=0) - dt.datetime.now()).total_seconds()
        self.logger.info(f"Daily limit reached for all keys. Waiting until midnight ({round(wait_time / 3600, 1)} hours).")
        time.sleep(wait_time)
        self.reset_key_usage()

class ProgressManager:
    def __init__(self, progress_file: str):
        self.progress_file = progress_file
        self.progress = self.load_progress()

    def load_progress(self) -> Dict[str, int]:
        try:
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {'last_district': 0, 'last_date': 0}

    def save_progress(self) -> None:
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f)

class Crawler:
    def __init__(self, config: Config):
        self.config = config
        self.config.validate()
        self.dbm = DBM(db_name=config.DB_NAME)
        self.logger = Logger().get_logger(module_name='modules.data.crawler')
        
        with open(config.API_CONFIG_PATH, 'r') as config_file:
            self.api_config = json.load(config_file)
            
        with open(config.TBL_CONFIG_PATH, 'r', encoding='utf8') as config_file:
            self.tbl_config = json.load(config_file)
            
        self.district_code = self.dbm.import_data(tbl_name=config.IMPORT_TBL_NAME, call_cols=self.tbl_config[config.IMPORT_TBL_NAME]['list'])
        self.api_manager = APIManager(self.api_config['service_key'])
        self.progress_manager = ProgressManager(config.PROGRESS_FILE)

    def set_query_list(self) -> List[str]:
        date_list = date_generator(self.config.START_YEAR, self.config.END_YEAR)
        
        query_cnt = 0
        query_list = []
        for district in range(self.progress_manager.progress['last_district'], len(self.district_code)):
            try:
                district_data = self.district_code.iloc[district]
                for date in range(self.progress_manager.progress['last_date'], len(date_list)):
                    service_key = self.api_manager.get_next_service_key()
                    
                    query_params = (
                        self.api_config['service_url'] +
                        'serviceKey=' + service_key +
                        '&LAWD_CD=' + str(district_data['region_code']) +
                        '&DEAL_YMD=' + str(date_list[date]) +
                        '&numOfRows=' + str(10000)
                    )
                    
                    query_cnt += 1
                    query_list.append(query_params)
                
                self.progress_manager.progress['last_district'] = district
                self.progress_manager.progress['last_date'] = 0
                self.progress_manager.save_progress()
                
                self.logger.info(f"[{district + 1} / {len(self.district_code)}] {district_data['addr_2']} Set query lists: [{query_cnt} Objects]")
            except Exception as e:
                self.logger.error(f"Error setting query lists for district {district}: {str(e)}")
        
        self.logger.info(f"Total queries generated: {len(query_list)}")
        return query_list

    def api_pipeline(self, query):
        self.api_manager.reset_key_usage()

        current_key = self.api_manager.service_keys[self.api_manager.current_key_idx]
        if self.api_manager.key_usage[current_key] >= 1000:
            if all(usage >= 1000 for usage in self.api_manager.key_usage.values()):
                self.api_manager.wait_next_day()
            else:
                current_key = self.api_manager.get_next_service_key()
        
        try:
            response = requests.get(query)
            response.raise_for_status()
            self.api_manager.key_usage[current_key] += 1
            
            self.logger.debug(f"API Response: {response.text}")
            
            soup = BeautifulSoup(response.content, 'lxml-xml')
            result_code = soup.find('resultCode').text if soup.find('resultCode') else 'Unknown'
            result_msg = soup.find('resultMsg').text if soup.find('resultMsg') else 'Unknown'
            total_count = int(soup.find('totalCount').text) if soup.find('totalCount') else 0
            
            if result_code != '000':
                self.logger.warning(f"API request failed. Result code: {result_code}, Message: {result_msg}")
                return [], 0, False

            item_list = soup.find_all('item')
            
            if not item_list or total_count != len(item_list):
                self.logger.info(f"No data returned from API. Result code: {result_code}, Message: {result_msg}, Total count: {total_count}")
                return [], 0, True

            insert_list = []
            for item in item_list:
                insert = self.preprocessing(item)
                if insert:
                    insert_list.append(insert)
            
            return insert_list, len(insert_list), True
        except Exception as e:
            self.logger.error(f"Error in API pipeline: {str(e)}")
            return [], 0, False

    def preprocessing(self, item: BeautifulSoup) -> List[Any]:
        def safe_find(item, tag):
            found = item.find(tag)
            return found.string.strip() if found and found.string else None

        try:
            year = safe_find(item, 'dealYear')
            month = safe_find(item, 'dealMonth')
            day = safe_find(item, 'dealDay')
            price = safe_find(item, 'dealAmount')
            area = safe_find(item, 'excluUseAr')
            code = safe_find(item, 'sggCd')
            dong_name = safe_find(item, 'umdNm')
            jibun = safe_find(item, 'jibun')
            con_year = safe_find(item, 'buildYear')
            apt_name = safe_find(item, 'aptNm')
            floor = safe_find(item, 'floor')
            apt_dong = safe_find(item, 'aptDong')

            if not all([year, month, day, price, area, code, dong_name, jibun, con_year, apt_name, floor]):
                self.logger.warning(f"Missing essential data: {item}")
                return None

            year = int(year)
            month = int(month)
            day = int(day)
            price = int(price.replace(',', ''))
            area = round(float(area), 0)
            con_year = int(con_year)
            floor = abs(int(floor))
            apt_name = re.sub(r'\(.*?\)', '', apt_name)

            district_info = self.district_code[self.district_code['region_code'] == code]
            if district_info.empty:
                self.logger.warning(f"No matching district info for code: {code}")
                return None

            district_info = district_info.iloc[0]
            sigungu = district_info['addr_1']
            addr_1 = district_info['addr_1']
            addr_2 = district_info['addr_2']

            region_code = code
            contract_dte = dt.datetime(year, month, day).strftime('%Y-%m-%d')
            district = sigungu
            cd_district = addr_1
            address = f"{addr_2} {dong_name} {jibun} {apt_name}"
            price_unit = float(price) / 10000
            py = round((float(price) / area) * 3.3, 0)
            py_unit = py / 10000

            schemas = [
                region_code, contract_dte, district, cd_district, con_year, address,
                apt_name, apt_dong, floor, area, price, price_unit, py, py_unit
            ]

            return schemas
        except ValueError as e:
            self.logger.error(f"ValueError in preprocessing: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in preprocessing: {str(e)}")
            return None

    def insert_to_db(self, tbl):
        query_list = self.set_query_list()
        observe_data_cnt = 0
        
        for idx, query in enumerate(query_list):
            query_length = len(query_list)
            query_index = idx + 1
            
            try:
                insert_list, insert_data_cnt, is_success = self.api_pipeline(query=query)
                if is_success:
                    if insert_list:
                        self.dbm.insert_data(tbl_name=tbl, data_list=insert_list)
                        observe_data_cnt += insert_data_cnt
                        self.logger.info(f"Processing : [{query_index} / {query_length}] \t Inserted : [{insert_data_cnt}] \t Observed : [{observe_data_cnt}]")
                    else:
                        self.logger.info(f"Processing : [{query_index} / {query_length}] \t No data to insert")
                    
                    self.progress_manager.progress['last_date'] += 1
                    self.progress_manager.save_progress()
                else:
                    self.logger.warning(f"Skipping query {query_index} due to API request failure")
                
            except Exception as e:
                if isinstance(e, AttributeError):
                    self.logger.error(f"AttributeError : {str(e)}.")
                elif isinstance(e, ValueError):
                    self.logger.error(f"ValueError : {str(e)}.")
                else:
                    self.logger.error(f"Error inserting data to database : {str(e)}.")
            
            finally:
                time.sleep(0.1)
        
        self.logger.info(f"Total queries processed: {query_length}, Total data inserted: {observe_data_cnt}")

if __name__ == '__main__':
    config = Config(DB_NAME='atamDB', IMPORT_TBL_NAME='district_code', START_YEAR=2020, END_YEAR=2023)
    pipeline = Crawler(config)
    pipeline.insert_to_db('trade')