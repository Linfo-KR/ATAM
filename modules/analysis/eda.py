import os
import sys
import json

import pyacet as acet

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from modules.util.logger import Logger
from modules.util.utils import *
from modules.data.dbm import DBM

class EDA:
    def __init__(self, db_name, import_tbl_name):
        self.dbm = DBM(db_name=db_name)
        self.import_tbl_name = import_tbl_name
        self.logger = Logger().get_logger(module_name='modules.analysis.eda')
        self.report_dir = './docs/eda_report'
        self.plot_dir = './images/eda_plot'
        
        with open('./config/db/table_configs.json', 'r', encoding='utf-8') as config:
            self.tbl_config = json.load(config)
            
        self.data = self.dbm.import_data(self.import_tbl_name, self.tbl_config[import_tbl_name]['list'], dates=self.tbl_config[import_tbl_name]['dates'])
        
    def basic_eda(self):
        try:
            cols = self.tbl_config[self.import_tbl_name]['list']
            exclude_cols = self.tbl_config[self.import_tbl_name]['exclude']
            acet.ReportGenerator(input=self.data, cols=cols, output_dir=self.report_dir, dataset_name='Apartment Trade').generate_report(exclude_cols)
            acet.Visualization(input=self.data, cols=cols, output_dir=self.plot_dir).visualize(exclude_cols)
        except Exception as e:
            self.logger.error(f"Error basic eda : {str(e)}.")
        
if __name__ == '__main__':
    eda = EDA('atamDB', 'trade')
    eda.basic_eda()