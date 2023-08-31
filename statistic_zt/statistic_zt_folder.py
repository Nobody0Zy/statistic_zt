import pandas as pd
import numpy as np

import os
import sys
import glob 
sys.path.append('E:\\python_project\\statistic_futures_zt\\common')
# sys.path.append('E:\\python_project\\statistic_futures_zt\\raw_futures_info_data')
import config


class StatisticZt:
    def __init__(self,):
        self.statistic_data_info_list = [
            'date','symbol','exchange','is_main_symbol'
            'zt_in','zt_in_count','zt_end','zt_end_count',
            'is_buy_in','is_buy_in_next_day',
            'zt_time','zt_time_rank','zt_time_volume','zt_time_amount',
            'zt_is_open','zt_is_open_count']
        
        self.price_info_list = [
            'date','symbol','exchange'
            'pre_close','open','high','low','close','open/pre_close',
            'next_open','next_high','next_low','next_close',
            'next_next_open','next_next_close',
            'next_open/high','next_high/high','next_low/high','next_close/high',
            ]
        
        self.upper_lower_info_dict = config.get_config('upper_lower_info_dict')
        self.main_symbol_day_dict = config.get_config('main_symbol_day_dict')
        self.k_bars_data_folder_path = config.get_config('k_bars_data_folder_path')
        self.statistic_date_list = self.__get_statistic_date_list()
        
    def __get_statistic_date_list(self):
        file_date_list = os.listdir(self.k_bars_data_folder_path)
        statistic_date_list = list(set([file_name[:10] for file_name in file_date_list]))
        statistic_date_list.sort()
        return statistic_date_list
    
        
    def _descri_zt_data(self):
        
    
    def get_zt_data(self,date,exchanges_files_ist):
        upper_lower_info_dict_at_date = self.upper_lower_info_dict[date]
        file_list = os.listdir(self.k_bars_data_folder_path)
        exchanges_file_list = [file_name for file_name in file_list if date in file_name]
        for file in exchanges_file_list:
            exchange = file.split('_')[1]
            zt_symbols = []
            
    def get_zt_data_dict(self):
        zt_data_dict = {}
        for date in self.statistic_date_list:
            