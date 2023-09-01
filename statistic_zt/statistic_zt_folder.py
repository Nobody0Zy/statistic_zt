import glob
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append('E:\\python_project\\statistic_futures_zt\\common')
# sys.path.append('E:\\python_project\\statistic_futures_zt\\raw_futures_info_data')
import config


class StatisticZt:
    def __init__(self, ):
        self.statistic_data_info_list = [
            'date', 'symbol', 'exchange', 'is_main_symbol'
                                          'zt_in', 'zt_in_count', 'zt_end', 'zt_end_count',
            'is_buy_in', 'is_buy_in_next_day',
            'zt_time', 'zt_time_rank', 'zt_time_volume', 'zt_time_amount',
            'zt_is_open', 'zt_is_open_count']

        self.price_info_list = [
            'date', 'symbol', 'exchange'
                              'pre_close', 'open', 'high', 'low', 'close', 'open/pre_close',
            'next_open', 'next_high', 'next_low', 'next_close',
            'next_next_open', 'next_next_close',
            'next_open/high', 'next_high/high', 'next_low/high', 'next_close/high',
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

    def get_zt_data(self, date, exchanges_files_list):
        date_zt_data_list = []
        for file in exchanges_files_list:
            exchange = file.split('_')[1]
            file_path = os.path.join(self.k_bars_data_folder_path, file)
            exchange_bar_data = pd.HDFStore(file_path, 'r')
            for symbol in exchange_bar_data.keys():
                if symbol in self.upper_lower_info_dict[date].keys():
                    symbol_bar_data = exchange_bar_data[symbol].loc[:, 'open':'close']
                    symbol_upper_price = max(self.upper_lower_info_dict[date][symbol])
                    symbol_bar_zt_data_df = symbol_bar_data[symbol_bar_data == symbol_upper_price].dropna(how='all')
                    if not symbol_bar_zt_data_df.empty:
                        date_zt_data_list.append([exchange, symbol])
        total_date_zt_date_list = sum(date_zt_data_list, [])
        return total_date_zt_date_list

    # @running_time
    # def get_zt_data_dict(self):
    #     zt_data_dict = {}
    #     for date in self.statistic_date_list:
    #         print(date)
    #         file_list = os.listdir(self.k_bars_data_folder_path)
    #         exchanges_file_list = [file_name for file_name in file_list if date in file_name]
    #         zt_data_dict[date] = self.get_zt_data(date, exchanges_file_list)
    #     return zt_data_dict

    @running_time
    def get_zt_data_dict(self):
        file_list = os.listdir(self.k_bars_data_folder_path)
        zt_data_list = []
        for file in file_list:
            # 打印进度条
            print(file)
            print(file_list.index(file), '/', len(file_list))
            date = file[:10]
            exchange = file.split('_')[1]
            file_path = os.path.join(self.k_bars_data_folder_path,file)
            k_bars_h5 = pd.HDFStore(file_path,'r')
            for symbol in k_bars_h5.keys():
                if symbol in self.upper_lower_info_dict[date].keys():
                    symbol_bar_data = k_bars_h5[symbol].loc[:,'open':'close']
                    symbol_upper_price = max(self.upper_lower_info_dict[date][symbol])
                    symbol_bar_zt_df = symbol_bar_data[symbol_bar_data == symbol_upper_price].dropna(how='all')
                    if not symbol_bar_zt_df.empty:
                        zt_data_list.append((date,exchange,symbol))
        return zt_data_list


if __name__ == '__main__':
    statistic_zt = StatisticZt()
    zt_data_dict = statistic_zt.get_zt_data_dict()
