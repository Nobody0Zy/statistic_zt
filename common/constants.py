import os

import pandas as pd

raw_data_info_folder_path = "E:\\python_project\\statistic_futures_zt\\raw_futures_info_data"

upper_lower_info_dict = pd.read_pickle(os.path.join(raw_data_info_folder_path,"upper_lower_info_dict.pkl"))

main_symbol_day_dict = pd.read_pickle(os.path.join(raw_data_info_folder_path,"main_symbol_day_dict.pkl"))

k_bars_data_folder_path = os.path.join(raw_data_info_folder_path,'k_bars_data')


