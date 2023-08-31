from constants import *

CONFIG = {
    'upper_lower_info_dict': upper_lower_info_dict,
    'main_symbol_day_dict': main_symbol_day_dict,
    'k_bars_data_folder_path': k_bars_data_folder_path,
    }

def get_config(key):
    return CONFIG[key]

