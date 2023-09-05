import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import List

import numpy as np
import pandas as pd
from tqdm import tqdm

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

from compose_h5_df import compose_h5_df


class ComposeDateBar:
    def __init__(self, compose_method, input_min_bar_folder_path, output_date_bar_folder_path):
        self.compose_method = compose_method
        self.input_min_bar_folder_path = input_min_bar_folder_path
        self.output_date_bar_folder_path = output_date_bar_folder_path

    def _process_file(self, file_path):
        date = os.path.basename(file_path).split('_')[0]
        exchange = os.path.basename(file_path).split('_')[1]
        k_bars_h5 = pd.HDFStore(file_path, 'r')
        k_bars_df_day = self.compose_method(k_bars_h5)
        k_bars_h5.close()
        k_bars_df_day.reset_index(inplace=True)
        k_bars_df_day.drop(['index'], axis=1, inplace=True)
        k_bars_df_day.insert(0, 'exchange', exchange)
        k_bars_df_day.insert(0, 'date', date)
        return k_bars_df_day

    def _get_file_paths(self) -> List[str]:
        file_list = os.listdir(self.input_min_bar_folder_path)
        file_path_list = [os.path.join(self.input_min_bar_folder_path, file) for file in file_list]
        return file_path_list

    def multiprocessing_process_file(self, num_process):
        need_compose_file_path_list = self._get_file_paths()

        with ProcessPoolExecutor(max_workers=num_process) as executor:
            date_bar_df_list = list(tqdm(
                executor.map(self._process_file, need_compose_file_path_list),
                total=len(need_compose_file_path_list),
                desc='Processing Files',
            ))

        # with Pool(num_process) as pool:
        #     date_bar_df_list = list(tqdm(pool.imap(self._process_file, need_compose_file_path_list),
        #                                  total=len(need_compose_file_path_list),
        #                                  desc="Processing Files"))
        # date_bar_df_list = pool.map(self._process_file,need_compose_file_path_list)

        date_bar_df_total = pd.concat(date_bar_df_list)
        symbol_list = date_bar_df_total.symbol.values.tolist()
        symbol_month_list = [''.join(re.findall(r'[A-Za-z]', symbol)) + symbol[-2:] for symbol in symbol_list]
        date_bar_df_total.insert(3, 'symbol_month', np.nan)
        date_bar_df_total['symbol_month'] = symbol_month_list

        date_bar_df_total.to_pickle(self.output_date_bar_folder_path)


@running_time
def main():
    compose_method = compose_h5_df
    input_min_bar_folder_path = "E:\\python_project\\statistic_futures_zt\\raw_futures_info_data\\k_bars_data"
    output_date_bar_folder_path = "E:\\python_project\\statistic_futures_zt\\compose_date_bar\\date_bar_by_compose.pkl"
    compose_date_bar = ComposeDateBar(compose_method, input_min_bar_folder_path, output_date_bar_folder_path)
    process_num = 10
    compose_date_bar.multiprocessing_process_file(process_num)


if __name__ == '__main__':
    main()
