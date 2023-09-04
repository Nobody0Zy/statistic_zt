import pandas as pd


def _compose_symbol_list(symbol_df):
    price_data_list = [
        symbol_df.open.iloc[0], symbol_df.high.max(), symbol_df.low.min(), symbol_df.close.iloc[-1],
        symbol_df.volume.sum(), symbol_df.amount.sum(), symbol_df.position.iloc[-1]]
    return price_data_list

def compose_h5_df(k_bars_h5):
    symbol_list = k_bars_h5.keys()
    symbol_df_list = []
    for symbol in symbol_list:
        if symbol != '/correct_info':
            symbol_df = k_bars_h5[symbol]
            symbol_price_data_list = _compose_symbol_list(symbol_df)
            symbol_price_data_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'position']
            symbol_date_df = pd.DataFrame([symbol_price_data_list], columns=symbol_price_data_columns)
            symbol_date_df.insert(0, 'symbol', symbol)
            symbol_df_list.append(symbol_date_df)
    symbol_bar_data = pd.concat(symbol_df_list, axis=0)
    return symbol_bar_data
