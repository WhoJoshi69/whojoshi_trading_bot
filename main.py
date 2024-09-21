import yfinance as yf
import pandas as pd
import numpy as np
from pytz import timezone
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_stock_data(symbol, period="5d", interval="1h"):
    stock = yf.Ticker(symbol)
    data = stock.history(period=period, interval=interval)
    data.index = data.index.tz_convert(timezone('Asia/Kolkata'))
    return data


def calculate_atr(high, low, close, period):
    tr = np.maximum(high - low, np.abs(high - close.shift(1)), np.abs(low - close.shift(1)))
    atr = tr.rolling(window=period).mean()
    return atr


def ut_bot_alerts(data, a=3, c=6, h=False):
    src = data['Close']
    xATR = calculate_atr(data['High'], data['Low'], data['Close'], c)
    nLoss = a * xATR

    xATRTrailingStop = pd.Series(index=data.index)
    pos = pd.Series(index=data.index)

    for i in range(1, len(data)):
        if src.iloc[i] > xATRTrailingStop.iloc[i - 1] and src.iloc[i - 1] > xATRTrailingStop.iloc[i - 1]:
            xATRTrailingStop.iloc[i] = max(xATRTrailingStop.iloc[i - 1], src.iloc[i] - nLoss.iloc[i])
        elif src.iloc[i] < xATRTrailingStop.iloc[i - 1] and src.iloc[i - 1] < xATRTrailingStop.iloc[i - 1]:
            xATRTrailingStop.iloc[i] = min(xATRTrailingStop.iloc[i - 1], src.iloc[i] + nLoss.iloc[i])
        elif src.iloc[i] > xATRTrailingStop.iloc[i - 1]:
            xATRTrailingStop.iloc[i] = src.iloc[i] - nLoss.iloc[i]
        else:
            xATRTrailingStop.iloc[i] = src.iloc[i] + nLoss.iloc[i]

        if src.iloc[i - 1] < xATRTrailingStop.iloc[i - 1] and src.iloc[i] > xATRTrailingStop.iloc[i - 1]:
            pos.iloc[i] = 1
        elif src.iloc[i - 1] > xATRTrailingStop.iloc[i - 1] and src.iloc[i] < xATRTrailingStop.iloc[i - 1]:
            pos.iloc[i] = -1
        else:
            pos.iloc[i] = pos.iloc[i - 1]

    ema = src.ewm(span=1, adjust=False).mean()
    above = (ema > xATRTrailingStop) & (ema.shift(1) <= xATRTrailingStop.shift(1))
    below = (ema < xATRTrailingStop) & (ema.shift(1) >= xATRTrailingStop.shift(1))

    buy = (src > xATRTrailingStop) & above
    sell = (src < xATRTrailingStop) & below

    return buy, sell


def get_last_signal(symbol):
    data = fetch_stock_data(symbol, period="5d", interval="15m")
    buy, sell = ut_bot_alerts(data, a=3, c=6)  # Using your UT Bot settings

    # Find the last True value for buy and sell
    last_buy = buy.iloc[::-1].idxmax() if buy.any() else None
    last_sell = sell.iloc[::-1].idxmax() if sell.any() else None

    # Count False values after last True
    buy_count = (buy.iloc[::-1] == False).cumsum()[last_buy] if last_buy is not None else 0
    sell_count = (sell.iloc[::-1] == False).cumsum()[last_sell] if last_sell is not None else 0

    if last_buy is None and last_sell is None:
        return "No Signal"
    elif last_buy is None:
        return f"Sell (last signal {sell_count} candles ago)"
    elif last_sell is None:
        return f"Buy (last signal {buy_count} candles ago)"
    else:
        if last_buy > last_sell:
            return f"Buy (last signal {buy_count} candles ago)"
        else:
            return f"Sell (last signal {sell_count} candles ago)"


def process_symbol(symbol):
    try:
        last_signal = get_last_signal(symbol)
        return f"{symbol}: Last signal was {last_signal}"
    except Exception as e:
        return f"Error processing {symbol}: {str(e)}"


def main():
    symbols = ["HDFCBANK.NS", "RELIANCE.NS", "TCS.NS"]  # Add more symbols here, up to 50
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_symbol, symbol) for symbol in symbols]
        
        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    main()
