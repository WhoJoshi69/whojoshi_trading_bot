import yfinance as yf
import pandas as pd
import numpy as np
from pytz import timezone
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


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
        signal_type, candles_ago = parse_signal(last_signal)
        return {
            "symbol": symbol,
            "signal_type": signal_type,
            "candles_ago": candles_ago,
            "full_message": f"{symbol}: Last signal was {last_signal}"
        }
    except Exception as e:
        return {
            "symbol": symbol,
            "signal_type": "Error",
            "candles_ago": float('inf'),
            "full_message": f"Error processing {symbol}: {str(e)}"
        }


def parse_signal(signal):
    if signal == "No Signal":
        return "No Signal", float('inf')
    parts = signal.split()
    signal_type = parts[0]
    candles_ago = int(parts[-3])
    return signal_type, candles_ago


def main():
    symbols = [
        "ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS",
        "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BPCL.NS", "BHARTIARTL.NS",
        "BRITANNIA.NS", "CIPLA.NS", "COALINDIA.NS", "DIVISLAB.NS", "DRREDDY.NS",
        "EICHERMOT.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS",
        "HEROMOTOCO.NS", "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ITC.NS",
        "INDUSINDBK.NS", "INFY.NS", "JSWSTEEL.NS", "KOTAKBANK.NS", "LT.NS",
        "M&M.NS", "MARUTI.NS", "NTPC.NS", "NESTLEIND.NS", "ONGC.NS",
        "POWERGRID.NS", "RELIANCE.NS", "SBILIFE.NS", "SBIN.NS", "SUNPHARMA.NS",
        "TCS.NS", "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "TECHM.NS",
        "TITAN.NS", "UPL.NS", "ULTRACEMCO.NS", "WIPRO.NS", "ZOMATO.NS"
    ]

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_symbol, symbol) for symbol in symbols]

        for future in as_completed(futures):
            results.append(future.result())

    # Sort results based on candles_ago
    sorted_results = sorted(results, key=lambda x: x['candles_ago'])

    # Convert to JSON and save to file
    with open('stock_signals.json', 'w') as f:
        json.dump(sorted_results, f, indent=2)

    print("Results have been saved to stock_signals.json")


if __name__ == "__main__":
    main()
