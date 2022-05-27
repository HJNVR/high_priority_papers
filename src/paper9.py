# Feature generation and pre-processing of paper 9
# ============================================================================

import numpy as np
import pandas as pd
import yfinance as yf

from fredapi import Fred
import investpy


def download_stock(ticker):
    """Download stock according to different tickers.

    @ticker: String
            Ticker of stock

    @return: DataFrame
            Stock data
    """
    df_ticker = yf.download(ticker, start=start_date, end=end_date)
    return df_ticker


def calculate_ema(prices, days, smoothing=2):
    """Calculate exponential moving average.

    @prices: Float
            Close stock prices at different dates

    @days: Int 
            Number of days

    @smoothings: Int
            Optional
    """
    ema = []
    for num_day in range(days-1):
        ema.append(0)
    ema.append(sum(prices[:days]) / days)
    for price in prices[days:]:
        ema.append((price * (smoothing / (1 + days))) +
                   ema[-1] * (1 - (smoothing / (1 + days))))
    return ema


def cal_export(df):
     """ Call all functions below and combine into a single dataframe

    @df: DataFrame
        
    """
    df = Close(df)
    df = SPYt(df)
    df = SPYt1(df)
    df = SPYt2(df)
    df = SPYt3(df)
    df = RDP5(df)
    df = RDP10(df)
    df = RDP15(df)
    df = RDP20(df)
    df = EMA10(df)
    df = EMA20(df)
    df = EMA50(df)
    df = EMA200(df)
    return df


# Below functions are to calculate the respective features
def Close(df):
    return df.drop(['Open', 'High', 'Low', 'Adj Close', 'Volume'], axis=1)

# 3 SPYt


def SPYt(df):
    df['SPYt'] = df['Close'].pct_change()
    return df

# 4 SPYt1


def SPYt1(df):
    df['Close_l1'] = df['Close'].shift(1)
    df['SPYt1'] = df['Close_l1'].pct_change()
    return df.drop(['Close_l1'], axis=1)

# 5 SPYt2


def SPYt2(df):
    df['Close_l2'] = df['Close'].shift(2)
    df['SPYt2'] = df['Close_l2'].pct_change()
    return df.drop(['Close_l2'], axis=1)

# 6 SPYt3


def SPYt3(df):
    df['Close_l3'] = df['Close'].shift(3)
    df['SPYt3'] = df['Close_l3'].pct_change()
    return df.drop(['Close_l3'], axis=1)

# 7 RDP5


def RDP5(df):
    df['Close_l5'] = df['Close'].shift(5)
    df['RDP5'] = (df['Close']-df['Close_l5'])/df['Close_l5']*100
    return df.drop(['Close_l5'], axis=1)

# 8 RDP10


def RDP10(df):
    df['Close_l10'] = df['Close'].shift(10)
    df['RDP10'] = (df['Close']-df['Close_l10'])/df['Close_l10']*100
    return df.drop(['Close_l10'], axis=1)

# 9 RDP15


def RDP15(df):
    df['Close_l15'] = df['Close'].shift(15)
    df['RDP15'] = (df['Close']-df['Close_l15'])/df['Close_l15']*100
    return df.drop(['Close_l15'], axis=1)

# 10 RDP20


def RDP20(df):
    df['Close_l20'] = df['Close'].shift(20)
    df['RDP20'] = (df['Close']-df['Close_l20'])/df['Close_l20']*100
    return df.drop(['Close_l20'], axis=1)

# 11 EMA10


def EMA10(df):
    df['EMA10'] = calculate_ema(df['Close'], 10)
    return df

# 12 EMA20


def EMA20(df):
    df['EMA20'] = calculate_ema(df['Close'], 20)
    return df

# 13 EMA50


def EMA50(df):
    df['EMA50'] = calculate_ema(df['Close'], 50)
    return df

# 14 EMA200


def EMA200(df):
    df['EMA200'] = calculate_ema(df['Close'], 200)
    return df


if __name__ == "__main__":
    start_date = '2000-01-01'
    end_date = '2020-12-31'

    df = yf.download("SPY", start=start_date, end=end_date)

    # Calculate features

    # 1 Date_SPY

    # 2 Close_SPY

    # 3 SPYt
    df['SPYt'] = df['Adj Close'].pct_change()
    # df_SPY['SPYt']=df_SPY['Adj Close'].resample('M').ffill().pct_change()

    # 4 SPYt1
    df['Adj Close_l1'] = df['Adj Close'].shift(1)
    df['SPYt1'] = df['Adj Close_l1'].pct_change()

    # 5 SPYt2
    df['Adj Close_l2'] = df['Adj Close'].shift(2)
    df['SPYt2'] = df['Adj Close_l2'].pct_change()

    # 6 SPYt3
    df['Adj Close_l3'] = df['Adj Close'].shift(3)
    df['SPYt3'] = df['Adj Close_l3'].pct_change()

    # 7 RDP5
    df['Adj Close_l5'] = df['Adj Close'].shift(5)
    df['RDP5'] = (df['Adj Close']-df['Adj Close_l5'])/df['Adj Close_l5']*100

    # 8 RDP10
    df['Adj Close_l10'] = df['Adj Close'].shift(10)
    df['RDP10'] = (df['Adj Close']-df['Adj Close_l10'])/df['Adj Close_l10']*100

    # 9 RDP15
    df['Adj Close_l15'] = df['Adj Close'].shift(15)
    df['RDP15'] = (df['Adj Close']-df['Adj Close_l15'])/df['Adj Close_l15']*100

    # 10 RDP20
    df['Adj Close_l20'] = df['Adj Close'].shift(20)
    df['RDP20'] = (df['Adj Close']-df['Adj Close_l20'])/df['Adj Close_l20']*100

    # 11 EMA10
    df['EMA10'] = calculate_ema(df['Adj Close'], 10)

    # 12 EMA20
    df['EMA20'] = calculate_ema(df['Adj Close'], 20)

    # 13 EMA50
    df['EMA50'] = calculate_ema(df['Adj Close'], 50)

    # 14 EMA200
    df['EMA200'] = calculate_ema(df['Adj Close'], 200)

    # 15 HSI
    df_HSI = yf.download("HSI", start=start_date, end=end_date)
    df['HSI'] = df_HSI['Adj Close'].pct_change()

    # 16 SSE*
    df_SSE = yf.download("000001.SS", start=start_date, end=end_date)
    df['SSE'] = df_SSE['Adj Close'].pct_change()

    # 17 FCHI*
    df_FCHI = yf.download("^FCHI", start=start_date, end=end_date)
    df['FCHI'] = df_FCHI['Adj Close'].pct_change()

    # 18 FTSE*
    df_FTSE = yf.download("^FTSE", start=start_date, end=end_date)
    df['FTSE'] = df_FTSE['Adj Close'].pct_change()

    # 19 GDAXI*
    df_GDAXI = yf.download("^GDAXI", start=start_date, end=end_date)
    df['GDAXI'] = df_GDAXI['Adj Close'].pct_change()

    # 20 DJI
    df_DJI = yf.download("DJI", start=start_date, end=end_date)
    df['DJI'] = df_DJI['Adj Close'].pct_change()

    # 21 IXIC*
    df_IXIC = yf.download("^IXIC", start=start_date, end=end_date)
    df['IXIC'] = df_IXIC['Adj Close'].pct_change()

    # 22 V
    df['V'] = df['Volume'].pct_change()

    # 23 AAPL
    df_AAPL = yf.download("AAPL", start=start_date, end=end_date)
    df['AAPL'] = df_AAPL['Adj Close'].pct_change()

    # 24 MSFT
    df_MSFT = yf.download("MSFT", start=start_date, end=end_date)
    df['MSFT'] = df_MSFT['Adj Close'].pct_change()

    # 25 XOM
    df_XOM = yf.download("XOM", start=start_date, end=end_date)
    df['XOM'] = df_XOM['Adj Close'].pct_change()

    # 26 GE
    df_GE = yf.download("GE", start=start_date, end=end_date)
    df['GE'] = df_GE['Adj Close'].pct_change()

    # 27 JNJ
    df_JNJ = yf.download("JNJ", start=start_date, end=end_date)
    df['JNJ'] = df_JNJ['Adj Close'].pct_change()

    # 28 WFC
    df_WFC = yf.download("WFC", start=start_date, end=end_date)
    df['WFC'] = df_WFC['Adj Close'].pct_change()

    # 29 AMZN
    df_AMZN = yf.download("AMZN", start=start_date, end=end_date)
    df['AMZN'] = df_AMZN['Adj Close'].pct_change()

    # 30 JPM
    df_JPM = yf.download("JPM", start=start_date, end=end_date)
    df['JPM'] = df_JPM['Adj Close'].pct_change()

    # import list of S&P500 company tickers
    stocks = pd.read_csv('S&P500 companies list (2000 to 2020).csv')
    tickers = stocks['ticker'].values

    dic = {}
    for t in tickers:
        dic[t] = download_stock(t)

    # calculate features and export as csv file
    for k, v in dic.items():
        if v.shape[0] == 5284:
            cal_export(v).to_csv('../result/paper9/'+k+'.csv')

    # Fred API key: ab766afb0df13dba8492403a7865f852
    fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')
    T1 = fred.get_series(
        'DTB4WK', observation_start=start_date, observation_end=end_date)
    T1.index.name = 'Date'
    T1.rename('T1', inplace=True)

    T3 = fred.get_series(
        'DTB3', observation_start=start_date, observation_end=end_date)
    T3.index.name = 'Date'
    T3.rename('T3', inplace=True)

    T6 = fred.get_series(
        'DTB6', observation_start=start_date, observation_end=end_date)
    T6.index.name = 'Date'
    T6.rename('T6', inplace=True)

    T60 = fred.get_series(
        'DGS5', observation_start=start_date, observation_end=end_date)
    T60.index.name = 'Date'
    T60.rename('T60', inplace=True)

    T120 = fred.get_series(
        'DGS10', observation_start=start_date, observation_end=end_date)
    T120.index.name = 'Date'
    T120.rename('T120', inplace=True)

    CD1 = fred.get_series(
        'DCD1M', observation_start=start_date, observation_end=end_date)
    CD1.index.name = 'Date'
    CD1.rename('CD1', inplace=True)

    CD3 = fred.get_series(
        'DCD90', observation_start=start_date, observation_end=end_date)
    CD3.index.name = 'Date'
    CD3.rename('CD3', inplace=True)

    CD6 = fred.get_series(
        'DCD6M', observation_start=start_date, observation_end=end_date)
    CD6.index.name = 'Date'
    CD6.rename('CD6', inplace=True)

    data = pd.read_csv("FRB_H15.csv")
    data = data.iloc[5:, :]
    data['Series Description'] = pd.to_datetime(data['Series Description'])

    CTB3M = pd.Series(
        data['Market yield on U.S. Treasury securities at 3-month   constant maturity, quoted on investment basis'])
    CTB3M.reset_index(drop=True, inplace=True)
    CTB3M.index = data['Series Description'].values
    CTB3M = CTB3M.dropna()
    CTB3M = CTB3M[CTB3M.values != 'ND'].astype(float)
    CTB3M = CTB3M - CTB3M.shift()
    CTB3M = CTB3M[(CTB3M.index >= start_date) & (CTB3M.index <= end_date)]
    CTB3M.index.name = 'Date'
    CTB3M.rename('CTB3M', inplace=True)

    CTB6M = pd.Series(
        data['Market yield on U.S. Treasury securities at 6-month   constant maturity, quoted on investment basis'])
    CTB6M.reset_index(drop=True, inplace=True)
    CTB6M.index = data['Series Description'].values
    CTB6M = CTB6M.dropna()
    CTB6M = CTB6M[CTB6M.values != 'ND'].astype(float)
    CTB6M = CTB6M - CTB6M.shift()
    CTB6M = CTB6M[(CTB6M.index >= start_date) & (CTB6M.index <= end_date)]
    CTB6M.index.name = 'Date'
    CTB6M.rename('CTB6M', inplace=True)

    CTB1Y = pd.Series(
        data['Market yield on U.S. Treasury securities at 1-year   constant maturity, quoted on investment basis'])
    CTB1Y.reset_index(drop=True, inplace=True)
    CTB1Y.index = data['Series Description'].values
    CTB1Y = CTB1Y.dropna()
    CTB1Y = CTB1Y[CTB1Y.values != 'ND'].astype(float)
    CTB1Y = CTB1Y - CTB1Y.shift()
    CTB1Y = CTB1Y[(CTB1Y.index >= start_date) & (CTB1Y.index <= end_date)]
    CTB1Y.index.name = 'Date'
    CTB1Y.rename('CTB1Y', inplace=True)

    CTB5Y = pd.Series(
        data['Market yield on U.S. Treasury securities at 5-year   constant maturity, quoted on investment basis'])
    CTB5Y.reset_index(drop=True, inplace=True)
    CTB5Y.index = data['Series Description'].values
    CTB5Y = CTB5Y.dropna()
    CTB5Y = CTB5Y[CTB5Y.values != 'ND'].astype(float)
    CTB5Y = CTB5Y - CTB5Y.shift()
    CTB5Y = CTB5Y[(CTB5Y.index >= start_date) & (CTB5Y.index <= end_date)]
    CTB5Y.index.name = 'Date'
    CTB5Y.rename('CTB5Y', inplace=True)

    CTB10Y = pd.Series(
        data['Market yield on U.S. Treasury securities at 10-year   constant maturity, quoted on investment basis'])
    CTB10Y.reset_index(drop=True, inplace=True)
    CTB10Y.index = data['Series Description'].values
    CTB10Y = CTB10Y.dropna()
    CTB10Y = CTB10Y[CTB10Y.values != 'ND'].astype(float)
    CTB10Y = CTB10Y - CTB10Y.shift()
    CTB10Y = CTB10Y[(CTB10Y.index >= start_date) & (CTB10Y.index <= end_date)]
    CTB10Y.index.name = 'Date'
    CTB10Y.rename('CTB10Y', inplace=True)

    AAA = fred.get_series(
        'DAAA', observation_start=start_date, observation_end=end_date)
    AAA.index.name = 'Date'
    AAA.rename('AAA', inplace=True)

    BAA = fred.get_series(
        'DBAA', observation_start=start_date, observation_end=end_date)
    BAA.index.name = 'Date'
    BAA.rename('BAA', inplace=True)

    TE1 = T120 - T1
    TE1.index.name = 'Date'
    TE1.rename('TE1', inplace=True)

    TE2 = T120 - T3
    TE2.index.name = 'Date'
    TE2.rename('TE2', inplace=True)

    TE3 = T120 - T6
    TE3.index.name = 'Date'
    TE3.rename('TE3', inplace=True)

    TE5 = T3 - T1
    TE5.index.name = 'Date'
    TE5.rename('TE5', inplace=True)

    TE6 = T6 - T1
    TE6.index.name = 'Date'
    TE6.rename('TE6', inplace=True)

    DE1 = BAA - AAA
    DE1.index.name = 'Date'
    DE1.rename('DE1', inplace=True)

    DE2 = BAA - T120
    DE2.index.name = 'Date'
    DE2.rename('DE2', inplace=True)

    DE4 = BAA - T6
    DE4.index.name = 'Date'
    DE4.rename('DE4', inplace=True)

    DE5 = BAA - T3
    DE5.index.name = 'Date'
    DE5.rename('DE5', inplace=True)

    DE6 = BAA - T1
    DE6.index.name = 'Date'
    DE6.rename('DE6', inplace=True)

    DE7 = CD6 - T6
    DE7.index.name = 'Date'
    DE7.rename('DE7', inplace=True)

    data = pd.read_excel(
        "https://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls", sheet_name='Data 1', skiprows=2)
    data = data.set_index('Date')

    Oil = (data - data.shift()) / data.shift()  # need SPY dates as control
    Oil = Oil[(Oil.index >= start_date) & (Oil.index <= end_date)]
    Oil.rename(columns={
               'Cushing, OK WTI Spot Price FOB (Dollars per Barrel)': 'Oil'}, inplace=True)

    # investpy
    start_date = "31/12/1999"
    end_date = "31/12/2020"

    USD_Y = investpy.get_currency_cross_historical_data(
        currency_cross='USD/JPY', from_date=start_date, to_date=end_date)
    USD_Y = USD_Y['Close']
    USD_Y.rename('USD_Y', inplace=True)

    USD_GBP = investpy.get_currency_cross_historical_data(
        currency_cross='GBP/USD', from_date=start_date, to_date=end_date)
    USD_GBP = USD_GBP['Close']
    USD_GBP.rename('USD_GBP', inplace=True)

    USD_CAD = investpy.get_currency_cross_historical_data(
        currency_cross='USD/CAD', from_date=start_date, to_date=end_date)
    USD_CAD = USD_CAD['Close']
    USD_CAD.rename('USD_CAD', inplace=True)

    USD_CNY = investpy.get_currency_cross_historical_data(
        currency_cross='USD/CNY', from_date=start_date, to_date=end_date)
    USD_CNY = USD_CNY['Close']
    USD_CNY.rename('USD_CNY', inplace=True)

    Gold = investpy.get_currency_cross_historical_data(
        currency_cross='XAU/USD', from_date=start_date, to_date=end_date)
    Gold['Gold'] = (Gold['Close'] - Gold['Close'].shift()) / \
        Gold['Close'].shift()
    Gold = Gold['Gold']

    df = df[['Close', 'SPYt', 'SPYt1', 'SPYt2', 'SPYt3', 'RDP5', 'RDP10', 'RDP15', 'RDP20', 'EMA10', 'EMA20', 'EMA50',
             'EMA200', 'HSI', 'SSE', 'FCHI', 'FTSE', 'GDAXI', 'DJI', 'IXIC', 'V', 'AAPL', 'MSFT', 'XOM', 'GE', 'JNJ', 'WFC', 'AMZN', 'JPM']]

    df = df.join(T1)
    df = df.join(T3)
    df = df.join(T6)
    df = df.join(T60)
    df = df.join(T120)
    df = df.join(CD1)
    df = df.join(CD3)
    df = df.join(CD6)
    df = df.join(Oil)
    df = df.join(Gold)
    df = df.join(CTB3M)
    df = df.join(CTB6M)
    df = df.join(CTB1Y)
    df = df.join(CTB5Y)
    df = df.join(CTB10Y)
    df = df.join(AAA)
    df = df.join(BAA)
    df = df.join(TE1)
    df = df.join(TE2)
    df = df.join(TE3)
    df = df.join(TE5)
    df = df.join(TE6)
    df = df.join(DE1)
    df = df.join(DE2)
    df = df.join(DE4)
    df = df.join(DE5)
    df = df.join(DE6)
    df = df.join(DE7)
    df = df.join(USD_Y)
    df = df.join(USD_GBP)
    df = df.join(USD_CAD)
    df = df.join(USD_CNY)
    df = df.iloc[1:, :]

    df.to_csv('../result/paper9/common_features.csv')

    # pre-processing
    #df.fillna(0)
    #convert to monthly
    #convert to yearly
    #convert to quarterly





