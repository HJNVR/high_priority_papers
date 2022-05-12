import unittest
import numpy as np 
import pandas as pd
from numpy.testing import assert_array_equal
import investpy
import yfinance as yf
from fredapi import Fred

def generate_feature(f1, f2):
    '''
    compare and choose latest start date and earliest end date
    '''
    start_date = max(f1.index[0], f2.index[0])
    end_date = min(f1.index[-1], f2.index[-1])
    feature = f1[(f1.index >= start_date) & (f1.index <= end_date)] \
        - f2[(f2.index >= start_date) & (f2.index <= end_date)]
    return feature

def calculate_ema(prices, days, smoothing=2):
    ema = []
    for num_day in range(days-1):
        ema.append('N/A')        
    ema.append(sum(prices[:days]) / days)
    for price in prices[days:]:
        ema.append((price * (smoothing / (1 + days))) + ema[-1] * (1 - (smoothing / (1 + days))))
    return ema

class Testing(unittest.TestCase):

    start_date = '2000-01-01'
    end_date = '2020-12-31'
    #investpy has differnt dat format
    ivp_start_date = "01/01/2000"
    ivp_end_date = "31/12/2020"
    
    # Fred API key: ab766afb0df13dba8492403a7865f852
    fred = Fred(api_key = 'ab766afb0df13dba8492403a7865f852')

    T1 = fred.get_series('DTB4WK', observation_start=start_date, observation_end=end_date)
    T3 = fred.get_series('DTB3', observation_start=start_date, observation_end=end_date)
    T6 = fred.get_series('DTB6', observation_start=start_date, observation_end=end_date)
    T60 = fred.get_series('DGS5', observation_start=start_date, observation_end=end_date)
    T120 = fred.get_series('DGS10', observation_start=start_date, observation_end=end_date)
    CD1 = fred.get_series('DCD1M', observation_start=start_date, observation_end=end_date)
    CD3 = fred.get_series('DCD90', observation_start=start_date, observation_end=end_date)
    CD6 = fred.get_series('DCD6M', observation_start=start_date, observation_end=end_date)

    fed_data = pd.read_csv("https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package")
    fed_data = fed_data.iloc[5:,:]
    fed_data['Series Description'] = pd.to_datetime(fed_data['Series Description'])

    CTB3M = pd.Series(fed_data['Market yield on U.S. Treasury securities at 3-month   constant maturity, quoted on investment basis'])
    CTB3M.reset_index(drop=True, inplace=True)
    CTB3M.index = fed_data['Series Description'].values
    CTB3M = CTB3M.dropna()
    CTB3M = CTB3M[CTB3M.values != 'ND'].astype(float)
    CTB3M = CTB3M[(CTB3M.index >= start_date) & (CTB3M.index <= end_date)]
    CTB3M = CTB3M - CTB3M.shift()

    CTB6M = pd.Series(fed_data['Market yield on U.S. Treasury securities at 6-month   constant maturity, quoted on investment basis'])
    CTB6M.reset_index(drop=True, inplace=True)
    CTB6M.index = fed_data['Series Description'].values
    CTB6M = CTB6M.dropna()
    CTB6M = CTB6M[CTB6M.values != 'ND'].astype(float)
    CTB6M = CTB6M[(CTB6M.index >= start_date) & (CTB6M.index <= end_date)]
    CTB6M = CTB6M - CTB6M.shift()

    CTB1Y = pd.Series(fed_data['Market yield on U.S. Treasury securities at 1-year   constant maturity, quoted on investment basis'])
    CTB1Y.reset_index(drop=True, inplace=True)
    CTB1Y.index = fed_data['Series Description'].values
    CTB1Y = CTB1Y.dropna()
    CTB1Y = CTB1Y[CTB1Y.values != 'ND'].astype(float)
    CTB1Y = CTB1Y[(CTB1Y.index >= start_date) & (CTB1Y.index <= end_date)]
    CTB1Y = CTB1Y - CTB1Y.shift()

    CTB5Y = pd.Series(fed_data['Market yield on U.S. Treasury securities at 5-year   constant maturity, quoted on investment basis'])
    CTB5Y.reset_index(drop=True, inplace=True)
    CTB5Y.index = fed_data['Series Description'].values
    CTB5Y = CTB5Y.dropna()
    CTB5Y = CTB5Y[CTB5Y.values != 'ND'].astype(float)
    CTB5Y = CTB5Y[(CTB5Y.index >= start_date) & (CTB5Y.index <= end_date)]
    CTB5Y = CTB5Y - CTB5Y.shift()

    CTB10Y = pd.Series(fed_data['Market yield on U.S. Treasury securities at 10-year   constant maturity, quoted on investment basis'])
    CTB10Y.reset_index(drop=True, inplace=True)
    CTB10Y.index = fed_data['Series Description'].values
    CTB10Y = CTB10Y.dropna()
    CTB10Y = CTB10Y[CTB10Y.values != 'ND'].astype(float)
    CTB10Y = CTB10Y[(CTB10Y.index >= start_date) & (CTB10Y.index <= end_date)]
    CTB10Y = CTB10Y - CTB10Y.shift()
    
    AAA = fred.get_series('DAAA', observation_start=start_date, observation_end=end_date)
    BAA = fred.get_series('DBAA', observation_start=start_date, observation_end=end_date)
    TE1 = generate_feature(T120, T1)
    TE2 = generate_feature(T120, T3)
    TE3 = generate_feature(T120, T6)
    TE5 = generate_feature(T3, T1)
    TE6 = generate_feature(T6, T1)
    DE1 = generate_feature(BAA, AAA)
    DE2 = generate_feature(BAA, T120)
    DE4 = generate_feature(BAA, T6)
    DE5 = generate_feature(BAA, T3)
    DE6 = generate_feature(BAA, T1)
    DE7 = generate_feature(CD6, T6)

    OIL = pd.read_excel("https://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls", sheet_name = 'Data 1', skiprows = 2)
    OIL = OIL.set_index('Date')
    OIL = OIL[(OIL.index >= start_date) & (OIL.index <=end_date)]
    OIL = (OIL - OIL.shift()) / OIL.shift()

    Gold = pd.read_csv('GOLD_2000-2020.csv').iloc[1:, ::]
    Gold.index = pd.to_datetime(Gold.index)

    #Investpy
    USD_Y = investpy.get_currency_cross_historical_data(currency_cross='USD/JPY', from_date=ivp_start_date, to_date=ivp_end_date)
    USD_GBP = investpy.get_currency_cross_historical_data(currency_cross='GBP/USD', from_date=ivp_start_date, to_date=ivp_end_date)
    USD_CAD = investpy.get_currency_cross_historical_data(currency_cross='USD/CAD', from_date=ivp_start_date, to_date=ivp_end_date)
    USD_CNY = investpy.get_currency_cross_historical_data(currency_cross='USD/CNY', from_date=ivp_start_date, to_date=ivp_end_date)

    
    #Yahoo Finance
    test = pd.read_csv('test.csv').loc[1:, ::]
    test.reset_index(drop=True)
    test = test.fillna(0)

    df = yf.download("SPY", start=start_date, end=end_date)

    df['SPYt']=df['Adj Close'].pct_change()
    SPYt = df['SPYt'][1:].fillna(0)
    
    df['Adj Close_l1'] = df['Adj Close'].shift(1)
    df['SPYt1']=df['Adj Close_l1'].pct_change()
    SPYt1 = df['SPYt1'][1:].fillna(0)
    
    df['Adj Close_l2'] = df['Adj Close'].shift(2)
    df['SPYt2']=df['Adj Close_l2'].pct_change()
    SPYt2 = df['SPYt2'][1:].fillna(0)

    df['Adj Close_l3'] = df['Adj Close'].shift(3)
    df['SPYt3']=df['Adj Close_l3'].pct_change()
    SPYt3 = df['SPYt3'][1:].fillna(0)

    df['Adj Close_l5'] = df['Adj Close'].shift(5)
    df['RDP5']=(df['Adj Close']-df['Adj Close_l5'])/df['Adj Close_l5']*100
    RDP5 = df['RDP5'][1:].fillna(0)

    df['Adj Close_l10'] = df['Adj Close'].shift(10)
    df['RDP10']=(df['Adj Close']-df['Adj Close_l10'])/df['Adj Close_l10']*100
    RDP10 = df['RDP10'][1:].fillna(0)

    df['Adj Close_l15'] = df['Adj Close'].shift(15)
    df['RDP15']=(df['Adj Close']-df['Adj Close_l15'])/df['Adj Close_l15']*100
    RDP15 = df['RDP15'][1:].fillna(0)

    df['Adj Close_l20'] = df['Adj Close'].shift(20)
    df['RDP20']=(df['Adj Close']-df['Adj Close_l20'])/df['Adj Close_l20']*100
    RDP20 = df['RDP20'][1:].fillna(0)

    df['EMA10']=calculate_ema(df['Adj Close'], 10)
    EMA10 = df['EMA10'][1:].replace('N/A', 0)
    
    df['EMA20']=calculate_ema(df['Adj Close'], 20)
    EMA20 = df['EMA20'][1:].replace('N/A', 0)
    
    df['EMA50']=calculate_ema(df['Adj Close'], 50)
    EMA50 = df['EMA50'][1:].replace('N/A', 0)

    df['EMA200']=calculate_ema(df['Adj Close'], 200)
    EMA200 = df['EMA200'][1:].replace('N/A', 0)
    
    df_HSI = yf.download("HSI", start=start_date, end=end_date)
    df['HSI']=df_HSI['Adj Close'].pct_change()
    HSI = df['HSI'][1:].fillna(0)

    df_SSE = yf.download("000001.SS", start=start_date, end=end_date)
    df['SSE']=df_SSE['Adj Close'].pct_change()
    SSE = df['SSE'][1:].fillna(0)

    df_FCHI = yf.download("^FCHI", start=start_date, end=end_date)
    df['FCHI']=df_FCHI['Adj Close'].pct_change()
    FCHI = df['FCHI'][1:].fillna(0)

    df_FTSE = yf.download("^FTSE", start=start_date, end=end_date)
    df['FTSE']=df_FTSE['Adj Close'].pct_change()
    FTSE  = df['FTSE'][1:].fillna(0)

    df_GDAXI = yf.download("^GDAXI", start=start_date, end=end_date)
    df['GDAXI']=df_GDAXI['Adj Close'].pct_change()
    GDAXI  = df['GDAXI'][1:].fillna(0)

    df_DJI = yf.download("DJI", start=start_date, end=end_date)
    df['DJI']=df_DJI['Adj Close'].pct_change()
    DJI = df['DJI'][1:].fillna(0)
    
    df_IXIC = yf.download("^IXIC", start=start_date, end=end_date)
    df['IXIC']=df_IXIC['Adj Close'].pct_change()
    IXIC = df['IXIC'][1:].fillna(0)

    df['V']=df['Volume'].pct_change()
    V = df['V'][1:].fillna(0)

    df_AAPL = yf.download("AAPL", start=start_date, end=end_date)
    df['AAPL']=df_AAPL['Adj Close'].pct_change()
    AAPL = df['AAPL'][1:].fillna(0)
    
    df_MSFT = yf.download("MSFT", start=start_date, end=end_date)
    df['MSFT']=df_MSFT['Adj Close'].pct_change()
    MSFT = df['MSFT'][1:].fillna(0)
    
    df_XOM = yf.download("XOM", start=start_date, end=end_date)
    df['XOM']=df_XOM['Adj Close'].pct_change()
    XOM = df['XOM'][1:].fillna(0)

    df_GE = yf.download("GE", start=start_date, end=end_date)
    df['GE']=df_GE['Adj Close'].pct_change()
    GE = df['GE'][1:].fillna(0)

    df_JNJ = yf.download("JNJ", start=start_date, end=end_date)
    df['JNJ']=df_JNJ['Adj Close'].pct_change()
    JNJ = df['JNJ'][1:].fillna(0)

    df_WFC = yf.download("WFC", start=start_date, end=end_date)
    df['WFC']=df_WFC['Adj Close'].pct_change()
    WFC = df['WFC'][1:].fillna(0)

    df_AMZN = yf.download("AMZN", start=start_date, end=end_date)
    df['AMZN']=df_AMZN['Adj Close'].pct_change()
    AMZN = df['AMZN'][1:].fillna(0)

    df_JPM = yf.download("JPM", start=start_date, end=end_date)
    df['JPM']=df_JPM['Adj Close'].pct_change()
    JPM = df['JPM'][1:].fillna(0)

    def setUp(self):
        self.vars = [self.T1, self.T3, self.T6, self.T60, self.T120, self.CD1, self.CD3, self.CD6, self.CTB3M, self.CTB6M,
                     self.CTB1Y, self.CTB5Y, self.CTB10Y, self.AAA, self.BAA, self.TE1, self.TE2, self.TE3, self.TE5, self.TE6,
                     self.DE1, self.DE2, self.DE4, self.DE5, self.DE6, self.DE7, self.OIL, self.Gold, self.USD_Y, self.USD_GBP,
                     self.USD_CAD, self.USD_CNY,
                     self.SPYt,self.SPYt, self.SPYt1, self.SPYt2, self.SPYt3, self.RDP5, self.RDP10, self.RDP15, self.RDP20,
                     self.EMA10, self.EMA20, self.EMA50, self.EMA200, self.HSI, self.SSE, self.FCHI, self.FTSE, self.GDAXI,
                     self.DJI, self.IXIC, self.V, self.AAPL, self.MSFT, self.XOM, self.GE, self.JNJ, self.WFC, self.AMZN, self.JPM]

        self.var_cal = [self.SPYt, self.SPYt1, self.SPYt2, self.SPYt3, self.RDP5, self.RDP10, self.RDP15, self.RDP20, self.EMA10,
                        self.EMA20, self.EMA50, self.EMA200]
        self.var_cal_str = ['SPYt', 'SPYt1', 'SPYt2', 'SPYt3', 'RDP5', 'RDP10', 'RDP15', 'RDP20', 'EMA10', 'EMA20', 'EMA50',
                            'EMA200']
        
    def test_date(self):
        for var in self.vars:
            self.assertTrue(var.index[0].year >= int(self.start_date[:4]))
            self.assertTrue(var.index[-1].year <= int(self.end_date[:4]))

    def test_calculation(self):
        for i in range(len(self.var_cal)):
            assert_array_equal(self.var_cal[i].round(0).values, self.test[self.var_cal_str[i]].round(0).values)
            
if __name__ == '__main__':
    unittest.main()
        
