# Feature generation and pre-processing of paper 11
# each SP 500 company has one csv file merged with common feature
# Execution complete in 3 mins
# ============================================================================

import numpy as np
import pandas as pd
import sys
import time
import wrds
import json
import datetime
import investpy
import yfinance as yf
from fredapi import Fred
import warnings
warnings.filterwarnings("ignore")
# function to calculate all features

start = time.time()


class Paper11():
    def __init__(self):
        with open('config.json') as config_file:
            self.data = json.load(config_file)

        # logic check #1. check stock pool
        self.stock_pool = self.data['stock_pool']
        with open('stock_pool_list.txt') as f:
            self.stock_pool_list = [line.rstrip() for line in f]
        if self.stock_pool not in self.stock_pool_list:
            raise Exception('Please pick valid stock_pool: ',
                            self.stock_pool_list)

        # logic check #2. check date
        if bool(datetime.datetime.strptime(self.data['start_date'], '%Y-%m-%d')):
            self.fred_start_date = self.data['start_date']
        if bool(datetime.datetime.strptime(self.data['end_date'], '%Y-%m-%d')):
            self.fred_end_date = self.data['end_date']
        if datetime.datetime.strptime(self.data['start_date'], '%Y-%m-%d') > \
           datetime.datetime.strptime(self.data['end_date'], '%Y-%m-%d'):
            raise Exception('Start_date should be earlier than end_date')

        start_year = self.data['start_date'][:self.data['start_date'].find(
            '-')]
        start_month = self.data['start_date'][self.data['start_date'].find(
            '-')+1:self.data['start_date'].rfind('-')]
        start_day = self.data['start_date'][self.data['start_date'].rfind(
            '-')+1:]
        end_year = self.data['end_date'][:self.data['end_date'].find('-')]
        end_month = self.data['end_date'][self.data['end_date'].find(
            '-')+1:self.data['end_date'].rfind('-')]
        end_day = self.data['end_date'][self.data['end_date'].rfind('-')+1:]

        self.wrds_start_date = start_month+'/'+start_day+'/'+start_year
        self.wrds_end_date = end_month+'/'+end_day+'/'+end_year
        self.investpy_start_date = start_day+'/'+start_month+'/'+start_year
        self.investpy_end_date = end_day+'/'+end_month+'/'+end_year
        self.features_df = pd.DataFrame()
        self.wrds_username = self.data['wrds_username']
        self.fred = Fred(api_key=self.data['fred_api_key'])
        self.parm = {"start_date": self.wrds_start_date,
                     "end_date": self.wrds_end_date}

    def cal_export(self, df):
        df = self.day(df)
        df = self.close(df)
        df = self.vol(df)
        df = self.mom1(df)
        df = self.mom2(df)
        df = self.mom3(df)
        df = self.roc5(df)
        df = self.roc10(df)
        df = self.roc15(df)
        df = self.roc20(df)
        df = self.ema10(df)
        df = self.ema20(df)
        df = self.ema50(df)
        df = self.ema200(df)
        df = self.fut_ret1(df)
        df = self.fut_ret2(df)
        df.index = df.index.to_period("M")
        return df

    # functions to calculate stock specific features
    # 1 day

    def day(self, df):
        df["Date"] = df.index.values
        df["day"] = df["Date"].dt.dayofweek
        return df.drop(["Date"], axis=1)

    # 2 Close

    def close(self, df):
        df.rename(columns={"prc": "close"}, inplace=True)
        return df

    def fut_ret1(self, df):
        df['fut_ret1'] = (df['close'].shift(-1)-df['close'])/df['close']
        return df

    def fut_ret2(self, df):
        df['fut_ret2'] = (df['close'].shift(-2)-df['close'])/df['close']
        return df

    # 3 VOL

    def vol(self, df):
        df["vol"] = df["vol"].pct_change().values
        return df

    # 4 MOM-1

    def mom1(self, df):
        df["Close_l1"] = df["close"].shift(1)
        df["mom1"] = df["Close_l1"].pct_change()
        return df.drop(["Close_l1"], axis=1)

    # 5 MOM-2

    def mom2(self, df):
        df["Close_l2"] = df["close"].shift(2)
        df["mom2"] = df["Close_l2"].pct_change()
        return df.drop(["Close_l2"], axis=1)

    # 6 MOM-3

    def mom3(self, df):
        df["Close_l3"] = df["close"].shift(3)
        df["mom3"] = df["Close_l3"].pct_change()
        return df.drop(["Close_l3"], axis=1)

    # 7 ROC-5

    def roc5(self, df):
        df["Close_l5"] = df["close"].shift(5)
        df["roc5"] = (df["close"] - df["Close_l5"]) / df["Close_l5"]
        return df.drop(["Close_l5"], axis=1)

    # 8 ROC-10

    def roc10(self, df):
        df["Close_l10"] = df["close"].shift(10)
        df["roc10"] = (df["close"] - df["Close_l10"]) / df["Close_l10"]
        return df.drop(["Close_l10"], axis=1)

    # 9 ROC-15

    def roc15(self, df):
        df["Close_l15"] = df["close"].shift(15)
        df["roc15"] = (df["close"] - df["Close_l15"]) / df["Close_l15"]
        return df.drop(["Close_l15"], axis=1)

    # 10 ROC-20

    def roc20(self, df):
        df["Close_l20"] = df["close"].shift(20)
        df["roc20"] = (df["close"] - df["Close_l20"]) / df["Close_l20"]
        return df.drop(["Close_l20"], axis=1)

    # 11 EMA10

    def calculate_ema(self, prices, days, smoothing=2):
        ema = []
        if len(prices) < days:
            ema += [0] * len(prices)
        else:
            for num_day in range(days - 1):
                ema.append(0)
            ema.append(sum(prices[:days]) / days)
            for price in prices[days:]:
                ema.append(
                    (price * (smoothing / (1 + days)))
                    + ema[-1] * (1 - (smoothing / (1 + days)))
                )
        return ema

    def ema10(self, df):
        df["ema10"] = self.calculate_ema(df["close"], 10)
        return df

    # 12 EMA20

    def ema20(self, df):
        df["ema20"] = self.calculate_ema(df["close"], 20)
        return df

    # 13 EMA50

    def ema50(self, df):
        df["ema50"] = self.calculate_ema(df["close"], 50)
        return df

    # 14 EMA200

    def ema200(self, df):
        df["ema200"] = self.calculate_ema(df["close"], 200)
        return df

    def generate_ticker_files(self):

        csv_files = {}
        # query data from WRDS
        conn = wrds.Connection(wrds_username=self.wrds_username)

        crsp_msf = conn.raw_sql("""
                              select a.date, a.permno, a.prc, a.vol
                              from crsp.msf as a
                              left join crsp.msenames as b
                              on a.permno=b.permno
                              where b.namedt<=a.date
                              and a.date<=b.nameendt
                              and a.date >= %(start_date)s
                              and a.date <= %(end_date)s
                              and b.exchcd between 1 and 3
                              """, params=self.parm)

        conn.close()

        # change date and price column format in dataframe
        crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
        crsp_msf.set_index('date', inplace=True)
        crsp_msf['prc'] = crsp_msf['prc'].astype(float)

        # import list of S&P500 company tickers/permnos
        stocks = pd.read_csv('S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]

        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = crsp_msf[crsp_msf['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                df = self.cal_export(v)
                df.reset_index(inplace=True)
                csv_files[dic_map[k]] = df

        return csv_files

    def sp500_generate_common_features(self):
        start_date = self.fred_start_date
        end_date = self.fred_end_date

        df = yf.download("WMT", start=start_date, end=end_date)

        # Calculate features

        # 1 Day
        df["Date"] = df.index
        df["Day"] = df["Date"].dt.dayofweek

        # 2 Close

        # 3 VOL
        df["VOL"] = df["Volume"].pct_change()

        # 4 MOM-1
        df["Adj Close_l1"] = df["Adj Close"].shift(1)
        df["MOM1"] = df["Adj Close_l1"].pct_change()

        # 5 MOM-2
        df["Adj Close_l2"] = df["Adj Close"].shift(2)
        df["MOM2"] = df["Adj Close_l2"].pct_change()

        # 6 MOM-3
        df["Adj Close_l2"] = df["Adj Close"].shift(2)
        df["MOM3"] = df["Adj Close_l2"].pct_change()

        # 7 ROC-5
        df["Adj Close_l5"] = df["Adj Close"].shift(5)
        df["ROC5"] = (df["Adj Close"] - df["Adj Close_l5"]) / \
            df["Adj Close_l5"]

        # 8 ROC-10
        df["Adj Close_l10"] = df["Adj Close"].shift(10)
        df["ROC10"] = (df["Adj Close"] - df["Adj Close_l10"]) / \
            df["Adj Close_l10"]

        # 9 ROC-15
        df["Adj Close_l15"] = df["Adj Close"].shift(15)
        df["ROC15"] = (df["Adj Close"] - df["Adj Close_l15"]) / \
            df["Adj Close_l15"]

        # 10 ROC-20
        df["Adj Close_l20"] = df["Adj Close"].shift(20)
        df["ROC20"] = (df["Adj Close"] - df["Adj Close_l20"]) / \
            df["Adj Close_l20"]

        # 11 EMA-10
        def calculate_ema(prices, days, smoothing=2):
            ema = []
            for num_day in range(days - 1):
                ema.append("N/A")
            ema.append(sum(prices[:days]) / days)
            for price in prices[days:]:
                ema.append(
                    (price * (smoothing / (1 + days)))
                    + ema[-1] * (1 - (smoothing / (1 + days)))
                )
            return ema

        df["EMA10"] = calculate_ema(df["Adj Close"], 10)

        # 12 EMA-20
        df["EMA20"] = calculate_ema(df["Adj Close"], 20)

        # 13 EMA-50
        df["EMA50"] = calculate_ema(df["Adj Close"], 50)

        # 14 EMA-200
        df["EMA200"] = calculate_ema(df["Adj Close"], 200)

        # 45 IXIC
        df_IXIC = yf.download("^IXIC", start=start_date, end=end_date)
        df["IXIC"] = df["Adj Close"].pct_change()

        # 46 GSPC
        df_GSPC = yf.download("^GSPC", start=start_date, end=end_date)
        df["GSPC"] = df_GSPC["Adj Close"].pct_change()

        # 47 DJI
        df_DJI = yf.download("DJI", start=start_date, end=end_date)
        df["DJI"] = df_DJI["Adj Close"].pct_change()

        # 48 NYSE
        df_NYSE = yf.download("^NYA", start=start_date, end=end_date)
        df["NYSE"] = df_NYSE["Adj Close"].pct_change()

        # 49 RUSSELL
        df_RUSSELL = yf.download("^RUT", start=start_date, end=end_date)
        df["RUSSELL"] = df_DJI["Adj Close"].pct_change()

        # 50 HSI
        df_HSI = yf.download("HSI", start=start_date, end=end_date)
        df["HSI"] = df_HSI["Adj Close"].pct_change()

        # 51 SSE
        df_SSE = yf.download("000001.SS", start=start_date, end=end_date)
        df["SSE"] = df_SSE["Adj Close"].pct_change()

        # 52 FCHI
        df_FCHI = yf.download("^FCHI", start=start_date, end=end_date)
        df["FCHI"] = df_FCHI["Adj Close"].pct_change()

        # 53 FTSE
        df_FTSE = yf.download("^FTSE", start=start_date, end=end_date)
        df["FTSE"] = df_FTSE["Adj Close"].pct_change()

        # 54 GDAXI
        df_GDAXI = yf.download("^GDAXI", start=start_date, end=end_date)
        df["GDAXI"] = df_GDAXI["Adj Close"].pct_change()

        # 55 USD-Y
        df_USDY = yf.download("JPY=X", start=start_date, end=end_date)
        df["USDY"] = df_USDY["Adj Close"].pct_change()

        # 56 USD-GBP
        df_USDGBP = yf.download("GBP=X", start=start_date, end=end_date)
        df["USDGBP"] = df_USDGBP["Adj Close"].pct_change()

        # 57 USD-CAD
        df_USDCAD = yf.download("CAD=X", start=start_date, end=end_date)
        df["USDCAD"] = df_USDCAD["Adj Close"].pct_change()

        # 58 USD-CNY
        df_USDCNY = yf.download("CNY=X", start=start_date, end=end_date)
        df["USDCNY"] = df_USDCNY["Adj Close"].pct_change()

        # 64 XOM
        df_XOM = yf.download("XOM", start=start_date, end=end_date)
        df["XOM"] = df_XOM["Adj Close"].pct_change()

        # 65 JPM
        df_JPM = yf.download("JPM", start=start_date, end=end_date)
        df["JPM"] = df_JPM["Adj Close"].pct_change()

        # 66 AAPL
        df_AAPL = yf.download("AAPL", start=start_date, end=end_date)
        df["AAPL"] = df_AAPL["Adj Close"].pct_change()

        # 67 MSFT
        df_MSFT = yf.download("MSFT", start=start_date, end=end_date)
        df["MSFT"] = df_MSFT["Adj Close"].pct_change()

        # 68 GE
        df_GE = yf.download("GE", start=start_date, end=end_date)
        df["GE"] = df_GE["Adj Close"].pct_change()

        # 69 JNJ
        df_JNJ = yf.download("JNJ", start=start_date, end=end_date)
        df["JNJ"] = df_JNJ["Adj Close"].pct_change()

        # 70 WFC
        df_WFC = yf.download("WFC", start=start_date, end=end_date)
        df["WFC"] = df_WFC["Adj Close"].pct_change()

        # 71 AMZN
        df_AMZN = yf.download("AMZN", start=start_date, end=end_date)
        df["AMZN"] = df_AMZN["Adj Close"].pct_change()

        df = df[['Close', 'IXIC', 'GSPC', 'DJI', 'NYSE', 'RUSSELL', 'HSI', 'SSE', 'FCHI', 'FTSE',
                 'GDAXI', 'USDY', 'USDGBP', 'USDCAD', 'USDCNY', 'XOM', 'JPM', 'AAPL',
                 'MSFT', 'GE', 'JNJ', 'WFC', 'AMZN']]

        start_date = '30/12/1999'
        end_date = self.investpy_end_date

        oil_brent = investpy.search.search_quotes(
            text='LCO', n_results=1).retrieve_historical_data(from_date=start_date, to_date=end_date)
        oil_brent.rename(columns={'Change Pct': 'OIL_BRENT'}, inplace=True)

        oil_wti = investpy.get_commodity_historical_data(
            commodity='Crude Oil WTI', from_date=start_date, to_date=end_date)
        oil_wti['OIL_WTI'] = (
            oil_wti['Close'] - oil_wti['Close'].shift()) / oil_wti['Close'].shift()

        gold_f = investpy.search.search_quotes(text='ZG', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        gold_f.rename(columns={'Change Pct': 'Gold_F'}, inplace=True)

        xau_usd = investpy.get_currency_cross_historical_data(
            currency_cross='XAU/USD', from_date=start_date, to_date=end_date)
        xau_usd['XAU_USD'] = (
            xau_usd['Close'] - xau_usd['Close'].shift()) / xau_usd['Close'].shift()

        xag_usd = investpy.get_currency_cross_historical_data(
            currency_cross='XAG/USD', from_date=start_date, to_date=end_date)
        xag_usd['XAG_USD'] = (
            xag_usd['Close'] - xag_usd['Close'].shift()) / xag_usd['Close'].shift()

        gas = investpy.search.search_quotes(text='NG', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        gas.rename(columns={'Change Pct': 'Gas'}, inplace=True)

        silver = investpy.search.search_quotes(text='ZI', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        silver.rename(columns={'Change Pct': 'Silver'}, inplace=True)

        copper = investpy.search.search_quotes(text='HG', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        copper.rename(columns={'Change Pct': 'Copper'}, inplace=True)

        usd_aud = investpy.get_currency_cross_historical_data(
            currency_cross='USD/AUD', from_date=start_date, to_date=end_date)
        usd_aud['USD_AUD'] = (
            usd_aud['Close'] - usd_aud['Close'].shift()) / usd_aud['Close'].shift()

        usd_nzd = investpy.get_currency_cross_historical_data(
            currency_cross='USD/NZD', from_date=start_date, to_date=end_date)
        usd_nzd['USD_NZD'] = (
            usd_nzd['Close'] - usd_nzd['Close'].shift()) / usd_nzd['Close'].shift()

        usd_chf = investpy.get_currency_cross_historical_data(
            currency_cross='USD/CHF', from_date=start_date, to_date=end_date)
        usd_chf['USD_CHF'] = (
            usd_chf['Close'] - usd_chf['Close'].shift()) / usd_chf['Close'].shift()

        usd_eur = investpy.get_currency_cross_historical_data(
            currency_cross='USD/EUR', from_date=start_date, to_date=end_date)
        usd_eur['USD_EUR'] = (
            usd_eur['Close'] - usd_eur['Close'].shift()) / usd_eur['Close'].shift()

        usdx = investpy.search.search_quotes(text='DXY', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        usdx.rename(columns={'Change Pct': 'USDX'}, inplace=True)

        fchi_f = investpy.search.search_quotes(text='F40', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        fchi_f.rename(columns={'Change Pct': 'FCHI_F'}, inplace=True)

        ftse_f = investpy.search.search_quotes(text='FTSE 100 Futures', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        ftse_f.rename(columns={'Change Pct': 'FTSE_F'}, inplace=True)

        gdaxi_f = investpy.search.search_quotes(text='DAX Futures', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        gdaxi_f.rename(columns={'Change Pct': 'GDAXI_F'}, inplace=True)

        hsi_f = investpy.search.search_quotes(text='HK50', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        hsi_f.rename(columns={'Change Pct': 'HSI_F'}, inplace=True)

        nikkei_f = investpy.search.search_quotes(
            text='Nikkei 225 Futures', n_results=1).retrieve_historical_data(from_date=start_date, to_date=end_date)
        nikkei_f.rename(columns={'Change Pct': 'NIKKEI_F'}, inplace=True)

        kospi_f = investpy.search.search_quotes(text='KOR200c1', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        kospi_f.rename(columns={'Change Pct': 'KOSPI_F'}, inplace=True)

        ixic_f = investpy.search.search_quotes(text='NQU2', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        ixic_f.rename(columns={'Change Pct': 'IXIC_F'}, inplace=True)

        dji_f = investpy.search.search_quotes(text='1YMU2', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        dji_f.rename(columns={'Change Pct': 'DJI_F'}, inplace=True)

        sp_f = investpy.search.search_quotes(text='VIXL', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        sp_f.rename(columns={'Change Pct': 'SP_F'}, inplace=True)

        russell_f = investpy.search.search_quotes(
            text='US2000', n_results=1).retrieve_historical_data(from_date=start_date, to_date=end_date)
        russell_f.rename(columns={'Change Pct': 'RUSSELL_F'}, inplace=True)

        usdx_f = investpy.search.search_quotes(text='DX', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        usdx_f.rename(columns={'Change Pct': 'USDX_F'}, inplace=True)

        # Fred API key: ab766afb0df13dba8492403a7865f852
        fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')
        dtb4wk = fred.get_series(
            'DTB4WK', observation_start=start_date, observation_end=end_date)
        dtb4wk.index.name = 'Date'
        dtb4wk.rename('DTB4WK', inplace=True)

        dtb3 = fred.get_series(
            'DTB3', observation_start=start_date, observation_end=end_date)
        dtb3.index.name = 'Date'
        dtb3.rename('DTB3', inplace=True)

        dtb6 = fred.get_series(
            'DTB6', observation_start=start_date, observation_end=end_date)
        dtb6.index.name = 'Date'
        dtb6.rename('DTB6', inplace=True)

        dgs5 = fred.get_series(
            'DGS5', observation_start=start_date, observation_end=end_date)
        dgs5.index.name = 'Date'
        dgs5.rename('DGS5', inplace=True)

        dgs10 = fred.get_series(
            'DGS10', observation_start=start_date, observation_end=end_date)
        dgs10.index.name = 'Date'
        dgs10.rename('DGS10', inplace=True)

        data = pd.read_csv('paper11/FRB_H15.csv')
        data = data.iloc[5:, :]
        data['Series Description'] = pd.to_datetime(data['Series Description'])

        ctb3m = pd.Series(
            data['Market yield on U.S. Treasury securities at 3-month   constant maturity, quoted on investment basis'])
        ctb3m.reset_index(drop=True, inplace=True)
        ctb3m.index = data['Series Description'].values
        ctb3m = ctb3m.dropna()
        ctb3m = ctb3m[ctb3m.values != 'ND'].astype(float)
        ctb3m = ctb3m - ctb3m.shift()
        ctb3m = ctb3m[(ctb3m.index >= start_date) & (ctb3m.index <= end_date)]
        ctb3m.index.name = 'Date'
        ctb3m.rename('CTB3M', inplace=True)

        ctb6m = pd.Series(
            data['Market yield on U.S. Treasury securities at 6-month   constant maturity, quoted on investment basis'])
        ctb6m.reset_index(drop=True, inplace=True)
        ctb6m.index = data['Series Description'].values
        ctb6m = ctb6m.dropna()
        ctb6m = ctb6m[ctb6m.values != 'ND'].astype(float)
        ctb6m = ctb6m - ctb6m.shift()
        ctb6m = ctb6m[(ctb6m.index >= start_date) & (ctb6m.index <= end_date)]
        ctb6m.index.name = 'Date'
        ctb6m.rename('CTB6M', inplace=True)

        ctb1y = pd.Series(
            data['Market yield on U.S. Treasury securities at 1-year   constant maturity, quoted on investment basis'])
        ctb1y.reset_index(drop=True, inplace=True)
        ctb1y.index = data['Series Description'].values
        ctb1y = ctb1y.dropna()
        ctb1y = ctb1y[ctb1y.values != 'ND'].astype(float)
        ctb1y = ctb1y - ctb1y.shift()
        ctb1y = ctb1y[(ctb1y.index >= start_date) & (ctb1y.index <= end_date)]
        ctb1y.index.name = 'Date'
        ctb1y.rename('CTB1Y', inplace=True)

        daaa = fred.get_series(
            'DAAA', observation_start=start_date, observation_end=end_date)
        daaa.index.name = 'Date'
        daaa.rename('DAAA', inplace=True)

        dbaa = fred.get_series(
            'DBAA', observation_start=start_date, observation_end=end_date)
        dbaa.index.name = 'Date'
        dbaa.rename('DBAA', inplace=True)

        te1 = dgs10 - dtb4wk
        te1.index.name = 'Date'
        te1.rename('TE1', inplace=True)

        te2 = dgs10 - dtb3
        te2.index.name = 'Date'
        te2.rename('TE2', inplace=True)

        te3 = dgs10 - dtb6
        te3.index.name = 'Date'
        te3.rename('TE3', inplace=True)

        te5 = dtb3 - dtb4wk
        te5.index.name = 'Date'
        te5.rename('TE5', inplace=True)

        te6 = dtb6 - dtb4wk
        te6.index.name = 'Date'
        te6.rename('TE6', inplace=True)

        de1 = dbaa - daaa
        de1.index.name = 'Date'
        de1.rename('DE1', inplace=True)

        de2 = dbaa - dgs10
        de2.index.name = 'Date'
        de2.rename('DE2', inplace=True)

        de4 = dbaa - dtb6
        de4.index.name = 'Date'
        de4.rename('DE4', inplace=True)

        de5 = dbaa - dtb3
        de5.index.name = 'Date'
        de5.rename('DE5', inplace=True)

        de6 = dbaa - dtb4wk
        de6.index.name = 'Date'
        de6.rename('DE6', inplace=True)

        data = pd.read_excel(
            "https://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls", sheet_name='Data 1', skiprows=2)
        data = data.set_index('Date')

        oil = (data - data.shift()) / data.shift()  # need SPY dates as control
        oil = oil[(oil.index >= start_date) & (oil.index <= end_date)]
        oil.rename(columns={
            'Cushing, OK WTI Spot Price FOB (Dollars per Barrel)': 'Oil'}, inplace=True)

        gold = investpy.get_currency_cross_historical_data(
            currency_cross='XAU/USD', from_date=start_date, to_date=end_date)
        gold['Gold'] = (gold['Close'] - gold['Close'].shift()) / \
            gold['Close'].shift()
        gold = gold['Gold']

        df = df.join(oil_brent['OIL_BRENT'])
        df = df.join(oil_wti['OIL_WTI'])
        df = df.join(gold_f['Gold_F'])
        df = df.join(xau_usd['XAU_USD'])
        df = df.join(xag_usd['XAG_USD'])
        df = df.join(gas['Gas'])
        df = df.join(silver['Silver'])
        df = df.join(copper['Copper'])
        df = df.join(usd_aud['USD_AUD'])
        df = df.join(usd_nzd['USD_NZD'])
        df = df.join(usd_chf['USD_CHF'])
        df = df.join(usd_eur['USD_EUR'])
        df = df.join(usdx['USDX'])
        df = df.join(fchi_f['FCHI_F'])
        df = df.join(ftse_f['FTSE_F'])
        df = df.join(gdaxi_f['GDAXI_F'])
        df = df.join(hsi_f['HSI_F'])
        df = df.join(nikkei_f['NIKKEI_F'])
        df = df.join(kospi_f['KOSPI_F'])
        df = df.join(ixic_f['IXIC_F'])
        df = df.join(dji_f['DJI_F'])
        df = df.join(sp_f['SP_F'])
        df = df.join(russell_f['RUSSELL_F'])
        df = df.join(usdx_f['USDX_F'])
        df = df.join(dtb4wk)
        df = df.join(dtb3)
        df = df.join(dtb6)
        df = df.join(dgs5)
        df = df.join(dgs10)
        df = df.join(ctb3m)
        df = df.join(ctb6m)
        df = df.join(ctb1y)
        df = df.join(daaa)
        df = df.join(dbaa)
        df = df.join(te1)
        df = df.join(te2)
        df = df.join(te3)
        df = df.join(te5)
        df = df.join(te6)
        df = df.join(de1)
        df = df.join(de2)
        df = df.join(de4)
        df = df.join(de5)
        df = df.join(de6)
        df = df.join(oil)
        df = df.join(gold)
        df = df.iloc[1:, :]

        # convert to monthly
        df = df.groupby([df.index.year, df.index.month]).last()
        df.index.rename(['year', 'month'], inplace=True)
        df.reset_index(level=['year', 'month'], inplace=True)
        df['year'] = df['year'].astype('str')
        df['month'] = df['month'].astype('str')
        df['date'] = df['year'] + '-' + df['month']
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.index = df.index.to_period('M')
        df.reset_index(inplace=True)
        df.drop(columns=['year', 'month'], inplace=True)

        return df

    def sp500_generate(self):
        # password: WRDSaccess_135@
        conn = wrds.Connection(wrds_username=self.wrds_username)

        crsp_msf = conn.raw_sql("""
                              select a.date, a.permno, a.prc, a.vol
                              from crsp.msf as a
                              left join crsp.msenames as b
                              on a.permno=b.permno
                              where b.namedt<=a.date
                              and a.date<=b.nameendt
                              and a.date >= %(start_date)s
                              and a.date <= %(end_date)s
                              and b.exchcd between 1 and 3
                              """, params=self.parm)

        conn.close()

        # change date and price column format in dataframe
        crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
        crsp_msf.set_index('date', inplace=True)
        crsp_msf['prc'] = crsp_msf['prc'].astype(float)

        # import list of S&P500 company tickers/permnos
        stocks = pd.read_csv('S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]
        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = crsp_msf[crsp_msf['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        df = yf.download("SPY", start=self.fred_start_date,
                         end=self.fred_end_date)
        df.reset_index(inplace=True)
        df.columns = df.columns.str.lower()
        daily_prc = df
        daily_prc['date'] = pd.to_datetime(daily_prc['date'])
        daily_prc['year'] = daily_prc['date'].dt.year
        daily_prc['month'] = daily_prc['date'].dt.month
        daily_prc = daily_prc.sort_values(['date']).reset_index(drop=True)
        lvlm_dt_ref = daily_prc.groupby(['year', 'month'])['date'].max()
        lvlm_dt_ref  # purpose is to find the last trading day of the month
        date_dic = {}
        df_date = lvlm_dt_ref.reset_index(level=['year', 'month'])
        df_date.set_index('date', inplace=True)
        df_date['YYMMDD'] = df_date.index.astype(str)
        df_date.index = df_date.index.to_period('M')
        df_date.index = df_date.index.astype(str)

        # df.to_csv('../result/paper9/common_features.csv')

        # calculate features and export as csv file
        #final_csv = pd.DataFrame()
        common_features = self.sp500_generate_common_features()
        paper11_final = pd.DataFrame()
        ticker_files = {}
        count = 1
        for k, v in dic.items():
            csv_file = self.cal_export(v).merge(
                common_features, left_on=['date'], right_on=['date'])
            csv_file['ticker'] = dic_map[k]
            csv_file.insert(2, 'ticker', csv_file.pop('ticker'))
            csv_file.columns = csv_file.columns.str.lower()
            csv_file = csv_file.sort_values(by='date')
            csv_file.reset_index(inplace=True)
            csv_file = csv_file.drop(columns=['index'])
            csv_file['date'] = csv_file['date'].astype(str)
            dates = []
            for i in csv_file.index:
                dates.append(
                    df_date[df_date.index == csv_file['date'][i]]['YYMMDD'][0])
            csv_file['date'] = dates
            csv_file['date'] = pd.to_datetime(csv_file['date'])
            if count == 1:
                paper11_final = csv_file
            else:
                try:
                    paper11_final = pd.concat(
                        [paper11_final, csv_file], ignore_index=True)
                except:
                    # delisted companies
                    pass
            ticker_files[dic_map[k]] = csv_file
            # csv_file.to_csv('../result/paper9/'+dic_map[k]+'.csv')
            print("\rPaper11 completed {}/{}.".format(count, 835), end='')
            sys.stdout.flush()
            count += 1
        paper11_final.to_csv('../result/sp500/paper11/paper11_features.csv')
        return ticker_files

    def generate(self):
        if self.stock_pool == 'sp500':
            return self.sp500_generate()


if __name__ == "__main__":
    Paper11().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(
        int(hours), int(minutes), seconds))
    print('Please check /result/' + Paper11().stock_pool +
          '/paper11/paper11_features.csv')
