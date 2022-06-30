# Feature generation and pre-processing of paper 9
# each SP 500 company has one csv file merged with common feature
# Execution complete in 3 mins
# ============================================================================

import numpy as np
import pandas as pd
import sys
import time
import wrds
import json
import investpy
import yfinance as yf
from fredapi import Fred
import warnings
warnings.filterwarnings("ignore")

start = time.time()
class Paper9:
    # function to calculate all features

    def cal_export(self, df):
        df = self.close(df)
        df = self.spyt(df)
        df = self.spyt1(df)
        df = self.spyt2(df)
        df = self.spyt3(df)
        df = self.rdp5(df)
        df = self.rdp10(df)
        df = self.rdp15(df)
        df = self.rdp20(df)
        df = self.ema10(df)
        df = self.ema20(df)
        df = self.ema50(df)
        df = self.ema200(df)
        df = self.fut_ret1(df)
        df = self.fut_ret2(df)

        df.index = df.index.to_period('M')
        return df

    def close(self, df):
        df.rename(columns={'prc': 'close'}, inplace=True)
        return df.drop(['vol'], axis=1)

    def fut_ret1(self, df):
        df['fut_ret1']=(df['close'].shift(-1)-df['close'])/df['close']
        return df
    
    def fut_ret2(self, df):
        df['fut_ret2']=(df['close'].shift(-2)-df['close'])/df['close']
        return df

    # 3 SPYt

    def spyt(self, df):
        df['spyt'] = df['close'].pct_change()
        return df

    # 4 SPYt1

    def spyt1(self, df):
        df['Close_l1'] = df['close'].shift(1)
        df['spyt1'] = df['Close_l1'].pct_change()
        return df.drop(['Close_l1'], axis=1)

    # 5 SPYt2

    def spyt2(self, df):
        df['Close_l2'] = df['close'].shift(2)
        df['spyt2'] = df['Close_l2'].pct_change()
        return df.drop(['Close_l2'], axis=1)

    # 6 SPYt3

    def spyt3(self, df):
        df['Close_l3'] = df['close'].shift(3)
        df['spyt3'] = df['Close_l3'].pct_change()
        return df.drop(['Close_l3'], axis=1)

    # 7 RDP5

    def rdp5(self, df):
        df['Close_l5'] = df['close'].shift(5)
        df['rdp5'] = (df['close']-df['Close_l5'])/df['Close_l5']*100
        return df.drop(['Close_l5'], axis=1)

    # 8 RDP10

    def rdp10(self, df):
        df['Close_l10'] = df['close'].shift(10)
        df['rdp10'] = (df['close']-df['Close_l10'])/df['Close_l10']*100
        return df.drop(['Close_l10'], axis=1)

    # 9 RDP15

    def rdp15(self, df):
        df['Close_l15'] = df['close'].shift(15)
        df['rdp15'] = (df['close']-df['Close_l15'])/df['Close_l15']*100
        return df.drop(['Close_l15'], axis=1)

    # 10 RDP20

    def rdp20(self, df):
        df['Close_l20'] = df['close'].shift(20)
        df['rdp20'] = (df['close']-df['Close_l20'])/df['Close_l20']*100
        return df.drop(['Close_l20'], axis=1)

    # 11 EMA10

    def calculate_ema(self, prices, days, smoothing=2):
        ema = []
        if len(prices) < days:
            ema += [0]*len(prices)
        else:
            for num_day in range(days-1):
                ema.append(0)
            ema.append(sum(prices[:days]) / days)
            for price in prices[days:]:
                ema.append((price * (smoothing / (1 + days))) +
                           ema[-1] * (1 - (smoothing / (1 + days))))
        return ema

    def ema10(self, df):
        df['ema10'] = self.calculate_ema(df['close'], 10)
        return df

    # 12 EMA20

    def ema20(self, df):
        df['ema20'] = self.calculate_ema(df['close'], 20)
        return df

    # 13 EMA50

    def ema50(self, df):
        df['ema50'] = self.calculate_ema(df['close'], 50)
        return df

    # 14 EMA200

    def ema200(self, df):
        df['ema200'] = self.calculate_ema(df['close'], 200)
        return df

    def generate_common_features(self):
        start_date = '2000-01-01'
        end_date = '2020-12-31'

        df = yf.download("SPY", start=start_date, end=end_date)

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
        df['RDP5'] = (df['Adj Close']-df['Adj Close_l5']) / \
            df['Adj Close_l5']*100

        # 8 RDP10
        df['Adj Close_l10'] = df['Adj Close'].shift(10)
        df['RDP10'] = (df['Adj Close']-df['Adj Close_l10']) / \
            df['Adj Close_l10']*100

        # 9 RDP15
        df['Adj Close_l15'] = df['Adj Close'].shift(15)
        df['RDP15'] = (df['Adj Close']-df['Adj Close_l15']) / \
            df['Adj Close_l15']*100

        # 10 RDP20
        df['Adj Close_l20'] = df['Adj Close'].shift(20)
        df['RDP20'] = (df['Adj Close']-df['Adj Close_l20']) / \
            df['Adj Close_l20']*100

        # 11 EMA10
        df['EMA10'] = self.calculate_ema(df['Adj Close'], 10)

        # 12 EMA20
        df['EMA20'] = self.calculate_ema(df['Adj Close'], 20)

        # 13 EMA50
        df['EMA50'] = self.calculate_ema(df['Adj Close'], 50)

        # 14 EMA200
        df['EMA200'] = self.calculate_ema(df['Adj Close'], 200)

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

        # Fred API key: ab766afb0df13dba8492403a7865f852
        fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')
        t1 = fred.get_series(
            'DTB4WK', observation_start=start_date, observation_end=end_date)
        t1.index.name = 'Date'
        t1.rename('T1', inplace=True)

        t3 = fred.get_series(
            'DTB3', observation_start=start_date, observation_end=end_date)
        t3.index.name = 'Date'
        t3.rename('T3', inplace=True)

        t6 = fred.get_series(
            'DTB6', observation_start=start_date, observation_end=end_date)
        t6.index.name = 'Date'
        t6.rename('T6', inplace=True)

        t60 = fred.get_series(
            'DGS5', observation_start=start_date, observation_end=end_date)
        t60.index.name = 'Date'
        t60.rename('T60', inplace=True)

        t120 = fred.get_series(
            'DGS10', observation_start=start_date, observation_end=end_date)
        t120.index.name = 'Date'
        t120.rename('T120', inplace=True)

        cd1 = fred.get_series(
            'DCD1M', observation_start=start_date, observation_end=end_date)
        cd1.index.name = 'Date'
        cd1.rename('CD1', inplace=True)

        cd3 = fred.get_series(
            'DCD90', observation_start=start_date, observation_end=end_date)
        cd3.index.name = 'Date'
        cd3.rename('CD3', inplace=True)

        cd6 = fred.get_series(
            'DCD6M', observation_start=start_date, observation_end=end_date)
        cd6.index.name = 'Date'
        cd6.rename('CD6', inplace=True)

        data = pd.read_csv('paper9/FRB_H15.csv')
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

        ctb5y = pd.Series(
            data['Market yield on U.S. Treasury securities at 5-year   constant maturity, quoted on investment basis'])
        ctb5y.reset_index(drop=True, inplace=True)
        ctb5y.index = data['Series Description'].values
        ctb5y = ctb5y.dropna()
        ctb5y = ctb5y[ctb5y.values != 'ND'].astype(float)
        ctb5y = ctb5y - ctb5y.shift()
        ctb5y = ctb5y[(ctb5y.index >= start_date) & (ctb5y.index <= end_date)]
        ctb5y.index.name = 'Date'
        ctb5y.rename('CTB5Y', inplace=True)

        ctb10y = pd.Series(
            data['Market yield on U.S. Treasury securities at 10-year   constant maturity, quoted on investment basis'])
        ctb10y.reset_index(drop=True, inplace=True)
        ctb10y.index = data['Series Description'].values
        ctb10y = ctb10y.dropna()
        ctb10y = ctb10y[ctb10y.values != 'ND'].astype(float)
        ctb10y = ctb10y - ctb10y.shift()
        ctb10y = ctb10y[(ctb10y.index >= start_date)
                        & (ctb10y.index <= end_date)]
        ctb10y.index.name = 'Date'
        ctb10y.rename('CTB10Y', inplace=True)

        aaa = fred.get_series(
            'DAAA', observation_start=start_date, observation_end=end_date)
        aaa.index.name = 'Date'
        aaa.rename('AAA', inplace=True)

        baa = fred.get_series(
            'DBAA', observation_start=start_date, observation_end=end_date)
        baa.index.name = 'Date'
        baa.rename('BAA', inplace=True)

        te1 = t120 - t1
        te1.index.name = 'Date'
        te1.rename('TE1', inplace=True)

        te2 = t120 - t3
        te2.index.name = 'Date'
        te2.rename('TE2', inplace=True)

        te3 = t120 - t6
        te3.index.name = 'Date'
        te3.rename('TE3', inplace=True)

        te5 = t3 - t1
        te5.index.name = 'Date'
        te5.rename('TE5', inplace=True)

        te6 = t6 - t1
        te6.index.name = 'Date'
        te6.rename('TE6', inplace=True)

        de1 = baa - aaa
        de1.index.name = 'Date'
        de1.rename('DE1', inplace=True)

        de2 = baa - t120
        de2.index.name = 'Date'
        de2.rename('DE2', inplace=True)

        de4 = baa - t6
        de4.index.name = 'Date'
        de4.rename('DE4', inplace=True)

        de5 = baa - t3
        de5.index.name = 'Date'
        de5.rename('DE5', inplace=True)

        de6 = baa - t1
        de6.index.name = 'Date'
        de6.rename('DE6', inplace=True)

        de7 = cd6 - t6
        de7.index.name = 'Date'
        de7.rename('DE7', inplace=True)

        data = pd.read_excel(
            "https://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls", sheet_name='Data 1', skiprows=2)
        data = data.set_index('Date')

        oil = (data - data.shift()) / data.shift()  # need SPY dates as control
        oil = oil[(oil.index >= start_date) & (oil.index <= end_date)]
        oil.rename(columns={
                   'Cushing, OK WTI Spot Price FOB (Dollars per Barrel)': 'Oil'}, inplace=True)

        # investpy
        start_date = "31/12/1999"
        end_date = "31/12/2020"

        usd_y = investpy.get_currency_cross_historical_data(
            currency_cross='USD/JPY', from_date=start_date, to_date=end_date)
        usd_y = usd_y['Close']
        usd_y.rename('USD_Y', inplace=True)

        usd_gbp = investpy.get_currency_cross_historical_data(
            currency_cross='GBP/USD', from_date=start_date, to_date=end_date)
        usd_gbp = usd_gbp['Close']
        usd_gbp.rename('USD_GBP', inplace=True)

        usd_cad = investpy.get_currency_cross_historical_data(
            currency_cross='USD/CAD', from_date=start_date, to_date=end_date)
        usd_cad = usd_cad['Close']
        usd_cad.rename('USD_CAD', inplace=True)

        usd_cny = investpy.get_currency_cross_historical_data(
            currency_cross='USD/CNY', from_date=start_date, to_date=end_date)
        usd_cny = usd_cny['Close']
        usd_cny.rename('USD_CNY', inplace=True)

        gold = investpy.get_currency_cross_historical_data(
            currency_cross='XAU/USD', from_date=start_date, to_date=end_date)
        gold['Gold'] = (gold['Close'] - gold['Close'].shift()) / \
            gold['Close'].shift()
        gold = gold['Gold']

        df = df[['Close', 'SPYt', 'SPYt1', 'SPYt2', 'SPYt3', 'RDP5', 'RDP10', 'RDP15', 'RDP20', 'EMA10', 'EMA20', 'EMA50',
                 'EMA200', 'HSI', 'SSE', 'FCHI', 'FTSE', 'GDAXI', 'DJI', 'IXIC', 'V', 'AAPL', 'MSFT', 'XOM', 'GE', 'JNJ', 'WFC', 'AMZN', 'JPM']]

        df = df.join(t1)
        df = df.join(t3)
        df = df.join(t6)
        df = df.join(t60)
        df = df.join(t120)
        df = df.join(cd1)
        df = df.join(cd3)
        df = df.join(cd6)
        df = df.join(oil)
        df = df.join(gold)
        df = df.join(ctb3m)
        df = df.join(ctb6m)
        df = df.join(ctb1y)
        df = df.join(ctb5y)
        df = df.join(ctb10y)
        df = df.join(aaa)
        df = df.join(baa)
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
        df = df.join(de7)
        df = df.join(usd_y)
        df = df.join(usd_gbp)
        df = df.join(usd_cad)
        df = df.join(usd_cny)
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

    def generate(self):
        # load config file
        with open('config.json') as config_file:
            data = json.load(config_file)

        start_year = data['start_year']
        end_year = data['end_year']
        wrds_username = data['wrds_username']

        parm = {"start_year": start_year, "end_year": end_year}

        # import list of S&P500 company tickers
        # query data from WRDS
        #user: muhdnoor
        # password: WRDSaccess_135@
        conn = wrds.Connection(wrds_username=wrds_username)

        crsp_msf = conn.raw_sql("""
                              select a.date, a.permno, a.prc, a.vol
                              from crsp.msf as a
                              left join crsp.msenames as b
                              on a.permno=b.permno
                              where b.namedt<=a.date
                              and a.date<=b.nameendt
                              and a.date >= '01/01/%(start_year)s'
                              and a.date <= '12/31/%(end_year)s'
                              and b.exchcd between 1 and 3
                              """, params=parm)

        conn.close()

        # change date and price column format in dataframe
        crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
        crsp_msf.set_index('date', inplace=True)
        crsp_msf['prc'] = crsp_msf['prc'].astype(float)

        # import list of S&P500 company tickers/permnos
        stocks = pd.read_csv('paper9/S&P500 companies list (2000 to 2020).csv')
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

        daily_prc = pd.read_csv('sp500_daily_prc.csv')
        daily_prc['date'] = pd.to_datetime(daily_prc['date'])
        daily_prc['year'] = daily_prc['date'].dt.year
        daily_prc['month'] = daily_prc['date'].dt.month
        daily_prc = daily_prc.sort_values(['ticker', 'date']).reset_index(drop=True)
        lvlm_dt_ref = daily_prc.groupby(['year', 'month'])['date'].max()
        lvlm_dt_ref #purpose is to find the last trading day of the month
        date_dic = {}
        df_date = lvlm_dt_ref.reset_index(level=['year','month'])
        df_date.set_index('date',inplace=True)
        df_date['YYMMDD']=df_date.index.astype(str)
        df_date.index=df_date.index.to_period('M')
        df_date.index=df_date.index.astype(str)

        #df.to_csv('../result/paper9/common_features.csv')

        # calculate features and export as csv file
        #final_csv = pd.DataFrame()
        common_features = self.generate_common_features()
        paper9_final = pd.DataFrame()
        ticker_files = {}
        count = 1
        for k, v in dic.items():
            csv_file = self.cal_export(v).merge(common_features, left_on=['date'], right_on=['date'])
            csv_file['ticker'] = dic_map[k]
            csv_file.insert(2, 'ticker', csv_file.pop('ticker'))
            csv_file.columns= csv_file.columns.str.lower()
            csv_file = csv_file.sort_values(by='date')
            csv_file.reset_index(inplace=True)
            csv_file = csv_file.drop(columns=['index'])
            csv_file['date'] = csv_file['date'].astype(str)
            dates = []
            for i in csv_file.index:
                dates.append(df_date[df_date.index == csv_file['date'][i]]['YYMMDD'][0])
            csv_file['date'] = dates
            csv_file['date'] = pd.to_datetime(csv_file['date'])
            if count == 1:
                paper9_final = csv_file
            else:
                try:
                    paper9_final = pd.concat([paper9_final, csv_file], ignore_index=True)
                except:
                    # delisted companies
                    pass
            ticker_files[dic_map[k]] = csv_file
            #csv_file.to_csv('../result/paper9/'+dic_map[k]+'.csv')
            print("\rPaper9 completed {}/{}.".format(count, 835), end='')
            sys.stdout.flush()
            count += 1
        paper9_final.to_csv('../result/paper9/paper9_features.csv')
        
        return ticker_files


if __name__ == "__main__":
    Paper9().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))