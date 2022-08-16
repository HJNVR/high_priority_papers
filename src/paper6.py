# Feature generation and pre-processing of paper 6
# Execution complete in 2 mins
# ============================================================================

import numpy as np
import pandas as pd
import sys
import time
import wrds
import json
import investpy
import datetime
import yfinance as yf
from fredapi import Fred
import warnings
warnings.filterwarnings("ignore")

start = time.time()


class Paper6:
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
        self.wrds_username = self.data['wrds_username']
        self.features_df = pd.DataFrame()
        self.fred = Fred(api_key=self.data['fred_api_key'])
        self.parm = {"start_date": self.wrds_start_date,
                     "end_date": self.wrds_end_date}

    # function to calculate all features
    def cal_export(self, df):
        df = self.calculate_ma(df)
        df = self.calculate_momentum(df)
        df = self.calculate_d(df)
        df = self.calculate_maobv(df)
        df = self.ma_1_9(df)
        df = self.ma_2_9(df)
        df = self.ma_3_9(df)
        df = self.ma_1_12(df)
        df = self.ma_2_12(df)
        df = self.ma_3_12(df)
        df = self.mom9(df)
        df = self.mom12(df)
        df = self.vol_1_9(df)
        df = self.vol_2_9(df)
        df = self.vol_3_9(df)
        df = self.vol_1_12(df)
        df = self.vol_2_12(df)
        df = self.vol_3_12(df)
        df = self.fut_ret1(df)
        df = self.fut_ret2(df)
        df = self.cleanup(df)

        df.index = df.index.to_period('M')
        return df

    # functions to calculate stock specific features
    def fut_ret1(self, df):
        df['fut_ret1'] = (df['prc'].shift(-1)-df['prc'])/df['prc']
        return df

    def fut_ret2(self, df):
        df['fut_ret2'] = (df['prc'].shift(-2)-df['prc'])/df['prc']
        return df
    # Calculate MA

    def calculate_ma(self, df):
        df['ma1'] = df['prc'].rolling(1, min_periods=1).mean()
        df['ma2'] = df['prc'].rolling(2, min_periods=1).mean()
        df['ma3'] = df['prc'].rolling(3, min_periods=1).mean()
        df['ma9'] = df['prc'].rolling(9, min_periods=1).mean()
        df['ma12'] = df['prc'].rolling(12, min_periods=1).mean()
        return df

    # Calculate Momentum

    def calculate_momentum(self, df):
        df['Close_l1'] = df['prc'].shift(1)
        df['Close_l9'] = df['prc'].shift(9)
        df['Close_l12'] = df['prc'].shift(12)
        return df

    # Calculate D

    def calculate_d(self, df):
        d = []
        d.append(1)
        for i in range(1, len(df['prc'])):
            if df['prc'][i] >= df['Close_l1'][i]:
                d.append(1)
            else:
                d.append(-1)
        df['d'] = d
        return df

    # calculate moving average of OBV

    def calculate_maobv(self, df):
        obv = []
        for i in range(len(df['prc'])):
            obv.append(df['vol'][i]*df['d'][i])

        df['obv'] = obv
        df['maobv1'] = df['obv'].rolling(1, min_periods=1).mean()
        df['maobv2'] = df['obv'].rolling(2, min_periods=1).mean()
        df['maobv3'] = df['obv'].rolling(3, min_periods=1).mean()
        df['maobv9'] = df['obv'].rolling(9, min_periods=1).mean()
        df['maobv12'] = df['obv'].rolling(12, min_periods=1).mean()
        return df

    # Calculate features

    # paper6
    # 1 MA(1,9)
    def ma_1_9(self, df):
        ma_1_9 = []
        for i in range(len(df['prc'])):
            if df['ma1'][i] >= df['ma9'][i]:
                ma_1_9.append(1)
            else:
                ma_1_9.append(0)
        df['ma_1_9'] = ma_1_9
        return df

    # 2 MA(1,12)

    def ma_1_12(self, df):
        ma_1_12 = []
        for i in range(len(df['prc'])):
            if df['ma1'][i] >= df['ma12'][i]:
                ma_1_12.append(1)
            else:
                ma_1_12.append(0)
        df['ma_1_12'] = ma_1_12
        return df

    # 3 MA(2,9)

    def ma_2_9(self, df):
        ma_2_9 = []
        for i in range(len(df['prc'])):
            if df['ma2'][i] >= df['ma9'][i]:
                ma_2_9.append(1)
            else:
                ma_2_9.append(0)
        df['ma_2_9'] = ma_2_9
        return df

    # 4 MA(2,12)

    def ma_2_12(self, df):
        ma_2_12 = []
        for i in range(len(df['prc'])):
            if df['ma2'][i] >= df['ma12'][i]:
                ma_2_12.append(1)
            else:
                ma_2_12.append(0)
        df['ma_2_12'] = ma_2_12
        return df

    # 5 MA(3,9)

    def ma_3_9(self, df):
        ma_3_9 = []
        for i in range(len(df['prc'])):
            if df['ma3'][i] >= df['ma9'][i]:
                ma_3_9.append(1)
            else:
                ma_3_9.append(0)
        df['ma_3_9'] = ma_3_9
        return df

    # 6 MA(3,12)

    def ma_3_12(self, df):
        ma_3_12 = []
        for i in range(len(df['prc'])):
            if df['ma3'][i] >= df['ma12'][i]:
                ma_3_12.append(1)
            else:
                ma_3_12.append(0)
        df['ma_3_12'] = ma_3_12
        return df

    # 7 MOM9

    def mom9(self, df):
        mom9 = []
        for i in range(len(df['prc'])):
            if df['prc'][i] >= df['Close_l9'][i]:
                mom9.append(1)
            else:
                mom9.append(0)
        df['mom9'] = mom9
        return df

    # 8 MOM12

    def mom12(self, df):
        mom12 = []
        for i in range(len(df['prc'])):
            if df['prc'][i] >= df['Close_l12'][i]:
                mom12.append(1)
            else:
                mom12.append(0)
        df['mom12'] = mom12
        return df

    # 9 VOL(1,9)

    def vol_1_9(self, df):
        vol_1_9 = []
        for i in range(len(df['prc'])):
            if df['maobv1'][i] >= df['maobv9'][i]:
                vol_1_9.append(1)
            else:
                vol_1_9.append(0)
        df['vol_1_9'] = vol_1_9
        return df

    # 10 VOL(1,12)

    def vol_1_12(self, df):
        vol_1_12 = []
        for i in range(len(df['prc'])):
            if df['maobv1'][i] >= df['maobv12'][i]:
                vol_1_12.append(1)
            else:
                vol_1_12.append(0)
        df['vol_1_12'] = vol_1_12
        return df

    # 11 VOL(2,9)

    def vol_2_9(self, df):
        vol_2_9 = []
        for i in range(len(df['prc'])):
            if df['maobv2'][i] >= df['maobv9'][i]:
                vol_2_9.append(1)
            else:
                vol_2_9.append(0)
        df['vol_2_9'] = vol_2_9
        return df

    # 12 VOL(2,12)

    def vol_2_12(self, df):
        vol_2_12 = []
        for i in range(len(df['prc'])):
            if df['maobv2'][i] >= df['maobv12'][i]:
                vol_2_12.append(1)
            else:
                vol_2_12.append(0)
        df['vol_2_12'] = vol_2_12
        return df

    # 13 VOL(3,9)

    def vol_3_9(self, df):
        vol_3_9 = []
        for i in range(len(df['prc'])):
            if df['maobv3'][i] >= df['maobv9'][i]:
                vol_3_9.append(1)
            else:
                vol_3_9.append(0)
        df['vol_3_9'] = vol_3_9
        return df

    # 14 VOL(3,12)

    def vol_3_12(self, df):
        vol_3_12 = []
        for i in range(len(df['prc'])):
            if df['maobv3'][i] >= df['maobv12'][i]:
                vol_3_12.append(1)
            else:
                vol_3_12.append(0)
        df['vol_3_12'] = vol_3_12
        return df
    # clean-up unnecessary columns

    def cleanup(self, df):
        return df.drop(['prc', 'vol',
                        'ma1', 'ma2', 'ma3', 'ma9', 'ma12',
                        'Close_l1', 'Close_l9', 'Close_l12',
                       'd', 'obv', 'maobv1', 'maobv2', 'maobv3', 'maobv9', 'maobv12'], axis=1)

    def sp500_generate(self):
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

        #crsp_msf['ticker'] = [dic_map[k]] * df.shape[0]

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

        # calculate features and export as csv file
        paper6_final = pd.DataFrame()
        ticker_files = {}
        count = 1
        for k, v in dic.items():
            if v.shape[0] > 1:
                csv_file = self.cal_export(v)
                csv_file['ticker'] = dic_map[k]
                csv_file.insert(1, 'ticker', csv_file.pop('ticker'))
                csv_file.columns = csv_file.columns.str.lower()
                csv_file = csv_file.sort_values(by='date')
                csv_file.reset_index(inplace=True)
                # print(csv_file)
                #csv_file = csv_file.drop(columns=['index'])
                csv_file['date'] = csv_file['date'].astype(str)
                dates = []
                for i in csv_file.index:
                    dates.append(
                        df_date[df_date.index == csv_file['date'][i]]['YYMMDD'][0])
                csv_file['date'] = dates
                csv_file['date'] = pd.to_datetime(csv_file['date'])
                if count == 1:
                    paper6_final = csv_file
                else:
                    try:
                        paper6_final = pd.concat(
                            [paper6_final, csv_file], ignore_index=True)
                    except:
                        # delisted companies
                        pass
                ticker_files[dic_map[k]] = csv_file
                # csv_file.to_csv('../result/paper9/'+dic_map[k]+'.csv')
                print("\rPaper6 completed {}/{}.".format(count, 833), end='')
                sys.stdout.flush()
                count += 1
        paper6_final.to_csv('../result/sp500/paper6/paper6_features.csv')
        return ticker_files

    def generate(self):
        if self.stock_pool == 'sp500':
            return self.sp500_generate()


if __name__ == "__main__":
    Paper6().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nFiles completed in {:0>2}:{:0>2}:{:05.2f}".format(
        int(hours), int(minutes), seconds))
    print('Please check /result/' + Paper6().stock_pool +
          '/paper6/paper6_features.csv')
