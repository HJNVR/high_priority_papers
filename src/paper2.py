# Feature generation and pre-processing of paper 2
# user: muhdnoor
# password: WRDSaccess_135@
# Execution complete in 1 min
# ============================================================================

import pandas as pd
import os
import numpy as np
import datetime as dt
from statistics import mean
from full_fred.fred import Fred
import yfinance as yf
import wrds
import json
import sys
import time
import warnings
from pandas.tseries.offsets import MonthEnd
warnings.filterwarnings("ignore")

start = time.time()
class Paper2:
    def __init__(self):
        # Load config file
        with open('config.json') as config_file:
            self.data = json.load(config_file)
        
        self.wrds_start_date = self.data['wrds_start_date']
        self.wrds_end_date = self.data['wrds_end_date']
        self.fred_start_date = self.data['fred_start_date']
        self.fred_end_date = self.data['fred_end_date']
        #start_date = data['start_date']
        #start_year = data['start_year']
        #end_year = data['end_year']
        self.fred = Fred('paper2/fred-api.txt') 
        self.wrds_username = self.data['wrds_username']


    def generate(self):
        '''
        with open('paper2_config.json') as config_file:
            data = json.load(config_file)
            
        start_date = data['start_date']
        start_year = data['start_year']
        end_year = data['end_year']

        wrds_username = data['wrds_username']

        db = wrds.Connection(wrds_username=wrds_username)
        db.create_pgpass_file()
        db.close()
        '''

        # In[9]:


        # Generate macro variables

        conn = wrds.Connection(wrds_username=self.wrds_username)

        raw_monthly = pd.read_csv('paper2/raw_monthly.csv', index_col = 'date')
        raw_quarterly = pd.read_csv('paper2/raw_quarterly.csv', index_col = 'date')
        raw_annual = pd.read_csv('paper2/raw_annual.csv', index_col = 'date')
        
        # Query S&P500 data

        #start_date = data['start_date']
        ticker = yf.Ticker("^GSPC")
        sp500_d = ticker.history(period="max")
        sp500_d = sp500_d[['Close']]
        sp500_m = sp500_d.loc[sp500_d.groupby(sp500_d.index.to_period('M')).apply(lambda x: x.index.max())]
        sp500_m['yyyy-mm'] = sp500_m.index.to_period('M')

        # Query DJIA data (historical data not available before 1992)

        ticker = '^DJI'
        #start_date = data['start_date']
        djia_d = yf.download(ticker, self.fred_start_date)
        djia_d = djia_d[['Close']]
        djia_m = djia_d.loc[djia_d.groupby(djia_d.index.to_period('M')).apply(lambda x: x.index.max())]
        djia_m['yyyy-mm'] = djia_m.index.to_period('M')
        djia_m.rename(columns={'Close': 'djia'}, inplace=True)

        # Query monthly data from FRED

        tbl = self.fred.get_series_df('TB3MS')
        lty = self.fred.get_series_df('GS10')
        aaa = self.fred.get_series_df('AAA') 
        baa = self.fred.get_series_df('BAA') 
        cpi = self.fred.get_series_df('CPIAUCNS') 

        
        def clean_datasets(dataset):
            dataset['date'] = pd.to_datetime(dataset['date'])
            dataset = dataset[dataset['date'].dt.year >= 1955][['date', 'value']]
            dataset['value'] = dataset['value'].astype(float)
            dataset = dataset.reset_index()
            return dataset

        aaa = clean_datasets(aaa)
        baa = clean_datasets(baa)

        # Query WRDS data

        wrds_nyse = conn.raw_sql("""select caldt, totval, vwretx from crsp_q_indexes.msia where caldt >= '2000-01-01'""")
        wrds_sp500 = conn.raw_sql("""select caldt, vwretd, vwretx from crsp_q_indexes.msp500 where caldt >= '2000-01-01'""")

        # Calculate monthly data

        monthly = aaa[['date']].copy()
        monthly['yyyy-mm'] = monthly['date'].dt.to_period('M')
        monthly['BAA'] = baa['value'] / 100
        monthly['AAA'] = aaa['value'] / 100
        monthly = pd.merge(monthly, sp500_m, on=['yyyy-mm'], how='left')
        monthly.rename(columns={'Close': 'sp500'}, inplace=True)

        raw_monthly.index = pd.to_datetime(raw_monthly.index, dayfirst=True)
        raw_monthly['yyyy-mm'] = raw_monthly.index.to_period('M')
        monthly = pd.merge(monthly, raw_monthly[['yyyy-mm', 'ltr', 'corpr']], on=['yyyy-mm'], how='left')

        # Add lty data 

        lty1 = raw_monthly[['lty']].copy()
        lty1.index = pd.to_datetime(lty1.index)
        lty1 = lty1[:'2019-12-31']
        lty['date'] = pd.to_datetime(lty['date'])
        lty2 = lty[['date', 'value']]
        lty2 = lty2.set_index('date', drop=True)
        lty2 = lty2['2020-01-01':]
        lty2.rename(columns={'value': 'lty'}, inplace=True)
        lty2['lty'] = lty2['lty'].astype(float)
        lty2['lty'] = lty2['lty'] / 100
        lty_combined = pd.concat([lty1, lty2])
        lty_combined['yyyy-mm'] = lty_combined.index.to_period('M')
        monthly = pd.merge(monthly, lty_combined[['yyyy-mm', 'lty']], on=['yyyy-mm'], how='left')

        # Calculate rfree from tbl

        tbl['date'] = pd.to_datetime(tbl['date'])
        tbl['yyyy-mm'] = tbl['date'].dt.to_period('M')
        tbl['value'] = tbl.value.astype(float)
        tbl['tbl'] = tbl['value'] / 100
        tbl['Rfree'] = tbl['tbl'].shift(1) / 12
        monthly = pd.merge(monthly, tbl[['yyyy-mm', 'tbl', 'Rfree']], on=['yyyy-mm'], how='left')

        # Calculate infl from CPI index

        cpi['date'] = pd.to_datetime(cpi['date'])
        cpi['yyyy-mm'] = cpi['date'].dt.to_period('M')
        cpi['value'] = cpi.value.astype(float)
        cpi['infl'] = cpi['value'] / cpi['value'].shift(1) - 1
        monthly = pd.merge(monthly, cpi[['yyyy-mm', 'infl']], on=['yyyy-mm'], how='left')

        # Calculate svar from daily S&P500 prices

        sp500_d.index = pd.to_datetime(sp500_d.index)
        sp500_d['yyyy-mm'] = sp500_d.index.to_period('M')
        sp500_d['Close'] = sp500_d.Close.astype(float)
        sp500_d['svar'] = (sp500_d['Close'] / sp500_d['Close'].shift(1) - 1) ** 2
        svar_calc = sp500_d[['yyyy-mm', 'svar']].groupby(['yyyy-mm']).sum().reset_index()
        monthly = pd.merge(monthly, svar_calc, on=['yyyy-mm'], how='left')

        # Add WRDS S&P500 data

        wrds_sp500['caldt'] = pd.to_datetime(wrds_sp500['caldt'])
        wrds_sp500['yyyy-mm'] = wrds_sp500['caldt'].dt.to_period('M')
        wrds_sp500.rename(columns={'vwretd': 'CRSP_SPvw', 'vwretx': 'CRSP_SPvwx'}, inplace=True)
        monthly = pd.merge(monthly, wrds_sp500[['yyyy-mm', 'CRSP_SPvw', 'CRSP_SPvwx']], on=['yyyy-mm'], how='left')

        # Calculate ntis from WRDS NYSE data

        wrds_nyse['caldt'] = pd.to_datetime(wrds_nyse['caldt'])
        wrds_nyse['yyyy-mm'] = wrds_nyse['caldt'].dt.to_period('M')
        wrds_nyse['net_issue'] = wrds_nyse['totval'] * 1000 - wrds_nyse['totval'].shift(1) * 1000 * (1 + wrds_nyse['vwretx'])
        wrds_nyse['net_issue_12'] = wrds_nyse['net_issue'].rolling(min_periods=12, window=12).sum()

        wrds_nyse['ntis'] = wrds_nyse['net_issue_12'] / (wrds_nyse['totval'] * 1000)
        monthly = pd.merge(monthly, wrds_nyse[['yyyy-mm', 'ntis']], on=['yyyy-mm'], how='left')

        # Calculate e12 and d12 based on interpolating quarterly data

        raw_quarterly['E12'] = raw_quarterly['eps'].rolling(min_periods=4, window=4).sum()
        raw_quarterly['D12'] = raw_quarterly['dps'].rolling(min_periods=4, window=4).sum()
        raw_quarterly.index = pd.to_datetime(raw_quarterly.index, dayfirst=True)
        raw_quarterly = raw_quarterly.reindex(pd.date_range(start=raw_quarterly.index.min(), end=raw_quarterly.index.max(), freq='M'))
        raw_quarterly = raw_quarterly.interpolate(method='linear', limit_area='inside')

        raw_quarterly['yyyy-mm'] = raw_quarterly.index.to_period('M')
        monthly = pd.merge(monthly, raw_quarterly[['yyyy-mm', 'E12', 'D12']], on=['yyyy-mm'], how='left')

        # Calculate b/m based on annual book value and monthly price

        calc_bm = raw_monthly[['djia']].copy()
        calc_bm.index = pd.to_datetime(calc_bm.index)
        calc_bm['yyyy-mm'] = calc_bm.index.to_period('M')
        calc_bm = calc_bm[:'2019-12-31']
        calc_bm = pd.concat([calc_bm, djia_m])

        calc_bm['book_year'] = calc_bm['yyyy-mm'].apply(lambda x: x.year - 1 if x.month > 2 else x.year - 2)
        calc_bm['book'] = calc_bm['book_year'].map(raw_annual['djia_book'])
        calc_bm['b/m'] = calc_bm['book'] / calc_bm['djia']

        monthly = pd.merge(monthly, calc_bm[['yyyy-mm', 'b/m']], on=['yyyy-mm'], how='left')

        # Query quarterly data
        pnfi = self.fred.get_series_df('PNFI') 
        deflator = self.fred.get_series_df('A008RD3Q086SBEA') 

        # Ensure pnfi and deflator data start at 1947
        pnfi['date'] = pd.to_datetime(pnfi['date'])
        pnfi = pnfi[pnfi['date'].dt.year >= 1947][['date', 'value']]
        pnfi_list = np.array(pnfi['value']).astype(float)

        deflator['date'] = pd.to_datetime(deflator['date'])
        deflator = deflator[deflator['date'].dt.year >= 1947][['date', 'value']]
        deflator_list = np.array(deflator['value']).astype(float)

        # Convert nominal PNFI to real (2012) values
        pnfir = np.divide(pnfi_list, deflator_list) * 100

        # Calculate i/k
        allI = pnfir / 4
        delta = 0.1 / 4                                     # Depreciation rate for making capital out of investment - follows Goyal & Welch (2008)
        ikbar = delta + mean(allI[1:] / allI[:-1]) - 1      # Stationary i/k in k_t+1 = (1-delta)k_t + i_t - follows Goyal & Welch (2008)
        allk = 0 * allI
        allk[0] = allI[0] / ikbar
        for i in range(1, len(allk - 1)):
            allk[i] = (1 - delta) * allk[i-1] + allI[i-1]
        allik = allI / allk

        ik = pnfi[['date']].copy()
        ik['yyyy-mm'] = ik['date'].dt.to_period('M')
        ik['ik'] = allik
        ik = ik.reset_index(drop=True)

        monthly = pd.merge(monthly, ik[['yyyy-mm', 'ik']], on=['yyyy-mm'], how='left')
        monthly['ik'] = monthly['ik'].ffill(axis=0, limit=3)

        # Calculate eqis
        monthly['year'] = monthly['date'].dt.year
        raw_annual['year'] = raw_annual.index
        eqis = raw_annual[raw_annual['year'] >= 1955][['year', 'equity', 'debt']]
        eqis = eqis.reset_index(drop=True)
        eqis['equity'] = eqis['equity'].astype(float)
        eqis['debt'] = eqis['debt'].astype(float)
        eqis['eqis'] = eqis['equity'] / (eqis['equity'] + eqis['debt'])
        monthly = pd.merge(monthly, eqis[['year', 'eqis']], on=['year'], how='left')
        monthly['eqis'] = monthly['eqis'].ffill(axis=0, limit=12)
        monthly = monthly.drop(["year"], axis=1)


        
        # In[12]:


        # Generate all macro features

        raw = monthly

        # Copy feature variables
        features = raw[['date', 'yyyy-mm', 'CRSP_SPvw', 'CRSP_SPvwx', 'Rfree', 'b/m', 'tbl', 'svar', 'ntis', 
                        'lty', 'ltr', 'infl', 'ik', 'eqis']].copy()
        features['date'] = pd.to_datetime(features['date'])

        #1 Dividend price ratio
        features['d/p'] = np.log(raw['D12']) - np.log(raw['sp500'])

        #2 Earnings price ratio
        features['e/p'] = np.log(raw['E12']) - np.log(raw['sp500'])

        #7 Term spread
        features['tms'] = raw['lty'] - raw['tbl']

        #8 Default yield spread
        features['dfy'] = raw['BAA'] - raw['AAA']

        #9 Dividend yield
        features['d/y'] = np.log(raw['D12']) - np.log(raw['sp500'].shift(1))

        #10 Dividend payout ratio
        features['d/e'] = np.log(raw['D12']) - np.log(raw['E12'])

        #15 Default return spread
        features['dfr'] = raw['corpr'] - raw['ltr']

        # Start at chosen start date
        features = features[features['date'] >= self.wrds_start_date]
        #features = features.iloc[::2,:]
        
        df = yf.download("SPY", start=self.fred_start_date, end=self.fred_end_date)
        df.reset_index(inplace=True)
        df.columns = df.columns.str.lower()
        daily_prc = df
        daily_prc['date'] = pd.to_datetime(daily_prc['date'])
        daily_prc['year'] = daily_prc['date'].dt.year
        daily_prc['month'] = daily_prc['date'].dt.month
        daily_prc = daily_prc.sort_values(['date']).reset_index(drop=True)
        lvlm_dt_ref = daily_prc.groupby(['year', 'month'])['date'].max()
        lvlm_dt_ref #purpose is to find the last trading day of the month
        date_dic = {}
        df_date = lvlm_dt_ref.reset_index(level=['year','month'])
        df_date.set_index('date',inplace=True)
        df_date['YYMMDD']=df_date.index.astype(str)
        df_date.index=df_date.index.to_period('M')
        df_date.index=df_date.index.astype(str)

        #features.reset_index(inplace=True)
        features = features.drop(columns=['date'])
        features = features.rename(columns={'yyyy-mm': 'date'})
        features.columns= features.columns.str.lower()
        features['date'] = features['date'].astype(str)
        features = features.iloc[:-3, :]
        dates = []

        drop_index = []
        count = 0
        for i in features.date:
            try:
                dates.append(df_date[df_date.index == i]['YYMMDD'][0])
            except:
                drop_index.append(count)
            count += 1
        #features = features.drop(drop_index)
        #print(features.shape)
        #print(len(dates))
        features['date'] = dates
        features['date'] = pd.to_datetime(features['date'])
        features.insert(0, 'date', features.pop('date'))
        features.to_csv("../result/paper2/paper2_features.csv", index=False)
        print('Paper2 completed 1/1')
        return features
        
        
if __name__ == "__main__":
    Paper2().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))