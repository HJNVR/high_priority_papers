# Feature generation and pre-processing of paper1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
# Paper 1, 8 features are dups
# user: muhdnoor
# password: WRDSaccess_135@
# Execution complete in 30 mins
# ============================================================================

import numpy as np
import pandas as pd
import datetime
import wrds
import time
import json
import sys
import investpy
import yfinance as yf
from fredapi import Fred
from pandas.tseries.offsets import MonthEnd
import warnings
warnings.filterwarnings("ignore")

from paper1 import Paper1
from paper2 import Paper2
from paper3 import Paper3
from paper4 import Paper4
from paper5 import Paper5
from paper6 import Paper6
from paper7 import Paper7
from paper9 import Paper9
from paper10 import Paper10
from paper11 import Paper11

start = time.time()

paper1_csv_files = Paper1().generate()
paper2_csv_files = Paper2().generate()
paper3_csv_files = Paper3().generate()
paper4_csv_files = Paper4().generate()
paper5_csv_files = Paper5().generate()
paper6_csv_files = Paper6().generate()
paper7_csv_files = Paper7().generate()
paper9_csv_files = Paper9().generate()
paper10_csv_files = Paper10().generate()
paper11_csv_files = Paper11().generate()
final = pd.DataFrame()
count = 1
for k in paper6_csv_files.keys():
    df = paper6_csv_files[k].merge(paper9_csv_files[k], left_on=[
                                       'date', 'permno'], right_on=['date', 'permno'])
    try: 
        df = df.merge(paper1_csv_files[k], left_on='date', right_on='date')
    except:
        pass # avoid delisted
    df = df.merge(paper2_csv_files, left_on='date', right_on='date')
    df = df.merge(paper3_csv_files[k], left_on='date', right_on='date')
    df = df.merge(paper4_csv_files[k], left_on='date', right_on='date')
    df = df.merge(paper5_csv_files, left_on='date', right_on='date')
    df = df.merge(paper7_csv_files[k], left_on='date', right_on='date')
    df = df.merge(paper10_csv_files, left_on='date', right_on='date')
    df = df.merge(paper11_csv_files[k], left_on='date', right_on='date')
    df.reset_index(inplace=True, drop=True)
    if count == 1:
        final = df
    else:
        try:
            final = pd.concat([final, df], ignore_index=True)
        except:
            pass

    print("\rConsolidated features completed {}/{}.".format(count, 833), end='')
    sys.stdout.flush()
    count += 1

# clean features
print('Cleaning features ... ')
drop_list = []
for col in final.columns[1:]:
    # ex: 'permno_x' , check if 'permno' in col_name
    if 'permno' in col and len(col) != len('permno'):
        if 'permno' not in final.columns:
            try:
                final['permno'] = final[col].values
            except:
                final['permno'] = final[col].values[:, 0]
        else:
            drop_list.append(col)
    # ex: 'ticker_x' , check if 'ticker' in col_name
    if 'ticker' in col and len(col) != len('ticker'):
        #df = df.drop(columns=[col])
        drop_list.append(col)
    if 'date' in col:
        drop_list.append(col)

final = final.drop(columns=drop_list)
final.insert(1, 'permno', final.pop('permno'))
final.insert(2, 'ticker', final.pop('ticker'))
final.to_csv('../result/consolidated/consolidated_features.csv')
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
'''
# paper1
    # ticker AET permno should be 46850.0 | some columns are all empty
    paper1_csv_files = Paper1().generate_files()
    # print(paper_1_csv_files['AET'])

    # paper2
    paper2_csv_files = Paper2().generate_files()

    # paper3
    paper3_csv_files = Paper3().generate_files()

    # paper4
    paper4_csv_files = Paper4().generate_files()

    # paper5
    paper5_csv_files = Paper5().generate_files()

    # paper6
    paper6_csv_files = Paper6().generate_ticker_files()

    # paper7
    paper7_csv_files = Paper7().generate_files()

    # paper9
    paper9_csv_files = Paper9().generate_ticker_files()
    paper9_common_features = Paper9().generate_common_features()

    # paper10
    paper10_common_features = Paper10().generate_common_features()

    # paper11
    paper11_csv_files = Paper11().generate_ticker_files()
    paper11_common_features = Paper11().generate_common_features()

    daily_prc = pd.read_csv('sp500_daily_prc.csv')
    daily_prc['date'] = pd.to_datetime(daily_prc['date'])
    daily_prc['year'] = daily_prc['date'].dt.year
    daily_prc['month'] = daily_prc['date'].dt.month
    daily_prc = daily_prc.sort_values(
        ['ticker', 'date']).reset_index(drop=True)
    lvlm_dt_ref = daily_prc.groupby(['year', 'month'])['date'].max()
    lvlm_dt_ref  # purpose is to find the last trading day of the month
    date_dic = {}
    df_date = lvlm_dt_ref.reset_index(level=['year', 'month'])
    df_date.set_index('date', inplace=True)
    df_date['YYMMDD'] = df_date.index.astype(str)
    df_date.index = df_date.index.to_period('M')
    df_date.index = df_date.index.astype(str)

    for i in df_date.index:
        date_dic[i] = df_date['YYMMDD'][i]

    for k in paper6_csv_files.keys():
        # paper6_csv_files[k].reset_index(inplace=True)
        # paper7_csv_files[0][k].reset_index(inplace=True)
        # paper9_csv_files[k].reset_index(inplace=True)
        # paper11_csv_files[k].reset_index(inplace=True)
        # paper6_csv_files[k].to_csv('../result/paper6/'+k+'.csv')
        # paper7_csv_files[0][k].to_csv('../result/paper7/'+k+'.csv')
        # paper9_csv_files[k].to_csv('../result/paper9/'+k+'.csv')
        # paper11_csv_files[k].to_csv('../result/paper11/'+k+'.csv')

        df = paper6_csv_files[k].merge(paper9_csv_files[k], left_on=[
                                       'date', 'permno'], right_on=['date', 'permno'])
        try:
            df = df.merge(paper1_csv_files[k], left_on='date', right_on='DATE')
        except:
            pass

        df = df.merge(paper2_csv_files, left_on='date', right_on='yyyy-mm')
        df = df.merge(paper3_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper4_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper5_csv_files, left_on='date', right_on='sasdate')
        df = df.merge(paper7_csv_files[0][k], left_on='date', right_on='date')
        # common features
        df = df.merge(paper7_csv_files[1], left_on='date', right_on='sasdate')
        df = df.merge(paper9_common_features, left_on='date', right_on='date')
        df = df.merge(paper10_common_features, left_on='date', right_on='Date')
        df = df.merge(paper11_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper11_common_features, left_on='date', right_on='date')

        #df['date'] = df['date'].astype(str)
        temp = df.copy()
        temp['date'] = temp['date'].astype(str)
        l = []
        for i in temp.index:
            l.append(df_date[df_date.index == temp['date'][i]]['YYMMDD'][0])
        df['date'] = l
        df = df.sort_values(by='date')
        df.columns = df.columns.str.lower()
        if 'ticker' not in df.columns:
            df['ticker'] = k
        dates = df['date'].values
        if dates.shape[1] > 1:
            dates = df['date'].values[:, 0]

        drop_list = []
        count = 0
        for col in df.columns:
            # ex: 'permno_x' , check if 'permno' in col_name
            if 'permno' in col and len(col) != len('permno'):
                if 'permno' not in df.columns:
                    try:
                        df['permno'] = df[col].values
                    except:
                        df['permno'] = df[col].values[:, 0]
                else:
                    #df = df.drop(columns=[col])
                    drop_list.append(col)
            # ex: 'ticker_x' , check if 'ticker' in col_name
            if 'ticker' in col and len(col) != len('ticker'):
                #df = df.drop(columns=[col])
                drop_list.append(col)
            if 'date' in col:
                    drop_list.append(col)
        drop_list = drop_list + ['yyyy-mm', 'index']
        df = df.drop(columns=drop_list)
        df['date'] = dates
        df.insert(0, 'date', df.pop('date'))
        df.insert(1, 'permno', df.pop('permno'))
        df.insert(2, 'ticker', df.pop('ticker'))
        df.to_csv('../result/consolidated_papers/'+k+'.csv')
'''
'''
consolidated_features = Paper6().generate().merge(Paper9().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper1().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper2().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper3().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper4().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper5().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper7().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper10().generate(), left_on='date', right_on='date')
consolidated_features = consolidated_features.merge(Paper11().generate(), left_on='date', right_on='date')

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution complete in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))

'''
'''
class Paper1:
    def generate_files(self): 
        df = pd.read_csv('paper1_features.csv', index_col=[0])
        df['DATE'] = pd.to_datetime(df['DATE'])

        stocks = pd.read_csv('paper1/S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]

        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = df[df['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        ticker_files = {}
        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                # cal_export(v).to_csv('../result/consolidated_papers/'+file_name+'.csv')
                v.set_index('DATE', inplace=True)
                v.index = v.index.to_period('M')
                v.reset_index(inplace=True)
                ticker_files[dic_map[k]] = v

        return ticker_files


class Paper2:
    def generate_files(self):
        df = pd.read_csv('paper2_features.csv', index_col=[0])
        df.set_index('yyyy-mm', inplace=True)
        df.index = pd.to_datetime(df.index)
        df.index = df.index.to_period('M')
        df.reset_index(inplace=True)
        return df


class Paper3:
    def generate_files(self):
        df = pd.read_csv('paper3_features.csv', index_col=[0])
        df['date'] = pd.to_datetime(df['date'])

        stocks = pd.read_csv('paper1/S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]

        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = df[df['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        ticker_files = {}
        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                # cal_export(v).to_csv('../result/consolidated_papers/'+file_name+'.csv')
                v.set_index('date', inplace=True)
                v.index = v.index.to_period('M')
                v.reset_index(inplace=True)
                ticker_files[dic_map[k]] = v

        return ticker_files


class Paper4:
    def generate_files(self):
        df = pd.read_csv('paper4_features.csv', index_col=[0])
        df['date'] = pd.to_datetime(df['date'])

        stocks = pd.read_csv('paper1/S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]

        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = df[df['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        ticker_files = {}
        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                # cal_export(v).to_csv('../result/consolidated_papers/'+file_name+'.csv')
                v.set_index('date', inplace=True)
                v.index = v.index.to_period('M')
                v.reset_index(inplace=True)
                ticker_files[dic_map[k]] = v

        return ticker_files


class Paper5:
    def generate_files(self):
        with open('paper5_config.json') as config_file:
            data = json.load(config_file)

        start_date = data['start_date']
        end_date = data['end_date']
        wrds_username = data['wrds_username']

        # In[18]:

        df_fred_md = pd.read_csv(
            "https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv")
        #df_fred_md = pd.read_csv("current.csv")
        df_fred_md = df_fred_md.iloc[1:, :]
        df_fred_md["sasdate"] = pd.to_datetime(df_fred_md["sasdate"])
        df_fred_md["sasdate"] = df_fred_md["sasdate"] + MonthEnd(0)
        paper_5_features = df_fred_md[["sasdate", "AAA", "AAAFFM", "ACOGNO", "AMDMNOx", "AMDMUOx", "ANDENOx", "AWHMAN", "AWOTMAN", "BAA", "BAAFFM", "BUSINVx", "BUSLOANS", "CE16OV",
                                       "CES0600000007", "CES0600000008", "CES1021000001", "CES2000000008", "CES3000000008", "CLAIMSx", "CLF16OV", "CMRMTSPLx", "COMPAPFFx", "CONSPI", "CP3Mx", "CPIAPPSL",
                                       "CPIAUCSL", "CPIMEDSL", "CPITRNSL", "CPIULFSL", "CUMFNS", "CUSR0000SA0L2", "CUSR0000SA0L5", "CUSR0000SAC", "CUSR0000SAD", "CUSR0000SAS", "DDURRG3M086SBEA", "DMANEMP",
                                       "DNDGRG3M086SBEA", "DPCERA3M086SBEA", "DSERRG3M086SBEA", "DTCOLNVHFNM", "DTCTHFNM", "EXCAUSx", "EXJPUSx", "EXSZUSx", "EXUSUKx", "FEDFUNDS", "GS1", "GS10", "GS5", "HOUST",
                                       "HOUSTMW", "HOUSTNE", "HOUSTS", "HOUSTW", "HWI", "HWIURATIO", "INDPRO", "INVEST", "IPB51222S", "IPBUSEQ", "IPCONGD", "IPDCONGD", "IPDMAT", "IPFINAL", "IPFPNSS", "IPFUELS",
                                       "IPMANSICS", "IPMAT", "IPNCONGD", "IPNMAT", "ISRATIOx", "M1SL", "M2REAL", "M2SL", "MANEMP", "NDMANEMP", "NONBORRES", "NONREVSL", "OILPRICEx", "PAYEMS", "PCEPI", "PERMIT",
                                       "PERMITMW", "PERMITNE", "PERMITS", "PERMITW", "PPICMM", "REALLN", "RETAILx", "RPI", "S&P 500", "S&P div yield", "S&P PE ratio", "S&P: indust", "SRVPRD", "T10YFFM", "T1YFFM",
                                       "T5YFFM", "TB3MS", "TB3SMFFM", "TB6MS", "TB6SMFFM", "TOTRESNS", "UEMP15OV", "UEMP15T26", "UEMP27OV", "UEMP5TO14", "UEMPLT5", "UEMPMEAN", "UMCSENTx", "UNRATE", "USCONS", "USFIRE",
                                       "USGOOD", "USGOVT", "USTPU", "USTRADE", "USWTRADE", "W875RX1"]]

        # In[22]:

        paper_5_features = paper_5_features.query(
            '20000101 <= sasdate < 20210131')
        paper_5_features = paper_5_features.reset_index(drop=True)

        # In[23]:

        # start_date = '2000-01-01'
        # end_date = '2020-12-31'

        # Fred API key: ab766afb0df13dba8492403a7865f852
        fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')

        df5 = {}
        df5['AMBSL'] = fred.get_series(
            'AMBSL', observation_start=start_date, observation_end=end_date)
        df5['MZMSL'] = fred.get_series(
            'MZMSL', observation_start=start_date, observation_end=end_date)
        df5['PPICRM'] = fred.get_series(
            'PPICRM', observation_start=start_date, observation_end=end_date)
        df5['PPIFCG'] = fred.get_series(
            'PPIFCG', observation_start=start_date, observation_end=end_date)
        df5['PPIFGS'] = fred.get_series(
            'PPIFGS', observation_start=start_date, observation_end=end_date)
        df5['PPIITM'] = fred.get_series(
            'PPIITM', observation_start=start_date, observation_end=end_date)
        df5['TWEXMMTH'] = fred.get_series(
            'TWEXMMTH', observation_start=start_date, observation_end=end_date)
        df5 = pd.DataFrame(df5)

        # In[24]:

        paper_5_features = pd.concat([paper_5_features, df5], axis=1)
        paper_5_features = paper_5_features.dropna(subset=['sasdate'])
        paper_5_features = paper_5_features.reset_index(drop=True)

        # In[25]:
        paper_5_features.set_index('sasdate', inplace=True)
        paper_5_features.index = paper_5_features.index.to_period('M')
        paper_5_features.reset_index(inplace=True)

        # paper_5_features.to_csv('../result/paper5/paper5_features.csv')
        return paper_5_features


class Paper6:

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
        df = self.cleanup(df)

        df.index = df.index.to_period('M')
        return df

    # functions to calculate stock specific features

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

    def generate_ticker_files(self):

        csv_files = {}
        with open('config.json') as config_file:
            data = json.load(config_file)

        start_year = data['start_year']
        end_year = data['end_year']
        wrds_username = data['wrds_username']

        parm = {"start_year": start_year, "end_year": end_year}

        # query data from WRDS
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
        stocks = pd.read_csv('paper6/S&P500 companies list (2000 to 2020).csv')
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

        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                df = self.cal_export(v)
                df['ticker'] = [dic_map[k]] * df.shape[0]
                df.insert(1, 'ticker', df.pop('ticker'))
                df.reset_index(inplace=True)
                csv_files[dic_map[k]] = df

        return csv_files


class Paper7:
    def generate_files(self):
        # Load config file
        with open('paper7_config.json') as config_file:
            data = json.load(config_file)

        start_date = data['start_date']
        end_date = data['end_date']
        start_year = data['start_year']
        end_year = data['end_year']
        wrds_username = data['wrds_username']
        fred = data['fred_api_key']

        parm = {"start_year": start_year, "end_year": end_year}

        parm_sp500 = {"start_year": start_year, "end_year": end_year}

        df_fred_md = pd.read_csv(
            "https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv")
        df_fred_md = df_fred_md.iloc[1:, :]
        df_fred_md["sasdate"] = pd.to_datetime(df_fred_md["sasdate"])
        df_fred_md["sasdate"] = df_fred_md["sasdate"] + MonthEnd(0)
        paper_7_fredmd_features = df_fred_md[["sasdate", "AAA", "AAAFFM", "AMDMNOx", "AMDMUOx", "AWHMAN", "AWOTMAN", "BAA", "BAAFFM", "BUSINVx", "BUSLOANS", "CE16OV",
                                              "CES0600000007", "CES0600000008", "CES1021000001", "CES2000000008", "CES3000000008", "CLAIMSx", "CLF16OV", "CMRMTSPLx", "COMPAPFFx", "CONSPI", "CP3Mx", "CPIAPPSL",
                                              "CPIAUCSL", "CPIMEDSL", "CPITRNSL", "CPIULFSL", "CUMFNS", "CUSR0000SA0L2", "CUSR0000SA0L5", "CUSR0000SAC", "CUSR0000SAD", "CUSR0000SAS", "DDURRG3M086SBEA", "DMANEMP",
                                              "DNDGRG3M086SBEA", "DPCERA3M086SBEA", "DSERRG3M086SBEA", "DTCOLNVHFNM", "DTCTHFNM", "EXCAUSx", "EXJPUSx", "EXSZUSx", "EXUSUKx", "FEDFUNDS", "GS1", "GS10", "GS5", "HOUST",
                                              "HOUSTMW", "HOUSTNE", "HOUSTS", "HOUSTW", "HWI", "HWIURATIO", "INDPRO", "INVEST", "IPB51222S", "IPBUSEQ", "IPCONGD", "IPDCONGD", "IPDMAT", "IPFINAL", "IPFPNSS", "IPFUELS",
                                              "IPMANSICS", "IPMAT", "IPNCONGD", "IPNMAT", "ISRATIOx", "M1SL", "M2REAL", "M2SL", "MANEMP", "NDMANEMP", "NONBORRES", "NONREVSL", "OILPRICEx", "PAYEMS", "PCEPI", "PERMIT",
                                              "PERMITMW", "PERMITNE", "PERMITS", "PERMITW", "PPICMM", "REALLN", "RETAILx", "RPI", "S&P 500", "S&P div yield", "S&P PE ratio", "S&P: indust", "SRVPRD", "T10YFFM", "T1YFFM",
                                              "T5YFFM", "TB3MS", "TB3SMFFM", "TB6MS", "TB6SMFFM", "TOTRESNS", "UEMP15OV", "UEMP15T26", "UEMP27OV", "UEMP5TO14", "UEMPLT5", "UEMPMEAN", "UNRATE", "USCONS", "USFIRE",
                                              "USGOOD", "USGOVT", "USTPU", "USTRADE", "USWTRADE", "W875RX1", "WPSFD49207", "WPSFD49502", "WPSID61", "WPSID62"]]

        paper_7_fredmd_features = paper_7_fredmd_features.query(
            '20000101 <= sasdate < 20210131')
        paper_7_fredmd_features = paper_7_fredmd_features.reset_index(
            drop=True)

        fred = Fred(api_key=fred)

        df7 = {}
        df7['AMBSL'] = fred.get_series(
            'AMBSL', observation_start=start_date, observation_end=end_date)
        df7['MZMSL'] = fred.get_series(
            'MZMSL', observation_start=start_date, observation_end=end_date)
        df7 = pd.DataFrame(df7)
        df7.index = df7.index + MonthEnd(0)
        df7.reset_index(inplace=True)

        paper_7_fredmd_features = pd.merge(
            left=paper_7_fredmd_features, right=df7, left_on='sasdate', right_on='index')
        paper_7_fredmd_features.set_index('sasdate', inplace=True)
        paper_7_fredmd_features.index = paper_7_fredmd_features.index.to_period(
            'M')
        paper_7_fredmd_features.reset_index(inplace=True)

        # Query data from WRDS
        conn = wrds.Connection(wrds_username=wrds_username)

        crsp_msf = conn.raw_sql("""
                              select a.ret, a.retx, a.prc, a.shrout, a.vol, a.date, a.permno
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

        # Query data from WRDS
        conn = wrds.Connection(wrds_username=wrds_username)

        comp_annual = conn.raw_sql("""
                            /*header info*/
                            select c.gvkey, f.cusip, f.datadate, f.fyear, c.cik, substr(c.sic,1,2) as sic2, c.sic, c.naics,

                            /*firm variables*/
                            /*income statement*/
                            f.sale, f.revt, f.cogs, f.xsga, f.dp, f.xrd, f.xad, f.ib, f.ebitda,
                            f.ebit, f.nopi, f.spi, f.pi, f.txp, f.ni, f.txfed, f.txfo, f.txt, f.xint,

                            /*CF statement and others*/
                            f.capx, f.oancf, f.dvt, f.ob, f.gdwlia, f.gdwlip, f.gwo, f.mib, f.oiadp, f.ivao, f.prstkc,

                            /*assets*/
                            f.rect, f.act, f.che, f.ppegt, f.invt, f.at, f.aco, f.intan, f.ao, f.ppent, f.gdwl, f.fatb, f.fatl,

                            /*liabilities*/
                            f.lct, f.dlc, f.dltt, f.lt, f.dm, f.dcvt, f.cshrc, 
                            f.dcpstk, f.pstk, f.ap, f.lco, f.lo, f.drc, f.drlt, f.txdi,

                            /*equity and other*/
                            f.ceq, f.scstkc, f.emp, f.csho, f.seq, f.txditc, f.pstkrv, f.pstkl, f.np, f.txdc, f.dpc, f.ajex,

                            /*market*/
                            abs(f.prcc_f) as prcc_f

                            from comp.funda as f
                            left join comp.company as c
                            on f.gvkey = c.gvkey

                            /*get consolidated, standardized, industrial format statements*/
                            where f.indfmt = 'INDL' 
                            and f.datafmt = 'STD'
                            and f.popsrc = 'D'
                            and f.consol = 'C'
                            and f.datadate >= '01/01/%(start_year)s'
                            and f.datadate <= '12/31/%(end_year)s'
                            """, params=parm)

        comp_crsp_link = conn.raw_sql("""
                          select gvkey, lpermno as permno, linktype, linkprim, 
                          linkdt, linkenddt
                          from crsp.ccmxpf_linktable
                          where substr(linktype,1,1)='L'
                          and (linkprim ='C' or linkprim='P')
                          """)

        conn.close()

        # Merge permno with compustat data

        comp_crsp_link.drop(
            comp_crsp_link[comp_crsp_link.linktype == "LD"].index, inplace=True)

        comp_crsp_link['linkdt'] = pd.to_datetime(comp_crsp_link['linkdt'])
        comp_crsp_link['linkenddt'] = pd.to_datetime(
            comp_crsp_link['linkenddt'])
        comp_crsp_link['linkenddt'] = comp_crsp_link['linkenddt'].fillna(
            pd.to_datetime('today'))

        raw_annual = comp_annual.merge(
            comp_crsp_link[['gvkey', 'permno', 'linkdt', 'linkenddt']], how='left', on=['gvkey'])
        raw_annual['permno'] = raw_annual['permno'].astype('Int64')

        raw_annual['datadate'] = pd.to_datetime(raw_annual['datadate'])
        raw_annual = raw_annual.sort_values(
            ["permno", "datadate"]).drop_duplicates()
        raw_annual.dropna(subset=["fyear", "permno"], inplace=True)

        # Set link date bounds
        raw_annual = raw_annual[(raw_annual['datadate'] >= raw_annual['linkdt']) & (
            raw_annual['datadate'] <= raw_annual['linkenddt'])]
        raw_annual = raw_annual.drop(['linkdt', 'linkenddt'], axis=1)

        # Convert sic and sic2 columns from object to numeric data type
        raw_annual['sic'] = pd.to_numeric(raw_annual['sic'])
        raw_annual['sic2'] = pd.to_numeric(raw_annual['sic2'])

        month = crsp_msf.copy()
        month["date"] = pd.to_datetime(month["date"])
        month["date_std"] = month["date"] + MonthEnd(0)
        month["date_std"] = pd.to_datetime(month["date_std"])
        month["permno"] = month["permno"].astype('int64')

        annual = raw_annual.copy()
        annual["datadate"] = pd.to_datetime(annual["datadate"])
        annual["date_std"] = annual["datadate"] + MonthEnd(0)

        # In[44]:

        combined = pd.merge(month, annual, how="left",
                            on=["permno", "date_std"])
        combined.sort_values(["permno", "date_std"], inplace=True)

        annual_cols = ['gvkey', 'cusip', 'datadate', 'fyear', 'cik', 'sic2', 'sic', 'naics',
                       'sale', 'revt', 'cogs', 'xsga', 'dp', 'xrd', 'xad', 'ib', 'ebitda',
                       'ebit', 'nopi', 'spi', 'pi', 'txp', 'ni', 'txfed', 'txfo', 'txt',
                       'xint', 'capx', 'oancf', 'dvt', 'ob', 'gdwlia', 'gdwlip', 'gwo', 'mib',
                       'oiadp', 'ivao', 'rect', 'act', 'che', 'ppegt', 'invt', 'at', 'aco',
                       'intan', 'ao', 'ppent', 'gdwl', 'fatb', 'fatl', 'lct', 'dlc', 'dltt',
                       'lt', 'dm', 'dcvt', 'cshrc', 'dcpstk', 'pstk', 'ap', 'lco', 'lo', 'drc',
                       'drlt', 'txdi', 'ceq', 'scstkc', 'emp', 'csho', 'seq', 'txditc',
                       'pstkrv', 'pstkl', 'np', 'txdc', 'dpc', 'ajex', 'prcc_f', 'permno',
                       'date_std']

        combined[annual_cols] = combined[annual_cols].ffill(axis=0, limit=12)

        # In[45]:

        # Calculate features

        # 1 A2ME
        combined['market_cap'] = abs(combined['prc'] * combined['shrout'])
        combined['market_cap_l1'] = combined['market_cap'].shift()
        combined['a2me'] = combined['at'] / combined['market_cap_l1']

        # 2 AC

        # 3 AT
        combined['at_l1'] = combined['at'].shift()
        combined['at'] = combined['at_l1']

        # 4 ATO
        combined['operating_assets'] = combined['at'] - \
            combined['che'] - combined['ivao']
        combined['operating_liabilities'] = combined['at'] - combined['dlc'] - \
            combined['dltt'] - combined['mib'] - \
            combined['pstk'] - combined['ceq']
        combined['net_operating_assets'] = combined['operating_assets'] - \
            combined['operating_liabilities']
        combined['net_operating_assets_l1'] = combined['net_operating_assets'].shift()
        combined['sale_l1'] = combined['sale'].shift()
        combined['ato'] = combined['sale_l1'] / \
            combined['net_operating_assets_l1']

        # 5 BEME
        combined['ps'] = combined['pstkrv']
        combined['book_equity'] = combined['seq'] + \
            combined['txditc'] - combined['ps']
        combined['book_equity_l1'] = combined['book_equity'].shift()
        combined['beme'] = combined['book_equity_l1'] / \
            combined['market_cap_l1']

        # 6 Beta - Beta - product of correlations between the excess return of stock i and the market excess return and the ratio of volatilities

        # 7 C
        combined['che_l1'] = combined['che'].shift()
        combined['at_l1'] = combined['at'].shift()
        combined['c'] = combined['che_l1'] / combined['at_l1']

        # 8 CF

        # 9 CF2P

        # 10 CTO
        combined['at_l1'] = combined['at'].shift(1)
        combined['cto'] = combined['sale_l1'] / combined['at_l1']

        # 11 D2A

        # 12 D2P

        # 13 DPI2A
        combined['ppegt_invt'] = combined['ppegt'] + combined['invt']
        combined['ppegt_invt_l1'] = combined['ppegt_invt'].shift(1)
        combined['ppegt_invt_pctchange'] = (
            combined['ppegt_invt'] - combined['ppegt_invt_l1']) / combined['ppegt_invt_l1']
        combined['pi2a_pctchange'] = combined['ppegt_invt_pctchange'] - \
            combined['at_l1']

        # 14 E2P
        combined['ib_l1'] = combined['ib'].shift()
        combined['e2p'] = combined['ib_l1'] / combined['market_cap_l1']

        # 15 FC2Y
        combined['xsga_l1'] = combined['xsga'].shift()
        combined['fc2y'] = combined['xsga_l1'] / combined['sale_l1']

        # 16 idiovol

        # 17 Investment
        combined['at_l12'] = combined['at'].shift(12)
        combined['investment'] = (
            combined['at'] - combined['at_l12']) / combined['at_l12']

        # 18 Lev
        combined['seq_l1'] = combined['seq'].shift()
        combined['dltt_l1'] = combined['dltt'].shift()
        combined['dlc_l1'] = combined['dlc'].shift()
        combined['lev'] = (combined['dltt_l1'] + combined['dlc_l1']) / \
            (combined['dltt_l1'] + combined['dlc_l1'] + combined['seq_l1'])

        # 19 LME
        combined['lme'] = combined['market_cap_l1']

        # 20 LT_Rev

        # 21 LTurnover
        combined['vol_l1'] = combined['vol'].shift()
        combined['shrout_l1'] = combined['shrout'].shift()
        combined['lturnover'] = combined['vol_l1'] / combined['shrout_l1']

        # 22 MktBeta

        # 23 NI

        # 24 NOA
        combined['noa'] = combined['net_operating_assets_l1'] / \
            combined['at_l1']

        # 25 OA
        combined['act_l1'] = combined['act'].shift()
        combined['lct_l1'] = combined['lct'].shift()
        combined['txp_l1'] = combined['txp'].shift()
        combined['ncwc_l1'] = combined['act_l1'] - combined['che_l1'] - \
            combined['lct_l1'] - combined['dlc_l1'] - combined['txp_l1']
        combined['act_l2'] = combined['act'].shift(2)
        combined['che_l2'] = combined['che'].shift(2)
        combined['lct_l2'] = combined['lct'].shift(2)
        combined['dlc_l2'] = combined['dlc'].shift(2)
        combined['txp_l2'] = combined['txp'].shift(2)
        combined['ncwc_l2'] = combined['act_l2'] - combined['che_l2'] - \
            combined['lct_l2'] - combined['dlc_l2'] - combined['txp_l2']
        combined['change_ncwc'] = combined['ncwc_l1'] - combined['ncwc_l2']
        combined['dp_l1'] = combined['dp'].shift()
        combined['oa'] = (combined['change_ncwc'] -
                          combined['dp_l1']) / combined['at_l1']

        # 26 OL
        combined['cogs_l1'] = combined['cogs'].shift()
        combined['xsga_l1'] = combined['xsga'].shift()
        combined['ol'] = (combined['cogs_l1'] +
                          combined['xsga_l1']) / combined['at_l1']

        # 27 OP

        # 28 PCM
        combined['pcm'] = (combined['sale_l1'] -
                           combined['cogs_l1']) / combined['sale_l1']

        # 29 PM
        combined['oiadp_l1'] = combined['oiadp'].shift()
        combined['pm'] = combined['oiadp_l1'] / combined['sale_l1']

        # 30 PROF
        combined['gp_l1'] = combined['sale_l1'] - combined['cogs_l1']
        combined['prof'] = combined['gp_l1'] / combined['book_equity_l1']

        # 31 Q
        combined['txditc_l1'] = combined['txditc'].shift()
        combined['ceq_l1'] = combined['ceq'].shift()
        combined['q'] = (combined['at_l1'] + combined['market_cap_l1'] -
                         combined['ceq_l1'] - combined['txditc_l1']) / combined['at_l1']

        # 32 r 2-1
        combined['ret_l1'] = combined['ret'].shift()
        combined['r_2_1'] = combined['ret_l1']

        # 33 r 12-2
        combined['r_12_2'] = combined['ret'].rolling(10).sum()
        combined['r_12_2'] = combined['r_12_2'].shift(2)

        # 34 r 12-7
        combined['r_12_7'] = combined['ret'].rolling(5).sum()
        combined['r_12_7'] = combined['r_12_7'].shift(7)

        # 35 r 36-13
        combined['r_36_13'] = combined['ret'].rolling(23).sum()
        combined['r_36_13'] = combined['r_36_13'].shift(13)

        # 36 Rel to High
        # combined['prc_l2'] = combined['prc'].shift(2)
        # combined['rel_to_high'] = combined['prc_l1'] / combined['prc_l2']

        # 37 Resid_Var

        # 38 RNA
        combined['noa_l1'] = combined['noa'].shift()
        combined['rna'] = combined['oiadp_l1'] / combined['noa_l1']

        # 39 ROA
        combined['roa'] = combined['ib_l1'] / combined['at_l1']

        # 40 ROE
        combined['roe'] = combined['ib_l1'] / combined['book_equity_l1']

        # 41 S2P
        combined['s2p'] = combined['sale_l1'] / combined['market_cap_l1']

        # 42 SGA2S
        combined['sga2s'] = combined['xsga_l1'] / combined['sale_l1']

        # 43 Spread - average daily bid-ask spread in the previous months

        # 44 ST_Rev
        combined['st_rev'] = combined['ret_l1']

        # 45 SUV - difference between actual volume and predicted volume in the previous month

        # 46 Variance - variance of daily returns in the past two months

        # In[46]:

        paper_7_WRDS_features = combined[['date', 'permno', 'a2me', 'at', 'ato', 'beme', 'c', 'cto', 'pi2a_pctchange',
                                          'e2p', 'fc2y', 'investment', 'lev', 'lme', 'lturnover', 'noa', 'oa', 'ol',
                                          'pcm', 'pm', 'prof', 'q', 'r_2_1', 'r_12_2', 'r_12_7', 'r_36_13', 'rna', 'roa',
                                          'roe', 's2p', 'sga2s', 'st_rev']]
        paper_7_WRDS_features

        # In[47]:

        # Add ticker name
        conn = wrds.Connection(wrds_username=wrds_username)

        crsp_stocknames = conn.raw_sql("""
                            select * from crsp.stocknames 
                            """)

        # Match ticker code to permno
        crsp_stocknames.sort_values(["permno", "nameenddt"], inplace=True)
        crsp_stocknames.drop_duplicates(
            subset=["permno"], keep="last", inplace=True)
        paper_7_WRDS_features = pd.merge(paper_7_WRDS_features, crsp_stocknames[[
                                         "ticker", "permno"]], how="left", on=["permno"])

        # Retrieve S&P500 constituent companies

        sp500 = conn.raw_sql("""
                                select *
                                from crsp.msp500list
                                where ending >='01/01/%(start_year)s'
                                and start <= '12/31/%(end_year)s';
                                """, params=parm_sp500)

        conn.close()

        sp500["permno"] = sp500["permno"].astype("Int64")
        sp500 = sp500["permno"].tolist()

        # In[48]:

        paper_7_WRDS_features = paper_7_WRDS_features[paper_7_WRDS_features["permno"].isin(
            sp500)]
        paper_7_WRDS_features = paper_7_WRDS_features[['date', 'permno', 'ticker', 'a2me', 'at', 'ato', 'beme', 'c', 'cto', 'pi2a_pctchange',
                                                       'e2p', 'fc2y', 'investment', 'lev', 'lme', 'lturnover', 'noa', 'oa', 'ol',
                                                       'pcm', 'pm', 'prof', 'q', 'r_2_1', 'r_12_2', 'r_12_7', 'r_36_13', 'rna', 'roa',
                                                       'roe', 's2p', 'sga2s', 'st_rev']]
        #paper_7_features = pd.merge(paper_7_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])
        paper_7_WRDS_features = paper_7_WRDS_features.reset_index(drop=True)
        # paper_7_WRDS_features

        paper_7_WRDS_features['date'] = pd.to_datetime(
            paper_7_WRDS_features['date'])

        stocks = pd.read_csv('paper7/S&P500 companies list (2000 to 2020).csv')
        permnos = stocks['permno'].values
        tickers = stocks['ticker'].values

        dic_map = {}  # permno: tickers
        for i in range(len(permnos)):
            dic_map[permnos[i]] = tickers[i]

        # create dictionary with key as stock permno and value as dataframe downloaded from yahoo finance
        dic = {}
        for p in permnos:
            dic[p] = paper_7_WRDS_features[paper_7_WRDS_features['permno'] == p]

        # check number of empty dataframes
        delisted = []
        count = 0
        for k, v in dic.items():
            if v.shape[0] == 0:
                delisted.append(k)
            else:
                count += 1

        ticker_files = {}
        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                # cal_export(v).to_csv('../result/consolidated_papers/'+file_name+'.csv')
                v.set_index('date', inplace=True)
                v.index = v.index.to_period('M')
                v.reset_index(inplace=True)
                ticker_files[dic_map[k]] = v

        return ticker_files, paper_7_fredmd_features


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

        df.index = df.index.to_period('M')
        return df

    def close(self, df):
        df.rename(columns={'prc': 'close'}, inplace=True)
        return df.drop(['vol'], axis=1)

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

    def generate_ticker_files(self):
        csv_files = {}
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

        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                # cal_export(v).to_csv('../result/consolidated_papers/'+file_name+'.csv')
                df = self.cal_export(v)
                df.reset_index(inplace=True)
                csv_files[dic_map[k]] = df

        return csv_files

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


class Paper10:
    def generate_common_features(self):
        # define start and end date
        start_date = "2000-01-01"
        end_date = "2020-12-31"

        # Mkt-RF, SMB, HML, RF
        df = pd.read_csv('paper10/F-F_Research_Data_Factors.csv')
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m')
        df = df.set_index('Date')
        df = df.drop(df[df.index < start_date].index)
        df = df.drop(df[df.index > end_date].index)

        # r15-r11
        df1 = pd.read_csv('paper10/6_Portfolios_2x3.csv')
        df1['Date'] = pd.to_datetime(df1['Date'], format='%Y%m')
        df1 = df1.set_index('Date')
        df1 = df1.drop(df1[df1.index < start_date].index)
        df1 = df1.drop(df1[df1.index > end_date].index)
        df['r15-r11'] = df1['SMALL HiBM'] - df1['SMALL LoBM']

        # UMD
        df2 = pd.read_csv('paper10/10_Portfolios_Prior_12_2.csv')
        df2['Date'] = pd.to_datetime(df2['Date'], format='%Y%m')
        df2 = df2.set_index('Date')
        df2 = df2.drop(df2[df2.index < start_date].index)
        df2 = df2.drop(df2[df2.index > end_date].index)
        df['umd'] = df2['Hi PRIOR'] - df2['Lo PRIOR']

        # 44 industries features
        df3 = pd.read_csv('paper10/48_Industry_Portfolios.csv')
        df3['Date'] = pd.to_datetime(df3['Date'], format='%Y%m')
        df3 = df3.set_index('Date')
        df3 = df3.drop(df3[df3.index < start_date].index)
        df3 = df3.drop(df3[df3.index > end_date].index)
        df3 = df3.drop(['Hlth ', 'FabPr', 'Guns ', 'Gold '], axis=1)
        df = df.join(df3, on='Date')

        # 93 size/BM features
        df4 = pd.read_csv('paper10/100_Portfolios_10x10.csv')
        df4['date'] = pd.to_datetime(df4['Date'], format='%Y%m')
        df4 = df4.set_index('date')
        df4 = df4.drop(df4[df4.index < start_date].index)
        df4 = df4.drop(df4[df4.index > end_date].index)
        df['1_2'] = df4['ME1 BM2']
        df['1_4'] = df4['ME1 BM4']
        df['1_5'] = df4['ME1 BM5']
        df['1_6'] = df4['ME1 BM6']
        df['1_7'] = df4['ME1 BM7']
        df['1_8'] = df4['ME1 BM8']
        df['1_9'] = df4['ME1 BM9']
        df['1_high'] = df4['SMALL HiBM']
        df['2_low'] = df4['ME2 BM1']
        df['2_2'] = df4['ME2 BM2']
        df['2_3'] = df4['ME2 BM3']
        df['2_4'] = df4['ME2 BM4']
        df['2_5'] = df4['ME2 BM5']
        df['2_6'] = df4['ME2 BM6']
        df['2_7'] = df4['ME2 BM7']
        df['2_8'] = df4['ME2 BM8']
        df['2_9'] = df4['ME2 BM9']
        df['2_high'] = df4['ME2 BM10']
        df['3_low'] = df4['ME3 BM1']
        df['3_2'] = df4['ME3 BM2']
        df['3_3'] = df4['ME3 BM3']
        df['3_4'] = df4['ME3 BM4']
        df['3_5'] = df4['ME3 BM5']
        df['3_6'] = df4['ME3 BM6']
        df['3_7'] = df4['ME3 BM7']
        df['3_8'] = df4['ME3 BM8']
        df['3_9'] = df4['ME3 BM9']
        df['3_high'] = df4['ME3 BM10']
        df['4_low'] = df4['ME4 BM1']
        df['4_2'] = df4['ME4 BM2']
        df['4_3'] = df4['ME4 BM3']
        df['4_4'] = df4['ME4 BM4']
        df['4_5'] = df4['ME4 BM5']
        df['4_6'] = df4['ME4 BM6']
        df['4_7'] = df4['ME4 BM7']
        df['4_8'] = df4['ME4 BM8']
        df['4_9'] = df4['ME4 BM9']
        df['4_high'] = df4['ME4 BM10']
        df['5_low'] = df4['ME5 BM1']
        df['5_2'] = df4['ME5 BM2']
        df['5_3'] = df4['ME5 BM3']
        df['5_4'] = df4['ME5 BM4']
        df['5_5'] = df4['ME5 BM5']
        df['5_6'] = df4['ME5 BM6']
        df['5_7'] = df4['ME5 BM7']
        df['5_8'] = df4['ME5 BM8']
        df['5_9'] = df4['ME5 BM9']
        df['5_high'] = df4['ME5 BM10']
        df['6_low'] = df4['ME6 BM1']
        df['6_2'] = df4['ME6 BM2']
        df['6_3'] = df4['ME6 BM3']
        df['6_4'] = df4['ME6 BM4']
        df['6_5'] = df4['ME6 BM5']
        df['6_6'] = df4['ME6 BM6']
        df['6_7'] = df4['ME6 BM7']
        df['6_8'] = df4['ME6 BM8']
        df['6_9'] = df4['ME6 BM9']
        df['6_high'] = df4['ME6 BM10']
        df['7_low'] = df4['ME7 BM1']
        df['7_2'] = df4['ME7 BM2']
        df['7_3'] = df4['ME7 BM3']
        df['7_4'] = df4['ME7 BM4']
        df['7_5'] = df4['ME7 BM5']
        df['7_6'] = df4['ME7 BM6']
        df['7_7'] = df4['ME7 BM7']
        df['7_8'] = df4['ME7 BM8']
        df['7_9'] = df4['ME7 BM9']
        df['8_low'] = df4['ME8 BM1']
        df['8_2'] = df4['ME8 BM2']
        df['8_3'] = df4['ME8 BM3']
        df['8_4'] = df4['ME8 BM4']
        df['8_5'] = df4['ME8 BM5']
        df['8_6'] = df4['ME8 BM6']
        df['8_7'] = df4['ME8 BM7']
        df['8_8'] = df4['ME8 BM8']
        df['8_9'] = df4['ME8 BM9']
        df['8_high'] = df4['ME8 BM10']
        df['9_low'] = df4['ME9 BM1']
        df['9_2'] = df4['ME9 BM2']
        df['9_3'] = df4['ME9 BM3']
        df['9_4'] = df4['ME9 BM4']
        df['9_5'] = df4['ME9 BM5']
        df['9_6'] = df4['ME9 BM6']
        df['9_7'] = df4['ME9 BM7']
        df['9_8'] = df4['ME9 BM8']
        df['9_high'] = df4['ME9 BM10']
        df['10_low'] = df4['BIG LoBM']
        df['10_2'] = df4['ME10 BM2']
        df['10_3'] = df4['ME10 BM3']
        df['10_4'] = df4['ME10 BM4']
        df['10_5'] = df4['ME10 BM5']
        df['10_6'] = df4['ME10 BM6']
        df['10_7'] = df4['ME10 BM7']
        df.index = df.index.to_period('M')

        fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')

        yield10y = fred.get_series(
            'GS10', observation_start=start_date, observation_end=end_date)
        yield10y.index.name = 'Date'
        yield10y.rename('yield10y', inplace=True)
        yield10y = yield10y[2::3]
        yield10y.index = yield10y.index.to_period('M')

        yield1y = fred.get_series(
            'GS1', observation_start=start_date, observation_end=end_date)
        yield1y.index.name = 'Date'
        yield1y.rename('yield1y', inplace=True)
        yield1y = yield1y[2::3]
        yield1y.index = yield1y.index.to_period('M')

        trm10y_1y = yield10y - yield1y
        trm10y_1y.index.name = 'Date'
        trm10y_1y.rename('trm10y_1y', inplace=True)

        aaa = fred.get_series('AAA', observation_start=start_date,
                              observation_end=end_date)
        aaa.index.name = 'Date'
        aaa.rename('aaa', inplace=True)
        aaa.index = aaa.index.to_period('M')

        baa = fred.get_series('BAA', observation_start=start_date,
                              observation_end=end_date)
        baa.index.name = 'Date'
        baa.rename('baa', inplace=True)
        baa.index = baa.index.to_period('M')

        # def same name as def function
        DEF = aaa - baa
        DEF.index.name = 'Date'
        DEF.rename('DEF', inplace=True)

        riskfree = fred.get_series(
            'TB3MS', observation_start='1999-01-01', observation_end=end_date)
        rrel = riskfree[2::3]
        for i in range(4, len(rrel)):
            rrel[i] = rrel[i] - sum(rrel[i-4:i]) / 4
        rrel = rrel[4:]
        rrel.index.name = 'Date'
        rrel.rename('rrel', inplace=True)
        rrel.index = rrel.index.to_period('M')

        riskfree = fred.get_series(
            'TB3MS', observation_start=start_date, observation_end=end_date)
        riskfree.index = riskfree.index.to_period('M')
        trm10y_3m = yield10y - riskfree[2::3]
        trm10y_3m.index.name = 'Date'
        trm10y_3m.rename('trm10y_3m', inplace=True)

        p_e = pd.read_excel('paper10/PE.xlsx')
        p_e.set_index('Date', inplace=True)
        p_e.index = p_e.index.to_period('M')

        df = df.join(yield10y)
        df = df.join(yield1y)
        df = df.join(trm10y_1y)
        df = df.join(aaa)
        df = df.join(baa)
        df = df.join(DEF)
        df = df.join(rrel)
        df = df.join(trm10y_3m)
        df = df.join(p_e)

        df.reset_index(inplace=True)
        return df


class Paper11():
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
        with open('config.json') as config_file:
            data = json.load(config_file)

        start_year = data['start_year']
        end_year = data['end_year']
        wrds_username = data['wrds_username']

        parm = {"start_year": start_year, "end_year": end_year}

        # query data from WRDS
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
        stocks = pd.read_csv('paper6/S&P500 companies list (2000 to 2020).csv')
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

    def generate_common_features(self):
        start_date = "2000-01-01"
        end_date = "2020-12-31"

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
        end_date = '31/12/2020'

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

        ixic_f = investpy.search.search_quotes(text='NQc1', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        ixic_f.rename(columns={'Change Pct': 'IXIC_F'}, inplace=True)

        dji_f = investpy.search.search_quotes(text='1YMc1', n_results=1).retrieve_historical_data(
            from_date=start_date, to_date=end_date)
        dji_f.rename(columns={'Change Pct': 'DJI_F'}, inplace=True)

        sp_f = investpy.search.search_quotes(text='ESc1', n_results=1).retrieve_historical_data(
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

if __name__ == "__main__":
    # paper1
    # ticker AET permno should be 46850.0 | some columns are all empty
    paper1_csv_files = Paper1().generate_files()
    # print(paper_1_csv_files['AET'])

    # paper2
    paper2_csv_files = Paper2().generate_files()

    # paper3
    paper3_csv_files = Paper3().generate_files()

    # paper4
    paper4_csv_files = Paper4().generate_files()

    # paper5
    paper5_csv_files = Paper5().generate_files()

    # paper6
    paper6_csv_files = Paper6().generate_ticker_files()

    # paper7
    paper7_csv_files = Paper7().generate_files()

    # paper9
    paper9_csv_files = Paper9().generate_ticker_files()
    paper9_common_features = Paper9().generate_common_features()

    # paper10
    paper10_common_features = Paper10().generate_common_features()

    # paper11
    paper11_csv_files = Paper11().generate_ticker_files()
    paper11_common_features = Paper11().generate_common_features()

    daily_prc = pd.read_csv('sp500_daily_prc.csv')
    daily_prc['date'] = pd.to_datetime(daily_prc['date'])
    daily_prc['year'] = daily_prc['date'].dt.year
    daily_prc['month'] = daily_prc['date'].dt.month
    daily_prc = daily_prc.sort_values(
        ['ticker', 'date']).reset_index(drop=True)
    lvlm_dt_ref = daily_prc.groupby(['year', 'month'])['date'].max()
    lvlm_dt_ref  # purpose is to find the last trading day of the month
    date_dic = {}
    df_date = lvlm_dt_ref.reset_index(level=['year', 'month'])
    df_date.set_index('date', inplace=True)
    df_date['YYMMDD'] = df_date.index.astype(str)
    df_date.index = df_date.index.to_period('M')
    df_date.index = df_date.index.astype(str)

    for i in df_date.index:
        date_dic[i] = df_date['YYMMDD'][i]

    for k in paper6_csv_files.keys():
        # paper6_csv_files[k].reset_index(inplace=True)
        # paper7_csv_files[0][k].reset_index(inplace=True)
        # paper9_csv_files[k].reset_index(inplace=True)
        # paper11_csv_files[k].reset_index(inplace=True)
        # paper6_csv_files[k].to_csv('../result/paper6/'+k+'.csv')
        # paper7_csv_files[0][k].to_csv('../result/paper7/'+k+'.csv')
        # paper9_csv_files[k].to_csv('../result/paper9/'+k+'.csv')
        # paper11_csv_files[k].to_csv('../result/paper11/'+k+'.csv')

        df = paper6_csv_files[k].merge(paper9_csv_files[k], left_on=[
                                       'date', 'permno'], right_on=['date', 'permno'])
        try:
            df = df.merge(paper1_csv_files[k], left_on='date', right_on='DATE')
        except:
            pass

        df = df.merge(paper2_csv_files, left_on='date', right_on='yyyy-mm')
        df = df.merge(paper3_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper4_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper5_csv_files, left_on='date', right_on='sasdate')
        df = df.merge(paper7_csv_files[0][k], left_on='date', right_on='date')
        # common features
        df = df.merge(paper7_csv_files[1], left_on='date', right_on='sasdate')
        df = df.merge(paper9_common_features, left_on='date', right_on='date')
        df = df.merge(paper10_common_features, left_on='date', right_on='Date')
        df = df.merge(paper11_csv_files[k], left_on='date', right_on='date')
        df = df.merge(paper11_common_features, left_on='date', right_on='date')

        #df['date'] = df['date'].astype(str)
        temp = df.copy()
        temp['date'] = temp['date'].astype(str)
        l = []
        for i in temp.index:
            l.append(df_date[df_date.index == temp['date'][i]]['YYMMDD'][0])
        df['date'] = l
        df = df.sort_values(by='date')
        df.columns = df.columns.str.lower()
        if 'ticker' not in df.columns:
            df['ticker'] = k
        dates = df['date'].values
        if dates.shape[1] > 1:
            dates = df['date'].values[:, 0]

        drop_list = []
        count = 0
        for col in df.columns:
            # ex: 'permno_x' , check if 'permno' in col_name
            if 'permno' in col and len(col) != len('permno'):
                if 'permno' not in df.columns:
                    try:
                        df['permno'] = df[col].values
                    except:
                        df['permno'] = df[col].values[:, 0]
                else:
                    #df = df.drop(columns=[col])
                    drop_list.append(col)
            # ex: 'ticker_x' , check if 'ticker' in col_name
            if 'ticker' in col and len(col) != len('ticker'):
                #df = df.drop(columns=[col])
                drop_list.append(col)
            if 'date' in col:
                    drop_list.append(col)
        drop_list = drop_list + ['yyyy-mm', 'index']
        df = df.drop(columns=drop_list)
        df['date'] = dates
        df.insert(0, 'date', df.pop('date'))
        df.insert(1, 'permno', df.pop('permno'))
        df.insert(2, 'ticker', df.pop('ticker'))
        df.to_csv('../result/consolidated_papers/'+k+'.csv')
'''
