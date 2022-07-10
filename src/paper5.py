# Feature generation and pre-processing of paper 5
# common_features only 
# Execution complete in 7 seconds
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
from pandas.tseries.offsets import MonthEnd
warnings.filterwarnings("ignore")

start = time.time()
class Paper5:
    def __init__(self):
        with open('config.json') as config_file:
            self.data = json.load(config_file)

        self.wrds_start_date = self.data['wrds_start_date']
        self.wrds_end_date = self.data['wrds_end_date']
        self.wrds_username = self.data['wrds_username']
        self.features_df = pd.DataFrame()
        self.fred = Fred(api_key=self.data['fred_api_key'])
        self.fred_start_date = self.data['fred_start_date']
        self.fred_end_date = self.data['fred_end_date']
        self.parm = {"start_date": self.wrds_start_date, "end_date": self.wrds_end_date}

    def generate_common_features(self):

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
            '20000101 <= sasdate')
        paper_5_features = paper_5_features.reset_index(drop=True)
        
        
        # In[23]:

        # start_date = '2000-01-01'
        # end_date = '2020-12-31'

        # Fred API key: ab766afb0df13dba8492403a7865f852
        #fred = Fred(api_key='ab766afb0df13dba8492403a7865f852')

        df5 = {}
        df5['AMBSL'] = self.fred.get_series(
            'AMBSL', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['MZMSL'] = self.fred.get_series(
            'MZMSL', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['PPICRM'] = self.fred.get_series(
            'PPICRM', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['PPIFCG'] = self.fred.get_series(
            'PPIFCG', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['PPIFGS'] = self.fred.get_series(
            'PPIFGS', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['PPIITM'] = self.fred.get_series(
            'PPIITM', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
        df5['TWEXMMTH'] = self.fred.get_series(
            'TWEXMMTH', observation_start=self.fred_start_date, observation_end=self.fred_end_date)
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

    def generate(self):
        common_features = self.generate_common_features()
        common_features.columns = common_features.columns.str.lower()

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


        common_features = common_features.rename(columns={'sasdate': 'date'})
        common_features['date'] = common_features['date'].astype(str)
        dates = []
        for i in common_features.index:
            dates.append(df_date[df_date.index == common_features['date'][i]]['YYMMDD'][0])
        common_features['date'] = dates
        common_features['date'] = pd.to_datetime(common_features['date'])
        common_features.to_csv('../result/paper5/paper5_features.csv')
        print('Paper5 completed 1/1')
        return common_features
        
if __name__ == "__main__":
    Paper5().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nExecution complete in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
