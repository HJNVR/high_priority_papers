#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:90% !important; }</style>"))


# In[2]:


import pandas as pd
from fredapi import Fred
from pandas.tseries.offsets import *


# In[3]:


df_fred_md = pd.read_csv("https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv")
df_fred_md = df_fred_md.iloc[1:,:]
df_fred_md["sasdate"] = pd.to_datetime(df_fred_md["sasdate"])
df_fred_md["sasdate"] = df_fred_md["sasdate"] + MonthEnd(0)
paper_5_features = df_fred_md[["sasdate","AAA","AAAFFM","ACOGNO","AMDMNOx","AMDMUOx","ANDENOx","AWHMAN","AWOTMAN","BAA","BAAFFM","BUSINVx","BUSLOANS","CE16OV",
              "CES0600000007","CES0600000008","CES1021000001","CES2000000008","CES3000000008","CLAIMSx","CLF16OV","CMRMTSPLx","COMPAPFFx","CONSPI","CP3Mx","CPIAPPSL",
              "CPIAUCSL","CPIMEDSL","CPITRNSL","CPIULFSL","CUMFNS","CUSR0000SA0L2","CUSR0000SA0L5","CUSR0000SAC","CUSR0000SAD","CUSR0000SAS","DDURRG3M086SBEA","DMANEMP",
              "DNDGRG3M086SBEA","DPCERA3M086SBEA","DSERRG3M086SBEA","DTCOLNVHFNM","DTCTHFNM","EXCAUSx","EXJPUSx","EXSZUSx","EXUSUKx","FEDFUNDS","GS1","GS10","GS5","HOUST",
              "HOUSTMW","HOUSTNE","HOUSTS","HOUSTW","HWI","HWIURATIO","INDPRO","INVEST","IPB51222S","IPBUSEQ","IPCONGD","IPDCONGD","IPDMAT","IPFINAL","IPFPNSS","IPFUELS",
              "IPMANSICS","IPMAT","IPNCONGD","IPNMAT","ISRATIOx","M1SL","M2REAL","M2SL","MANEMP","NDMANEMP","NONBORRES","NONREVSL","OILPRICEx","PAYEMS","PCEPI","PERMIT",
              "PERMITMW","PERMITNE","PERMITS","PERMITW","PPICMM","REALLN","RETAILx","RPI","S&P 500","S&P div yield","S&P PE ratio","S&P: indust","SRVPRD","T10YFFM","T1YFFM",
              "T5YFFM","TB3MS","TB3SMFFM","TB6MS","TB6SMFFM","TOTRESNS","UEMP15OV","UEMP15T26","UEMP27OV","UEMP5TO14","UEMPLT5","UEMPMEAN","UMCSENTx","UNRATE","USCONS","USFIRE",
              "USGOOD","USGOVT","USTPU","USTRADE","USWTRADE","W875RX1"]]


# In[4]:


paper_5_features = paper_5_features.query('20000101 <= sasdate < 20210131')
paper_5_features = paper_5_features.reset_index(drop=True)


# In[5]:


start_date = '2000-01-01'
end_date = '2020-12-31'

# Fred API key: ab766afb0df13dba8492403a7865f852
fred = Fred(api_key = 'ab766afb0df13dba8492403a7865f852')

df5 = {}
df5['AMBSL'] = fred.get_series('AMBSL', observation_start=start_date, observation_end=end_date)
df5['MZMSL'] = fred.get_series('MZMSL', observation_start=start_date, observation_end=end_date)
df5['PPICRM'] = fred.get_series('PPICRM', observation_start=start_date, observation_end=end_date)
df5['PPIFCG'] = fred.get_series('PPIFCG', observation_start=start_date, observation_end=end_date)
df5['PPIFGS'] = fred.get_series('PPIFGS', observation_start=start_date, observation_end=end_date)
df5['PPIITM'] = fred.get_series('PPIITM', observation_start=start_date, observation_end=end_date)
df5['TWEXMMTH'] = fred.get_series('TWEXMMTH', observation_start=start_date, observation_end=end_date)
df5 = pd.DataFrame(df5)


# In[6]:


paper_5_features = pd.concat([paper_5_features, df5], axis=1)
paper_5_features = paper_5_features.dropna(subset=['sasdate'])
paper_5_features = paper_5_features.reset_index(drop=True)
paper_5_features


# In[7]:


paper_5_features.to_csv('../result/paper5/paper5_features.csv')


# In[8]:


# 7 features not implemented yet for Paper 5

# NAPM
# NAPMEI
# NAPMII
# NAPMNOI
# NAPMPI
# NAPMPRI
# NAPMSDI

