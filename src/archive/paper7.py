#!/usr/bin/env python
# coding: utf-8

# In[54]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:90% !important; }</style>"))


# In[55]:


from fredapi import Fred
import wrds
import pandas as pd
import numpy as np
import json
import os
from pandas.tseries.offsets import *

# Load config file
with open('config.json') as config_file:
    data = json.load(config_file)

start_year = data['start_year']
end_year = data['end_year']
wrds_username = data['wrds_username']

parm = {"start_year": start_year, "end_year": end_year}

parm_sp500 = {"start_year": start_year, "end_year": end_year}


# In[56]:


df_fred_md = pd.read_csv("https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv")
df_fred_md = df_fred_md.iloc[1:,:]
df_fred_md["sasdate"] = pd.to_datetime(df_fred_md["sasdate"])
df_fred_md["sasdate"] = df_fred_md["sasdate"] + MonthEnd(0)
paper_7_features = df_fred_md[["sasdate","AAA","AAAFFM","AMDMNOx","AMDMUOx","AWHMAN","AWOTMAN","BAA","BAAFFM","BUSINVx","BUSLOANS","CE16OV",
              "CES0600000007","CES0600000008","CES1021000001","CES2000000008","CES3000000008","CLAIMSx","CLF16OV","CMRMTSPLx","COMPAPFFx","CONSPI","CP3Mx","CPIAPPSL",
              "CPIAUCSL","CPIMEDSL","CPITRNSL","CPIULFSL","CUMFNS","CUSR0000SA0L2","CUSR0000SA0L5","CUSR0000SAC","CUSR0000SAD","CUSR0000SAS","DDURRG3M086SBEA","DMANEMP",
              "DNDGRG3M086SBEA","DPCERA3M086SBEA","DSERRG3M086SBEA","DTCOLNVHFNM","DTCTHFNM","EXCAUSx","EXJPUSx","EXSZUSx","EXUSUKx","FEDFUNDS","GS1","GS10","GS5","HOUST",
              "HOUSTMW","HOUSTNE","HOUSTS","HOUSTW","HWI","HWIURATIO","INDPRO","INVEST","IPB51222S","IPBUSEQ","IPCONGD","IPDCONGD","IPDMAT","IPFINAL","IPFPNSS","IPFUELS",
              "IPMANSICS","IPMAT","IPNCONGD","IPNMAT","ISRATIOx","M1SL","M2REAL","M2SL","MANEMP","NDMANEMP","NONBORRES","NONREVSL","OILPRICEx","PAYEMS","PCEPI","PERMIT",
              "PERMITMW","PERMITNE","PERMITS","PERMITW","PPICMM","REALLN","RETAILx","RPI","S&P 500","S&P div yield","S&P PE ratio","S&P: indust","SRVPRD","T10YFFM","T1YFFM",
              "T5YFFM","TB3MS","TB3SMFFM","TB6MS","TB6SMFFM","TOTRESNS","UEMP15OV","UEMP15T26","UEMP27OV","UEMP5TO14","UEMPLT5","UEMPMEAN","UNRATE","USCONS","USFIRE",
              "USGOOD","USGOVT","USTPU","USTRADE","USWTRADE","W875RX1","WPSFD49207","WPSFD49502","WPSID61","WPSID62"]]


# In[57]:


paper_7_features = paper_7_features.query('20000101 <= sasdate < 20210131')
paper_7_features = paper_7_features.reset_index(drop=True)


# In[58]:


start_date = '2000-01-01'
end_date = '2020-12-31'

# Fred API key: ab766afb0df13dba8492403a7865f852
fred = Fred(api_key = 'ab766afb0df13dba8492403a7865f852')

df7 = {}
df7['AMBSL'] = fred.get_series('AMBSL', observation_start=start_date, observation_end=end_date)
df7['MZMSL'] = fred.get_series('MZMSL', observation_start=start_date, observation_end=end_date)
df7 = pd.DataFrame(df7)
df7


# In[59]:


paper_7_features = pd.concat([paper_7_features, df7], axis=1)
paper_7_features = paper_7_features.dropna(subset=['sasdate'])
paper_7_features = paper_7_features.reset_index(drop=True)


# In[ ]:


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


# In[ ]:


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

comp_crsp_link.drop(comp_crsp_link[comp_crsp_link.linktype == "LD"].index, inplace=True)

comp_crsp_link['linkdt'] = pd.to_datetime(comp_crsp_link['linkdt'])
comp_crsp_link['linkenddt'] = pd.to_datetime(comp_crsp_link['linkenddt'])
comp_crsp_link['linkenddt'] = comp_crsp_link['linkenddt'].fillna(pd.to_datetime('today'))

raw_annual = comp_annual.merge(comp_crsp_link[['gvkey', 'permno', 'linkdt', 'linkenddt']], how='left', on=['gvkey'])
raw_annual['permno'] = raw_annual['permno'].astype('Int64')

raw_annual['datadate'] = pd.to_datetime(raw_annual['datadate'])
raw_annual = raw_annual.sort_values(["permno", "datadate"]).drop_duplicates()
raw_annual.dropna(subset=["fyear", "permno"], inplace=True)

# Set link date bounds
raw_annual = raw_annual[(raw_annual['datadate'] >= raw_annual['linkdt']) & (raw_annual['datadate'] <= raw_annual['linkenddt'])]
raw_annual = raw_annual.drop(['linkdt', 'linkenddt'], axis=1)

# Convert sic and sic2 columns from object to numeric data type
raw_annual['sic'] = pd.to_numeric(raw_annual['sic'])
raw_annual['sic2'] = pd.to_numeric(raw_annual['sic2'])


# In[ ]:


month = crsp_msf.copy()
month["date"] = pd.to_datetime(month["date"])
month["date_std"] = month["date"] + MonthEnd(0)
month["date_std"] = pd.to_datetime(month["date_std"])
month["permno"] = month["permno"].astype('int64')

annual = raw_annual.copy()
annual["datadate"] = pd.to_datetime(annual["datadate"])
annual["date_std"] = annual["datadate"] + MonthEnd(0)


# In[ ]:


combined = pd.merge(month, annual, how="left", on=["permno", "date_std"])
combined.sort_values(["permno", "date_std"], inplace=True)


# In[ ]:


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


# In[ ]:


combined


# In[ ]:


# Calculate features

#1 A2ME
combined['market_cap'] = abs(combined['prc'] * combined['shrout']) 
combined['market_cap_l1'] = combined['market_cap'].shift()
combined['a2me'] = combined['at'] / combined['market_cap_l1']

#2 AC

#3 AT
combined['at_l1'] = combined['at'].shift()
combined['at'] = combined['at_l1']

#4 ATO
combined['operating_assets'] = combined['at'] - combined['che'] - combined['ivao']
combined['operating_liabilities'] = combined['at'] - combined['dlc'] - combined['dltt'] - combined['mib'] - combined['pstk'] - combined['ceq']
combined['net_operating_assets'] = combined['operating_assets'] - combined['operating_liabilities']
combined['net_operating_assets_l1'] = combined['net_operating_assets'].shift()
combined['sale_l1'] = combined['sale'].shift()
combined['ato'] = combined['sale_l1'] / combined['net_operating_assets_l1']

#5 BEME
combined['ps'] = combined['pstkrv']
combined['book_equity'] = combined['seq'] + combined['txditc'] - combined['ps']
combined['book_equity_l1'] = combined['book_equity'].shift()
combined['beme'] = combined['book_equity_l1'] / combined['market_cap_l1']

#6 Beta - Beta - product of correlations between the excess return of stock i and the market excess return and the ratio of volatilities

#7 C
combined['che_l1'] = combined['che'].shift()
combined['at_l1'] = combined['at'].shift()
combined['c'] = combined['che_l1'] / combined['at_l1']

#8 CF

#9 CF2P

#10 CTO
combined['at_l1'] = combined['at'].shift(1)
combined['cto'] = combined['sale_l1'] / combined['at_l1']

#11 D2A

#12 D2P

#13 DPI2A
combined['ppegt_invt'] = combined['ppegt'] + combined['invt']
combined['ppegt_invt_l1'] = combined['ppegt_invt'].shift(1)
combined['ppegt_invt_pctchange'] = (combined['ppegt_invt'] - combined['ppegt_invt_l1']) / combined['ppegt_invt_l1']                                     
combined['pi2a_pctchange'] = combined['ppegt_invt_pctchange'] - combined['at_l1']

#14 E2P
combined['ib_l1'] = combined['ib'].shift()                    
combined['e2p'] = combined['ib_l1'] / combined['market_cap_l1']

#15 FC2Y
combined['xsga_l1'] = combined['xsga'].shift()
combined['fc2y'] = combined['xsga_l1'] / combined['sale_l1']

#16 idiovol

#17 Investment
combined['at_l12'] = combined['at'].shift(12)                                                      
combined['investment'] = (combined['at'] - combined['at_l12']) / combined['at_l12']   

#18 Lev
combined['seq_l1'] = combined['seq'].shift()
combined['dltt_l1'] = combined['dltt'].shift()
combined['dlc_l1'] = combined['dlc'].shift()
combined['lev'] = (combined['dltt_l1'] + combined['dlc_l1']) / (combined['dltt_l1'] + combined['dlc_l1'] + combined['seq_l1'])

#19 LME
combined['lme'] = combined['market_cap_l1']

#20 LT_Rev

#21 LTurnover
combined['vol_l1'] = combined['vol'].shift()
combined['shrout_l1'] = combined['shrout'].shift()
combined['lturnover'] = combined['vol_l1'] / combined['shrout_l1']

#22 MktBeta

#23 NI

#24 NOA
combined['noa'] = combined['net_operating_assets_l1'] / combined['at_l1']

#25 OA
combined['act_l1'] = combined['act'].shift()
combined['lct_l1'] = combined['lct'].shift()
combined['txp_l1'] = combined['txp'].shift()
combined['ncwc_l1'] = combined['act_l1'] - combined['che_l1'] - combined['lct_l1'] - combined['dlc_l1'] - combined['txp_l1']
combined['act_l2'] = combined['act'].shift(2)
combined['che_l2'] = combined['che'].shift(2)                                         
combined['lct_l2'] = combined['lct'].shift(2)
combined['dlc_l2'] = combined['dlc'].shift(2)                                                    
combined['txp_l2'] = combined['txp'].shift(2)                                       
combined['ncwc_l2'] = combined['act_l2'] - combined['che_l2'] - combined['lct_l2'] - combined['dlc_l2'] - combined['txp_l2']                                                   
combined['change_ncwc'] = combined['ncwc_l1'] - combined['ncwc_l2']      
combined['dp_l1'] = combined['dp'].shift()
combined['oa'] = (combined['change_ncwc'] - combined['dp_l1']) / combined['at_l1']

#26 OL
combined['cogs_l1'] = combined['cogs'].shift() 
combined['xsga_l1'] = combined['xsga'].shift()                                                     
combined['ol'] = (combined['cogs_l1'] + combined['xsga_l1']) / combined['at_l1']

#27 OP

#28 PCM
combined['pcm'] = (combined['sale_l1'] - combined['cogs_l1']) / combined['sale_l1']

#29 PM
combined['oiadp_l1'] = combined['oiadp'].shift()                                                      
combined['pm'] = combined['oiadp_l1'] / combined['sale_l1']

#30 PROF
combined['gp_l1'] = combined['sale_l1'] - combined['cogs_l1']
combined['prof'] = combined['gp_l1'] / combined['book_equity_l1']

#31 Q
combined['txditc_l1'] = combined['txditc'].shift()       
combined['ceq_l1'] = combined['ceq'].shift()  
combined['q'] = (combined['at_l1'] + combined['market_cap_l1'] - combined['ceq_l1'] - combined['txditc_l1']) / combined['at_l1']

#32 r 2-1
combined['ret_l1']= combined['ret'].shift()
combined['r_2_1'] = combined['ret_l1']

#33 r 12-2
combined['r_12_2'] = combined['ret'].rolling(10).sum()
combined['r_12_2'] = combined['r_12_2'].shift(2)

#34 r 12-7
combined['r_12_7'] = combined['ret'].rolling(5).sum()
combined['r_12_7'] = combined['r_12_7'].shift(7)

#35 r 36-13
combined['r_36_13'] = combined['ret'].rolling(23).sum()
combined['r_36_13'] = combined['r_36_13'].shift(13)

#36 Rel to High
# combined['prc_l2'] = combined['prc'].shift(2)                                                      
# combined['rel_to_high'] = combined['prc_l1'] / combined['prc_l2']

#37 Resid_Var

#38 RNA
combined['noa_l1'] = combined['noa'].shift()
combined['rna'] = combined['oiadp_l1'] / combined['noa_l1']

#39 ROA
combined['roa'] = combined['ib_l1'] / combined['at_l1']

#40 ROE                                          
combined['roe'] = combined['ib_l1'] / combined['book_equity_l1']

#41 S2P
combined['s2p'] = combined['sale_l1'] / combined['market_cap_l1']

#42 SGA2S
combined['sga2s'] = combined['xsga_l1'] / combined['sale_l1']

#43 Spread - average daily bid-ask spread in the previous months

#44 ST_Rev
combined['st_rev'] = combined['ret_l1']

#45 SUV - difference between actual volume and predicted volume in the previous month

#46 Variance - variance of daily returns in the past two months


# In[ ]:


paper_7_features = combined[['date','permno','a2me','at','ato','beme','c','cto','pi2a_pctchange',
                            'e2p','fc2y','investment','lev','lme','lturnover','noa','oa','ol',
                            'pcm','pm','prof','q','r_2_1','r_12_2','r_12_7','r_36_13','rna','roa',
                            'roe','s2p','sga2s','st_rev']]
paper_7_features


# In[ ]:


# Add ticker name
conn = wrds.Connection(wrds_username=wrds_username)

crsp_stocknames = conn.raw_sql("""
                    select * from crsp.stocknames 
                    """)

# Match ticker code to permno
crsp_stocknames.sort_values(["permno", "nameenddt"], inplace=True)
crsp_stocknames.drop_duplicates(subset=["permno"], keep="last", inplace=True)
paper_3_features = pd.merge(paper_7_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])

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


# In[ ]:


# Save to csv
paper_7_features = paper_7_features[paper_7_features["permno"].isin(sp500)]
paper_7_features = paper_7_features[['date','permno','ticker','a2me','at','ato','beme','c','cto','pi2a_pctchange',
                            'e2p','fc2y','investment','lev','lme','lturnover','noa','oa','ol',
                            'pcm','pm','prof','q','r_2_1','r_12_2','r_12_7','r_36_13','rna','roa',
                            'roe','s2p','sga2s','st_rev']]
#paper_7_features = pd.merge(paper_7_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])
paper_7_features = paper_7_features.reset_index(drop=True)
paper_7_features


# In[ ]:


paper_7_features.to_csv('../result/paper7/paper7_features.csv')


# In[ ]:


# 18 features yet to be implemented

# VXOCLSx
# AC - Accrual
# Beta - CAPM Beta
# CF - Free Cash Flow to Book Value
# CF2P - Cashflow to price
# D2A - Capital intensity
# D2P - Dividend yield
# IdioVol - Idiosyncratic volatility
# LT_Rev - Long-term reversal
# MktBeta - Market Beta
# NI - Net Shares Issues
# OP - Operating accruals
# Rel2High - Closeness to past year high
# Resid_Var - Residual Variance
# Spread - Bid-ask spread
# ST_Rev - Short-term reversal
# SUV - Standard unexplained volume
# Variance - Variance


# In[ ]:




