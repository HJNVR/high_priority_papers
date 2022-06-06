#!/usr/bin/env python
# coding: utf-8

# In[56]:


#WRDS login details are as follows:
    
#user: muhdnoor
#password: WRDSaccess_135@


# In[57]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:80% !important; }</style>"))


# In[58]:


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


# In[59]:


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


# In[60]:


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


# In[61]:


month = crsp_msf.copy()
month["date"] = pd.to_datetime(month["date"])
month["date_std"] = month["date"] + MonthEnd(0)
month["date_std"] = pd.to_datetime(month["date_std"])
month["permno"] = month["permno"].astype('int64')

annual = raw_annual.copy()
annual["datadate"] = pd.to_datetime(annual["datadate"])
annual["date_std"] = annual["datadate"] + MonthEnd(0)


# In[62]:


combined = pd.merge(month, annual, how="left", on=["permno", "date_std"])
combined.sort_values(["permno", "date_std"], inplace=True)


# In[63]:


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


# In[64]:


# Calculate features

#1 A2ME
combined['market_cap'] = abs(combined['prc'] * combined['shrout']) 
combined['market_cap_l1'] = combined['market_cap'].shift()
combined['a2me'] = combined['at'] / combined['market_cap_l1']

#2 AOA: Absolute value of operation accruals

#3 ATO
combined['operating_assets'] = combined['at'] - combined['che'] - combined['ivao']
combined['operating_liabilities'] = combined['at'] - combined['dlc'] - combined['dltt'] - combined['mib'] - combined['pstk'] - combined['ceq']
combined['net_operating_assets'] = combined['operating_assets'] - combined['operating_liabilities']
combined['net_operating_assets_l1'] = combined['net_operating_assets'].shift()
combined['sale_l1'] = combined['sale'].shift()
combined['ato'] = combined['sale_l1'] / combined['net_operating_assets_l1']

#4 BEME
combined['ps'] = combined['pstkrv']
combined['book_equity'] = combined['seq'] + combined['txditc'] - combined['ps']
combined['book_equity_l1'] = combined['book_equity'].shift()
combined['beme'] = combined['book_equity_l1'] / combined['market_cap_l1']

#5 BEME_adj
#combined['beme_adj'] = combined['BEME'] - combined['BEME_ind_adj']

#6 Beta - product of correlations between the excess return of stock i and the market excess return and the ratio of volatilities

#7 Beta daily - sum of the regression coefficients of daily excess returns on the market excess return and one lag of the market excess return

#8 C
combined['che_l1'] = combined['che'].shift()
combined['at_l1'] = combined['at'].shift()
combined['c'] = combined['che_l1'] / combined['at_l1']

#9 C2D
combined['ib_l1'] = combined['ib'].shift()
combined['dp_l1'] = combined['dp'].shift()
combined['lt_l1'] = combined['lt'].shift()
combined['c2d'] = (combined['ib_l1'] + combined['dp_l1']) / combined['lt_l1']

#10 CTO
combined['at_l1'] = combined['at'].shift(1)
combined['cto'] = combined['sale_l1'] / combined['at_l1']

#11 Debt2P
combined['dltt_l1'] = combined['dltt'].shift()
combined['dlc_l1'] = combined['dlc'].shift()
combined['debt2p'] = (combined['dltt_l1'] + combined['dlc_l1']) / combined['market_cap_l1']

#12 change in ceq
combined['ceq_l1'] = combined['ceq'].shift(1)
combined['ceq_pctchange'] = (combined['ceq'] - combined['ceq_l1']) / combined['ceq_l1']

#13 change in (change in Gm - change in Sales)
combined['gm'] = combined['sale'] - combined['cogs']
combined['gm_l1'] = combined['gm'].shift(1)
combined['gm_pctchange'] = (combined['gm'] - combined['gm_l1']) / combined['gm_l1']
combined['sale_pctchange'] = (combined['sale'] - combined['sale_l1']) / combined['sale_l1']
combined['gm_sales_pctchange'] = combined['gm_pctchange'] - combined['sale_pctchange']

#14 change in So
combined['split_adj_shares'] = combined['csho'] * combined['ajex']
combined['split_adj_shares_l1'] = combined['split_adj_shares'].shift(1)
combined['so_pctchange'] = (combined['split_adj_shares'] - combined['split_adj_shares_l1']) / combined['split_adj_shares_l1']

#15 change in shrout
combined['shrout_l1'] = combined['shrout'].shift(1)
combined['shrout_pctchange'] = (combined['shrout'] - combined['shrout_l1']) / combined['shrout_l1']

#16 change in PI2A
combined['ppegt_invt'] = combined['ppegt'] + combined['invt']
combined['ppegt_invt_l1'] = combined['ppegt_invt'].shift()
combined['ppegt_invt_pctchange'] = (combined['ppegt_invt'] - combined['ppegt_invt_l1']) / combined['ppegt_invt_l1']                                     
combined['pi2a_pctchange'] = combined['ppegt_invt_pctchange'] - combined['at_l1']

#17 DTO - ratio of daily volume (VOL) to share
                                                     
#18 E2P
combined['ib_l1'] = combined['ib'].shift()                    
combined['e2p'] = combined['ib_l1'] / combined['market_cap_l1']
                                                      
#19 EPS                                          
combined['eps'] = combined['ib_l1'] / combined['shrout_l1'] 
                                                      
#20 Free CF
combined['wcapch'] = combined['act'] - combined['lct']
combined['wcapch_l1'] = combined['wcapch'].shift()
combined['wcapch_l2'] = combined['wcapch'].shift(2)
combined['change_wcapch'] = combined['wcapch_l1'] - combined['wcapch_l2']
combined['capx_l1'] = combined['capx'].shift()
combined['free_cf'] = (combined['dp_l1'] - combined['change_wcapch'] - combined['capx_l1']) / combined['book_equity_l1']                                                      

#21 Idio vol - the standard deviation of the residuals from a regression of excess returns 

#22 Investment
combined['at_l12'] = combined['at'].shift(12)                                                      
combined['investment'] = (combined['at'] - combined['at_l12']) / combined['at_l12']                                                                                                   

#23 IPM
combined['pi_l1'] = combined['pi'].shift()                                                      
combined['ipm'] = combined['pi_l1'] / combined['sale_l1']                                                      

#24 IVC
combined['invt_l12'] = combined['invt'].shift(12)
combined['invt_l24'] = combined['invt'].shift(24)
combined['at_l12'] = combined['at'].shift(12)
combined['at_l24'] = combined['at'].shift(24)
combined['ivc'] = (combined['invt_l12'] - combined['invt_l24']) / ((combined['at_l12'] + combined['at_l24'])) / 2                                                       
                                               
#25 Lev
combined['seq_l1'] = combined['seq'].shift()
combined['lev'] = (combined['dltt_l1'] + combined['dlc_l1']) / (combined['dltt_l1'] + combined['dlc_l1'] + combined['seq_l1'])                                                      

#26 LDP
combined['monthly_dividends'] = abs(combined['ret'] - combined['retx'])
combined['annual_dividends'] = combined['monthly_dividends'].rolling(12).sum()                                                       
combined['monthly_dividends_l1'] = combined['monthly_dividends'].shift()
combined['annual_dividends_l1'] = combined['annual_dividends'].shift()
combined['prc_l1'] = combined['prc'].shift()
combined['LDP'] = combined['annual_dividends_l1'] / combined['prc_l1']
                                                      
#27 LME                                        
combined['lme'] = combined['market_cap_l1']
                                                      
#28 LME adj
#combined['lme_adj'] = combined['lme'] - combined['lme_ind_adj']
                                                                                            
#29 LTurnover
combined['vol_l1'] = combined['vol'].shift(1)
combined['lturnover'] = combined['vol_l1'] / combined['shrout_l1']

#30 NOA
combined['noa'] = combined['net_operating_assets_l1'] / combined['at_l1']
                                                     
#31 NOP
combined['monthly_dividends_l1'] = combined['monthly_dividends'].shift()
combined['pstk_l1'] = combined['pstk'].shift()
combined['scstkc_l1'] = combined['scstkc'].shift()
combined['nop'] = (combined['monthly_dividends_l1'] + combined['pstk_l1'] - combined['scstkc_l1']) / combined['market_cap_l1']
                            
#32 O2P
combined['prstkc_l1'] = combined['prstkc'].shift()
combined['pstkrv_l1'] = combined['pstkrv'].shift()
combined['o2p'] = (combined['monthly_dividends_l1'] + combined['prstkc_l1'] - combined['pstkrv_l1']) / combined['market_cap_l1']
                                                      
#33 OA
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
combined['oa'] = (combined['change_ncwc'] - combined['dp_l1']) / combined['at_l1']
                                                      
#34 OL
combined['cogs_l1'] = combined['cogs'].shift() 
combined['xsga_l1'] = combined['xsga'].shift()                                                     
combined['ol'] = (combined['cogs_l1'] + combined['xsga_l1']) / combined['at_l1']                                                      

#35 PCM
combined['pcm'] = (combined['sale_l1'] - combined['cogs_l1']) / combined['sale_l1']
                                                      
#36 PM
combined['oiadp_l1'] = combined['oiadp'].shift()                                                      
combined['pm'] = combined['oiadp_l1'] / combined['sale_l1']
                                                      
#37 PM adj
#combined['pm_adj'] = combined['pm'] - combined['pm_ind_adj']                                                      

#38 Prof
combined['gp_l1'] = combined['sale_l1'] - combined['cogs_l1']
combined['prof'] = combined['gp_l1'] / combined['book_equity_l1']
                                                      
#39 Q
combined['txditc_l1'] = combined['txditc'].shift()                                                     
combined['q'] = (combined['at_l1'] + combined['market_cap_l1'] - combined['ceq_l1'] - combined['txditc_l1']) / combined['at_l1']                                                      

#40 Rel to High
# combined['prc_l2'] = combined['prc'].shift(2)                                                      
# combined['rel_to_high'] = combined['prc_l1'] / combined['prc_l2']                                                      
                                                      
#41 Ret max - maximum daily return in the previous month

#42 RNA
combined['noa_l1'] = combined['noa'].shift()
combined['rna'] = combined['oiadp_l1'] / combined['noa_l1']
                                                      
#43 ROA
combined['roa'] = combined['ib_l1'] / combined['at_l1']                                                      

#44 ROC
combined['roc'] = (combined['market_cap_l1'] + combined['dltt_l1'] - combined['at_l1']) / combined['che_l1']

#45 ROE                                          
combined['roe'] = combined['ib_l1'] / combined['book_equity_l1']
                                                      
#46 ROIC
combined['ebit_l1'] = combined['ebit'].shift()
combined['nopi_l1'] = combined['nopi'].shift()
combined['roic'] = (combined['ebit_l1'] - combined['nopi_l1']) / (combined['ceq_l1'] + combined['lt_l1'] + combined['che_l1'])    

#47 r 12-2
combined['r_12_2'] = combined['ret'].rolling(10).sum()
combined['r_12_2'] = combined['r_12_2'].shift(2)
                                                      
#48 r 12-7
combined['r_12_7'] = combined['ret'].rolling(5).sum()
combined['r_12_7'] = combined['r_12_7'].shift(7)
                                                      
#49 r 6-2
combined['r_6_2'] = combined['ret'].rolling(4).sum()
combined['r_6_2'] = combined['r_6_2'].shift(2)

#50 r 2-1
combined['ret_l1']= combined['ret'].shift()
combined['r_2_1'] = combined['ret_l1']
                                                      
#51 r 36-13
combined['r_36_13'] = combined['ret'].rolling(23).sum()
combined['r_36_13'] = combined['r_36_13'].shift(13)
                                                      
#52 S2C
combined['s2c'] = combined['sale_l1'] / combined['che_l1']

#53 S2P
combined['s2p'] = combined['sale_l1'] / combined['market_cap_l1']

#54 Sales_g
combined['sale_l12'] = combined['sale'].shift(12)                                                      
combined['sales_g'] = (combined['sale'] - combined['sale_l12']) / combined['sale_l12']

#55 SAT
combined['sat'] = combined['sale_l1'] / combined['at_l1']                                                     

#56 SAT adj
#combined['sat_adj'] = combined['sat'] - combined['sat_ind_adj']                                                      

#57 SGA2S
combined['sga2s'] = combined['xsga_l1'] / combined['sale_l1']
                                                      
#58 Spread - average daily bid-ask spread in the previous months

#59 Std turnover - standard deviation of the residuals from a regression of daily turnover

#60 Std volume - standard deviation of the residuals from a regression of daily volume

#61 SUV - difference between actual volume and predicted volume in the previous month

#62 Tan
combined['rect_l1'] = combined['rect'].shift()
combined['invt_l1'] = combined['invt'].shift()
combined['ppent_l1'] = combined['ppent'].shift()
combined['tan'] = (0.715 * combined['rect_l1'] + 0.547 * combined['invt_l1'] + 0.535 * combined['ppent_l1'] + combined['che_l1']) / combined['at_l1']

#63 Total vol - standard deviation of the residuals from a regression of excess returns on a constant


# In[65]:


paper_3_features = combined[['date','permno','a2me','ato','beme','c','c2d','cto','debt2p','ceq_pctchange','gm_sales_pctchange','so_pctchange',
                            'shrout_pctchange','pi2a_pctchange','e2p','eps','free_cf','investment','ipm','ivc','lev','LDP',
                            'lme','lturnover','noa','nop','o2p','oa','ol','pcm','pm','prof','q','rna','roa','roc','roe',
                            'roic','r_12_2','r_12_7','r_6_2','r_2_1','r_36_13','s2c','s2p','sales_g','sat','sga2s','tan']]
paper_3_features


# In[66]:


# Add ticker name
conn = wrds.Connection(wrds_username=wrds_username)

crsp_stocknames = conn.raw_sql("""
                    select * from crsp.stocknames 
                    """)

# Match ticker code to permno
crsp_stocknames.sort_values(["permno", "nameenddt"], inplace=True)
crsp_stocknames.drop_duplicates(subset=["permno"], keep="last", inplace=True)
paper_3_features = pd.merge(paper_3_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])

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


# In[68]:


# Save to csv
paper_3_features = paper_3_features[paper_3_features["permno"].isin(sp500)]
paper_3_features = paper_3_features[['date','permno','ticker','a2me','ato','beme','c','c2d','cto','debt2p','ceq_pctchange','gm_sales_pctchange','so_pctchange',
                            'shrout_pctchange','pi2a_pctchange','e2p','eps','free_cf','investment','ipm','ivc','lev','LDP',
                            'lme','lturnover','noa','nop','o2p','oa','ol','pcm','pm','prof','q','rna','roa','roc','roe',
                            'roic','r_12_2','r_12_7','r_6_2','r_2_1','r_36_13','s2c','s2p','sales_g','sat','sga2s','tan']]
#paper_3_features = pd.merge(paper_3_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])
paper_3_features = paper_3_features.reset_index(drop=True)
paper_3_features


# In[69]:


paper_3_features.to_csv('../result/paper3/paper3_features.csv')


# In[70]:


# 16 yet to be implemented

# aoa
# beme_adj
# beta
# beta_daily
# dto
# idiovol
# lme_adj
# pm_adj
# rel_to_high
# ret_max
# sat_adj
# spread
# std_turnover
# std_volume
# suv
# total_vol
