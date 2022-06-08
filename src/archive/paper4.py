#!/usr/bin/env python
# coding: utf-8

# In[14]:


#WRDS login details are as follows:
    
#user: muhdnoor
#password: WRDSaccess_135@


# In[15]:


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:80% !important; }</style>"))


# In[16]:


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


# In[17]:


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


# In[18]:


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


# In[19]:


month = crsp_msf.copy()
month["date"] = pd.to_datetime(month["date"])
month["date_std"] = month["date"] + MonthEnd(0)
month["date_std"] = pd.to_datetime(month["date_std"])
month["permno"] = month["permno"].astype('int64')

annual = raw_annual.copy()
annual["datadate"] = pd.to_datetime(annual["datadate"])
annual["date_std"] = annual["datadate"] + MonthEnd(0)


# In[20]:


combined = pd.merge(month, annual, how="left", on=["permno", "date_std"])
combined.sort_values(["permno", "date_std"], inplace=True)


# In[21]:


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


# In[22]:


# Calculate features

# Anomaly characteristics

#1 Size
combined['market_equity'] = abs(combined['prc'] * combined['shrout']) 
combined['size'] = combined['market_equity'].shift()

#2 Value (annual)
combined['ps'] = combined['pstkrv']
combined['book_equity'] = combined['seq'] + combined['txditc'] - combined['ps']
combined['book_equity_l1'] = combined['book_equity'].shift()
combined['market_equity_l1'] = combined['market_equity'].shift()
combined['value'] = combined['book_equity_l1'] / combined['market_equity_l1']

#3 Gross Profitability
combined['sale_l1'] = combined['sale'].shift()
combined['cogs_l1'] = combined['cogs'].shift()
combined['gp_l1'] = combined['sale_l1'] - combined['cogs_l1']
combined['prof'] = combined['gp_l1'] / combined['book_equity_l1']

#4 Value-Profitability
combined['valprof'] = combined['value'] + combined['prof']

#5 Piotroski’s F-score

#6 Debt Issuance
# if combined['debtiss'] >= 0:
#     combined['debtiss'] = 1
# else
#     combined['debtiss'] = 0

#7 Share Repurchases
# if combined['prstkc'] >= 0:
#     combined['repurch'] = 1
# else
#     combined['repurch'] = 0

#8 Share Issuance (annual)
combined['shrout_l12'] = combined['shrout'].shift(12)
combined['nissa'] = combined['shrout_l12'] / combined['shrout']

#9 Accruals

#10 Asset Growth
combined['at_l12'] = combined['at'].shift(12)
combined['growth'] = combined['at_l12'] / combined['at']

#11 Asset Turnover
combined['at_l1'] = combined['at'].shift()
combined['aturnover'] = combined['sale_l1'] / combined['at_l1']

#12 Gross Margins
combined['gmargins'] = combined['gp_l1'] / combined['sale_l1']

#13 Dividend Yield
combined['monthly_dividends'] = abs(combined['ret'] - combined['retx'])
combined['annual_dividends'] = combined['monthly_dividends'].rolling(12).sum()                                                       
combined['monthly_dividends_l1'] = combined['monthly_dividends'].shift()
combined['annual_dividends_l1'] = combined['annual_dividends'].shift()
combined['divp'] = combined['annual_dividends_l1'] / combined['market_equity_l1']

#14 Earnings/Price
combined['ib_l1'] = combined['ib'].shift()
combined['ep'] = combined['ib_l1'] / combined['market_equity_l1']

#15 Cash Flow / Market Value of Equity
combined['dp_l1'] = combined['dp'].shift()
combined['cfp'] = (combined['ib_l1'] + combined['dp_l1']) / combined['market_equity_l1'] 

#16 Net Operating Assets
combined['che_l1'] = combined['che'].shift()
combined['dlc_l1'] = combined['dlc'].shift()
combined['dltt_l1'] = combined['dltt'].shift()
combined['mib_l1'] = combined['mib'].shift()
combined['pstk_l1'] = combined['pstk'].shift()
combined['ceq_l1'] = combined['ceq'].shift()
combined['noa'] = (combined['at_l1'] - combined['che_l1']) - (combined['at_l1'] - combined['dlc_l1'] - combined['dltt_l1'] - combined['mib_l1'] - combined['pstk_l1'] - combined['ceq_l1'])

#17 Investment
combined['ppegt_l1'] = combined['ppegt'].shift()
combined['ppegt_pctchange'] = (combined['ppegt'] - combined['ppegt_l1']) / combined['ppegt_l1']
combined['invt_l1'] = combined['invt'].shift()
combined['invt_pctchange'] = (combined['invt'] - combined['invt_l1']) / combined['invt_l1']  
combined['inv'] = combined['ppegt_pctchange'] + combined['invt_pctchange'] - combined['at_l12']

#18 Investment-to-Capital
combined['capx_l1'] = combined['capx'].shift()
combined['invcap'] = combined['capx_l1'] / combined['ppegt_l1']

#19 Investment Growth
combined['capx_l12'] = combined['capx'].shift(12)
combined['inv_growth'] = (combined['capx'] - combined['capx_l12']) / combined['capx_l12']

#20 Sales Growth
combined['sale_l12'] = combined['sale'].shift(12)
combined['sgrowth'] = (combined['sale'] - combined['sale_l12']) / combined['sale_l12']

#21 Leverage
combined['lev'] = combined['at_l1'] / combined['market_equity_l1']

#22 Return on Assets (annual)
combined['roaa'] = combined['ib_l1'] / combined['at_l1']

#23 Return on Equity (annual)
combined['roea'] = combined['ib_l1'] / combined['book_equity_l1']

#24 Sales-to-Price
combined['sp'] = combined['sale_l1'] / combined['market_equity_l1']

#25 Growth in LTNOA
# gltnoa
# NOA = (RECT + INVT + ACO + PPENT
# + INTAN + AO - AP - LCO - LO) / AT, GRNOA = NOA - NOA−12, ACC=((RECT -
# RECT−12) + (INVT - INVT−12) + (ACO - ACO−12) - (AP - AP−12) - (LCO - LCO−12) - DP)
# / ((AT + AT−12) / 2)

#26 Momentum (6m)
combined['ret_l1'] = combined['ret'].shift()
combined['mom'] = combined['ret'].rolling(6).sum() - combined['ret_l1']

#27 Industry Momentum
# indmom
# indmom = rank(P6l =1 rind t−l).

#28 Value-Momentum
#combined['valmom'] = (combined['book_equity_l1'] / combined['market_equity_l1']) + combined['mom']

#29 Value-Momentum-Profitability
#combined['valmom'] = (combined['book_equity_l1'] / combined['market_equity_l1']) + combined['prof'] + combined['mom']

#30 Short Interest
# combined['shrout_l1'] = combined['shrout'].shift()
# combined['shortint'] = combined['valmom'] / combined['shrout_l1']

#31 Momentum (1 year) 
combined['mom12'] = combined['ret'].rolling(12).sum() - combined['ret_l1']

#32 Momentum-Reversal
combined['ret_19'] = combined['ret'].shift(19)
combined['ret_18'] = combined['ret'].shift(18)
combined['ret_17'] = combined['ret'].shift(17)
combined['ret_16'] = combined['ret'].shift(16)
combined['ret_15'] = combined['ret'].shift(15)
combined['ret_14'] = combined['ret'].shift(14)
combined['momrev'] = combined['ret_19'] + combined['ret_18'] + combined['ret_17'] + combined['ret_16'] + combined['ret_15'] + combined['ret_14']

#33 Long-term Reversals
combined['lrrev'] = combined['ret'].rolling(47).sum()
combined['lrrev'] = combined['lrrev'].shift(13)

#34 Value (monthly)
combined['beq'] = combined['book_equity'] / combined['market_equity']
combined['beq_l3'] = combined['beq'].shift(3)
combined['valuem'] = combined['beq_l3'] / combined['market_equity_l1']

#35 Share Issuance (monthly)
combined['shrout_l1'] = combined['shrout'].shift()
combined['shrout_l13'] = combined['shrout'].shift(13)
combined['nissm'] = combined['shrout_l13'] / combined['shrout_l1']

#36 PEAD (SUE)
    
#37 Return on Book Equity
combined['roe'] = combined['ib_l1'] / combined['beq_l3']

#38 Return on Market Equity
combined['market_equity_l4'] = combined['market_equity'].shift(4)
combined['rome'] = combined['ib_l1'] / combined['market_equity_l4']

#39 Return on Assets 
combined['at_l3'] = combined['at'].shift(3)
combined['roa'] = combined['ib_l1'] / combined['at_l3']

#40 Short-term Reversal 
combined['strev'] = combined['ret_l1']

#41 Idiosyncratic Volatility 

#42 Beta Arbitrage 
    
#43 Seasonality
# season
# season = P5l = 1 rt−l×12

#44 Industry Relative Reversals
# indrrev
# indrrev = r−1 − rind −1

#45 Industry Relative Reversals (Low Volatility)
# indrrevlv
# indrrevlv = r−1 − rind −1 if vol < NYSE

#46 Industry Momentum-Reversal
# indmomrev
# indmomrev = rank(industry momentum) + rank(industry relative-reversals low-vol)

#47 Composite Issuance
combined['market_equity_l13'] = combined['market_equity'].shift(13)
combined['market_equity_l60'] = combined['market_equity'].shift(60)
combined['ciss'] = np.log(combined['market_equity_l13'] / combined['market_equity_l60']) - combined['lrrev']
                                                                                         
#48 Price
combined['prc'] = np.log(combined['market_equity_l1'] / combined['shrout_l1'])        

#49 Firm Age
# age
# age = log(1 + number of months since listing)
            
#50 Share Volume
combined['shrout_3monthmovingaverage'] = combined.shrout.rolling(3).mean()
combined['shvol'] = combined['shrout_3monthmovingaverage'] / combined['shrout_l1']


# In[23]:


paper_4_features = combined[['date','permno','size','value','prof','valprof','nissa','growth','aturnover','gmargins',
                             'divp','ep','cfp','noa','inv','invcap','inv_growth','sgrowth','lev','roaa','roea',
                            'sp','mom','mom12','momrev','lrrev','valuem','nissm','roe','rome','roa','strev','ciss',
                            'prc','shvol']]
paper_4_features


# In[24]:


# Add ticker name
conn = wrds.Connection(wrds_username=wrds_username)

crsp_stocknames = conn.raw_sql("""
                    select * from crsp.stocknames 
                    """)

# Match ticker code to permno
crsp_stocknames.sort_values(["permno", "nameenddt"], inplace=True)
crsp_stocknames.drop_duplicates(subset=["permno"], keep="last", inplace=True)
paper_4_features = pd.merge(paper_4_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])

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


# In[25]:


# Save to csv
paper_4_features = paper_4_features[paper_4_features["permno"].isin(sp500)]
paper_4_features = paper_4_features[['date','permno','ticker','size','value','prof','valprof','nissa','growth','aturnover','gmargins',
                             'divp','ep','cfp','noa','inv','invcap','inv_growth','sgrowth','lev','roaa','roea',
                            'sp','mom','mom12','momrev','lrrev','valuem','nissm','roe','rome','roa','strev','ciss',
                            'prc','shvol']]
#paper_4_features = pd.merge(paper_4_features, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])
paper_4_features = paper_4_features.reset_index(drop=True)
paper_4_features


# In[26]:


paper_4_features.to_csv('../result/paper4/paper4_features.csv')


# In[27]:


#17 features yet to be implemented

# Piotroski’s F-score
# Debt Issuance
# Share Repurchases
# Accruals
# Growth in LTNOA
# Industry Momentum
# Value-Momentum
# Value-Momentum-Profitability
# Short Interest
# PEAD (SUE)
# Idiosyncratic Volatility
# Beta Arbitrage
# Seasonality
# Industry Relative Reversals
# Industry Relative Reversals (Low Volatility)
# Industry Momentum-Reversal
# Firm Age


# In[28]:


# Calculate features

# WRDS financial ratios

1. P/E (Diluted, Excl. EI) (pe_exi) – Valuation. Price-to-Earnings, excl. Extraordinary
Items (diluted).

2. P/E (Diluted, Incl. EI) (pe_inc) – Valuation. Price-to-Earnings, incl. Extraordinary
Items (diluted).

3. Price/Sales (ps) – Valuation. Multiple of Market Value of Equity to Sales.

4. Price/Cash flow (pcf ) – Valuation. Multiple of Market Value of Equity to Net Cash Flow
from Operating Activities.

5. Enterprise Value Multiple (evm) – Valuation. Multiple of Enterprise Value to EBITDA.

6. Book/Market (bm) – Valuation. Book Value of Equity as a fraction of Market Value of
Equity.

7. Shiller’s Cyclically Adjusted P/E Ratio (capei) – Valuation. Multiple of Market Value
of Equity to 5-year moving average of Net Income.

8. Dividend Payout Ratio (dpr) – Valuation. Dividends as a fraction of Income Before Extra.
Items.

9. Net Profit Margin (npm) – Profitability. Net Income as a fraction of Sales.

10. Operating Profit Margin Before Depreciation (opmbd) – Profitability. Operating Income
Before Depreciation as a fraction of Sales.

11. Operating Profit Margin After Depreciation (opmad) – Profitability. Operating Income
After Depreciation as a fraction of Sales.

12. Gross Profit Margin (gpm) – Profitability. Gross Profit as a fraction of Sales.

13. Pre-tax Profit Margin (ptpm) – Profitability. Pretax Income as a fraction of Sales.

14. Cash Flow Margin (cfm) – Financial Soundness. Income before Extraordinary Items and
Depreciation as a fraction of Sales.

15. Return on Assets (roa) – Profitability. Operating Income Before Depreciation as a fraction
of average Total Assets based on most recent two periods.

16. Return on Equity (roe) – Profitability. Net Income as a fraction of average Book Equity
based on most recent two periods, where Book Equity is defined as the sum of Total Parent
Stockholders’ Equity and Deferred Taxes and Investment Tax Credit.

17. Return on Capital Employed (roce) – Profitability. Earnings Before Interest and Taxes
as a fraction of average Capital Employed based on most recent two periods, where Capital
Employed is the sum of Debt in Long-term and Current Liabilities and Common/Ordinary
Equity.

18. After-tax Return on Average Common Equity (aftret_eq) – Profitability. Net Income
as a fraction of average of Common Equity based on most recent two periods.

19. After-tax Return on Invested Capital (aftret_invcapx) – Profitability. Net Income plus
Interest Expenses as a fraction of Invested Capital.

20. After-tax Return on Total Stockholders’ Equity (aftret_equity) – Profitability. Net
Income as a fraction of average of Total Shareholders’ Equity based on most recent two
periods.

21. Pre-tax return on Net Operating Assets (pretret_noa) – Profitability. Operating Income
After Depreciation as a fraction of average Net Operating Assets (NOA) based on most
recent two periods, where NOA is defined as the sum of Property Plant and Equipment and
Current Assets minus Current Liabilities.

22. Pre-tax Return on Total Earning Assets (pretret_earnat) – Profitability. Operating
Income After Depreciation as a fraction of average Total Earnings Assets (TEA) based on
most recent two periods, where TEA is defined as the sum of Property Plant and Equipment
and Current Assets.

23. Common Equity/Invested Capital (equity_invcap) – Capitalization. Common Equity
as a fraction of Invested Capital.

24. Long-term Debt/Invested Capital (debt_invcap) – Capitalization. Long-term Debt as
a fraction of Invested Capital.

25. Total Debt/Invested Capital (totdebt_invcap) – Capitalization. Total Debt (Long-term
and Current) as a fraction of Invested Capital.

26. Interest/Average Long-term Debt (int_debt) – Financial Soundness. Interest as a fraction
of average Long-term debt based on most recent two periods.

27. Interest/Average Total Debt (int_totdebt) – Financial Soundness. Interest as a fraction
of average Total Debt based on most recent two periods.

28. Cash Balance/Total Liabilities (cash_lt) – Financial Soundness. Cash Balance as a
fraction of Total Liabilities.

29. Inventory/Current Assets (invt_act) – Financial Soundness. Inventories as a fraction of
Current Assets.

30. Receivables/Current Assets (rect_act) – Financial Soundness. Accounts Receivables as
a fraction of Current Assets.

31. Total Debt/Total Assets (debt_at) – Solvency. Total Liabilities as a fraction of Total
Assets.

32. Short-Term Debt/Total Debt (short_debt) – Financial Soundness. Short-term Debt as
a fraction of Total Debt.

33. Current Liabilities/Total Liabilities (curr_debt) – Financial Soundness. Current Liabilities
as a fraction of Total Liabilities.

34. Long-term Debt/Total Liabilities (lt_debt) – Financial Soundness. Long-term Debt as
a fraction of Total Liabilities.

35. Free Cash Flow/Operating Cash Flow (fcf_ocf ) – Financial Soundness. Free Cash
Flow as a fraction of Operating Cash Flow, where Free Cash Flow is defined as the difference
between Operating Cash Flow and Capital Expenditures.

36. Advertising Expenses/Sales (adv_sale) – Other. Advertising Expenses as a fraction of
Sales.

37. Profit Before Depreciation/Current Liabilities (profit_lct) – Financial Soundness. Operating
Income before D&A as a fraction of Current Liabilities.

38. Total Debt/EBITDA (debt_ebitda) – Financial Soundness. Gross Debt as a fraction of
EBITDA.

39. Operating CF/Current Liabilities (ocf_lct) – Financial Soundness. Operating Cash
Flow as a fraction of Current Liabilities.

40. Total Liabilities/Total Tangible Assets (lt_ppent) – Financial Soundness. Total Liabilities
to Total Tangible Assets.

41. Long-term Debt/Book Equity (dltt_be) – Financial Soundness. Long-term Debt to Book
Equity.

42. Total Debt/Total Assets (debt_assets) – Solvency. Total Debt as a fraction of Total
Assets.

43. Total Debt/Capital (debt_capital) – Solvency. Total Debt as a fraction of Total Capital,
where Total Debt is defined as the sum of Accounts Payable and Total Debt in Current and
Long-term Liabilities, and Total Capital is defined as the sum of Total Debt and Total Equity
(common and preferred).

44. Total Debt/Equity (de_ratio) – Solvency. Total Liabilities to Shareholders’ Equity (common
and preferred).

45. After-tax Interest Coverage (intcov) – Solvency. Multiple of After-tax Income to Interest
and Related Expenses.

46. Cash Ratio (cash_ratio) – Liquidity. Cash and Short-term Investments as a fraction of
Current Liabilities.

47. Quick Ratio (Acid Test) (quick_ratio) – Liquidity. Quick Ratio: Current Assets net of
Inventories as a fraction of Current Liabilities.

48. Current Ratio (curr_ratio) – Liquidity. Current Assets as a fraction of Current Liabilities.

49. Capitalization Ratio (capital_ratio) – Capitalization. Total Long-term Debt as a fraction
of the sum of Total Long-term Debt, Common/Ordinary Equity and Preferred Stock.

50. Cash Flow/Total Debt (cash_debt) – Financial Soundness. Operating Cash Flow as a
fraction of Total Debt.

51. Inventory Turnover (inv_turn) – Efficiency. COGS as a fraction of the average Inventories
based on the most recent two periods.

52. Asset Turnover (at_turn) – Efficiency. Sales as a fraction of the average Total Assets based
on the most recent two periods.

53. Receivables Turnover (rect_turn) – Efficiency. Sales as a fraction of the average of Accounts
Receivables based on the most recent two periods.

54. Payables Turnover (pay_turn) – Efficiency. COGS and change in Inventories as a fraction
of the average of Accounts Payable based on the most recent two periods.

55. Sales/Invested Capital (sale_invcap) – Efficiency. Sales per dollar of Invested Capital.

56. Sales/Stockholders Equity (sale_equity) – Efficiency. Sales per dollar of total Stockholders’
Equity.

57. Sales/Working Capital (sale_nwc) – Efficiency. Sales per dollar of Working Capital,
defined as difference between Current Assets and Current Liabilities.

58. Research and Development/Sales (RD_SALE) – Other. R&D expenses as a fraction of
Sales.

59. Accruals/Average Assets (Accrual) – Other. Accruals as a fraction of average Total Assets
based on most recent two periods.

60. Gross Profit/Total Assets (GProf ) – Profitability. Gross Profitability as a fraction of
Total Assets.

61. Book Equity (be) – Other. Firm size as measured by total book equity.

62. Cash Conversion Cycle (Days) (cash_conversion) – Liquidity. Inventories per daily
COGS plus Account Receivables per daily Sales minus Account Payables per daily COGS.

63. Effective Tax Rate (efftax) – Profitability. Income Tax as a fraction of Pretax Income.

64. Interest Coverage Ratio (intcov_ratio) – Solvency. Multiple of Earnings Before Interest
and Taxes to Interest and Related Expenses.

65. Labor Expenses/Sales (staff_sale) – Other. Labor Expenses as a fraction of Sales.

66. Dividend Yield (divyield) – Valuation. Indicated Dividend Rate as a fraction of Price.

67. Price/Book (ptb) – Valuation. Multiple of Market Value of Equity to Book Value of Equity.

68. Trailing P/E to Growth (PEG) ratio (PEG_trailing) – Valuation. Price-to-Earnings,
excl. Extraordinary Items (diluted) to 3-Year past EPS Growth.

69-80. Return in Month t − i (ret_lagi) – Other. Past one-month returns in months t − i for
i = {1, ..., 12}.


# In[ ]:




