# Feature generation and pre-processing of paper 4
# Execution complete in 4 mins
# ============================================================================

import wrds
import pandas as pd
import numpy as np
import json
import os
import sys
import time
import warnings
from pandas.tseries.offsets import MonthEnd
warnings.filterwarnings("ignore")

start = time.time()
class Paper4:
    def __init__(self):
        with open('config.json') as config_file:
            self.data = json.load(config_file)

        self.wrds_start_date = self.data['wrds_start_date']
        self.wrds_end_date = self.data['wrds_end_date']
        self.wrds_username = self.data['wrds_username']
        self.parm = {"start_date": self.wrds_start_date, "end_date": self.wrds_end_date}
        self.features_df = pd.DataFrame()
        self.parm = {"start_date": self.wrds_start_date, "end_date": self.wrds_end_date}

    def generate(self):
        # In[3]:


        # Query data from WRDS
        conn = wrds.Connection(wrds_username=self.wrds_username)

        crsp_msf = conn.raw_sql("""
                              select a.ret, a.retx, a.prc, a.shrout, a.vol, a.date, a.permno
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

        # Query data from WRDS
        conn = wrds.Connection(wrds_username=self.wrds_username)

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
                            and f.datadate >= %(start_date)s
                            and f.datadate <= %(end_date)s
                            """, params=self.parm)

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


        # In[4]:


        month = crsp_msf.copy()
        month["date"] = pd.to_datetime(month["date"])
        month["date_std"] = month["date"] + MonthEnd(0)
        month["date_std"] = pd.to_datetime(month["date_std"])
        month["permno"] = month["permno"].astype('int64')

        annual = raw_annual.copy()
        annual["datadate"] = pd.to_datetime(annual["datadate"])
        annual["date_std"] = annual["datadate"] + MonthEnd(0)

        combined = pd.merge(month, annual, how="left", on=["permno", "date_std"])
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


        # In[5]:


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

        paper_4_features = combined[['date','permno','size','value','prof','valprof','nissa','growth','aturnover','gmargins',
                                     'divp','ep','cfp','noa','inv','invcap','inv_growth','sgrowth','lev','roaa','roea',
                                    'sp','mom','mom12','momrev','lrrev','valuem','nissm','roe','rome','roa','strev','ciss',
                                    'prc','shvol']]


        # In[7]:


        # Add ticker name
        conn = wrds.Connection(wrds_username=self.wrds_username)

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
                                where ending >= %(start_date)s
        						and start <= %(end_date)s;
                                """, params=self.parm)

        conn.close()

        sp500["permno"] = sp500["permno"].astype("Int64")
        sp500 = sp500["permno"].tolist()


        # In[8]:


        # Save to csv
        paper_4_features = paper_4_features[paper_4_features["permno"].isin(sp500)]

        paper_4_features = paper_4_features[['date','permno','ticker','size','value','prof','valprof','nissa','growth','aturnover','gmargins',
                                     'divp','ep','cfp','noa','inv','invcap','inv_growth','sgrowth','lev','roaa','roea',
                                    'sp','mom','mom12','momrev','lrrev','valuem','nissm','roe','rome','roa','strev','ciss',
                                    'prc','shvol']]

        paper_4_features = paper_4_features.reset_index(drop=True)

        
        df = paper_4_features
        df['date'] = pd.to_datetime(df['date'])

        stocks = pd.read_csv('S&P500 companies list (2000 to 2020).csv')
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
        paper4_final = pd.DataFrame()
        # calculate features and export as csv file
        for k, v in dic.items():
            if v.shape[0] > 1:
                #v.set_index('date', inplace=True)
                #v.index = v.index.to_period('M')
                #v.reset_index(inplace=True)
                if count == 1:
                    paper4_final = v
                else:
                    try:
                        paper4_final = pd.concat([paper4_final, v], ignore_index=True)
                    except:
                        # delisted companies
                        pass
                v['date'] = pd.to_datetime(v['date'])
                ticker_files[dic_map[k]] = v
                print("\rPaper4 completed {}/{}.".format(count, 1665), end='')
                sys.stdout.flush()
                count += 1
        paper4_final.to_csv('../result/paper4/paper4_features.csv')    
        return ticker_files

if __name__ == "__main__":
    Paper4().generate()
    end = time.time()
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))


