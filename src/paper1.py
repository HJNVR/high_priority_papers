#!/usr/bin/env python
# coding: utf-8

# In[1]:


#WRDS login details are as follows:
    
#user: muhdnoor
#password: WRDSaccess_135@


# In[2]:


import wrds
import json
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning) 

# Load config file
with open('config.json') as config_file:
    data = json.load(config_file)

wrds_username = data['wrds_username']

db = wrds.Connection(wrds_username=wrds_username)
db.create_pgpass_file()
db.close()


# In[3]:


#annual feature generation

import wrds
import pandas as pd
import numpy as np
import json
import os
import datetime
from pandas.tseries.offsets import *

# Load config file
with open('config.json') as config_file:
    data = json.load(config_file)

start_year = data['start_year']
end_year = data['end_year']
wrds_username = data['wrds_username']

parm = {"start_year": start_year, "end_year": end_year}

# Create result directory
# if not os.path.isdir('../result/'):
#     os.mkdir("../result/")

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
                    f.capx, f.oancf, f.dvt, f.ob, f.gdwlia, f.gdwlip, f.gwo, f.mib, f.oiadp, f.ivao,

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

# Calculate lagged variables

# Lagged variable 1: at_l1
raw_annual['at_l1'] = raw_annual.groupby(['permno'])['at'].shift(1)

# Lagged variable 2: act_l1
raw_annual['act_l1'] = raw_annual.groupby(['permno'])['act'].shift(1)

# Lagged variable 3: che_l1
raw_annual['che_l1'] = raw_annual.groupby(['permno'])['che'].shift(1)

# Lagged variable 4: lct_l1
raw_annual['lct_l1'] = raw_annual.groupby(['permno'])['lct'].shift(1)

# Lagged variable 5: txp_l1
raw_annual['txp_l1'] = raw_annual.groupby(['permno'])['txp'].shift(1)

# Lagged variable 6: lt_l1
raw_annual['lt_l1'] = raw_annual.groupby(['permno'])['lt'].shift(1)

# Lagged variable 7: dlc_l1
raw_annual['dlc_l1'] = raw_annual.groupby(['permno'])['dlc'].shift(1)

# Lagged variable 8: csho_l1
raw_annual['csho_l1'] = raw_annual.groupby(['permno'])['csho'].shift(1)

# Lagged variable 9: invt_l1
raw_annual['invt_l1'] = raw_annual.groupby(['permno'])['invt'].shift(1)

# Lagged variable 10: dvt_l1
raw_annual['dvt_l1'] = raw_annual.groupby(['permno'])['dvt'].shift(1)

raw_annual['ceq'] = np.where(raw_annual['ceq'] == 0, np.nan, raw_annual['ceq'])

# Lagged variable 11: ceq_l1
raw_annual['ceq_l1'] = raw_annual.groupby(['permno'])['ceq'].shift(1)

# Lagged variable 12: capx_l1
raw_annual['capx_l1'] = raw_annual.groupby(['permno'])['capx'].shift(1)

# Lagged variable 13: ppent_l1
raw_annual['ppent_l1'] = raw_annual.groupby(['permno'])['ppent'].shift(1)

# Lagged variable 14: rect_l1
raw_annual['rect_l1'] = raw_annual.groupby(['permno'])['rect'].shift(1)

# Lagged variable 15: aco_l1
raw_annual['aco_l1'] = raw_annual.groupby(['permno'])['aco'].shift(1)

# Lagged variable 16: intan_l1
raw_annual['intan_l1'] = raw_annual.groupby(['permno'])['intan'].shift(1)

# Lagged variable 17: ao_l1
raw_annual['ao_l1'] = raw_annual.groupby(['permno'])['ao'].shift(1)

# Lagged variable 18: ap_l1
raw_annual['ap_l1'] = raw_annual.groupby(['permno'])['ap'].shift(1)

# Lagged variable 19: lco_l1
raw_annual['lco_l1'] = raw_annual.groupby(['permno'])['lco'].shift(1)

# Lagged variable 20: lo_l1
raw_annual['lo_l1'] = raw_annual.groupby(['permno'])['lo'].shift(1)

# Lagged variable 21: emp_l1
raw_annual['emp_l1'] = raw_annual.groupby(['permno'])['emp'].shift(1)

# Lagged variable 22: ppegt_l1
raw_annual['ppegt_l1'] = raw_annual.groupby(['permno'])['ppegt'].shift(1)

# Lagged variable 23: dp_l1
raw_annual['dp_l1'] = raw_annual.groupby(['permno'])['dp'].shift(1)

# Lagged variable 24: sale_l1
raw_annual['sale_l1'] = raw_annual.groupby(['permno'])['sale'].shift(1)

# Lagged variable 25: cogs_l1
raw_annual['cogs_l1'] = raw_annual.groupby(['permno'])['cogs'].shift(1)

# Lagged variable 26: xsga_l1
raw_annual['xsga_l1'] = raw_annual.groupby(['permno'])['xsga'].shift(1)

# Lagged variable 27: ni_l1
raw_annual['ni_l1'] = raw_annual.groupby(['permno'])['ni'].shift(1)

# Lagged variable 28: xrd_l1
raw_annual['xrd_l1'] = raw_annual.groupby(['permno'])['xrd'].shift(1)

# Lagged variable 29: ib_l1
raw_annual['ib_l1'] = raw_annual.groupby(['permno'])['ib'].shift(1)

# Lagged variable 30: at_l2
raw_annual['at_l2'] = raw_annual.groupby(['permno'])['at'].shift(2)

# Lagged variable 31: capx_l2
raw_annual['capx_l2'] = raw_annual.groupby(['permno'])['capx'].shift(2)

# Lagged variable 32: capx_l2
raw_annual['dltt_l1'] = raw_annual.groupby(['permno'])['dltt'].shift(1)

# Feature 2: acc
raw_annual["acc"] = (raw_annual["ib"] - raw_annual["oancf"]) / ((raw_annual["at"] + raw_annual["at_l1"]) / 2) 
raw_annual["acc"] = np.where((raw_annual['oancf'].isnull()), ((((raw_annual["act"] 
- raw_annual["act_l1"]) - (raw_annual["che"] - raw_annual["che_l1"])) 
- (raw_annual["lct"] - raw_annual["lct_l1"]) - (raw_annual["dlc"] - raw_annual["dlc_l1"]) 
- (raw_annual["txp"] - raw_annual["txp_l1"]) - raw_annual["dp"]) / ((raw_annual["at"] + raw_annual["at_l1"]) / 2))
, raw_annual['acc'])

# Feature 1: absacc
raw_annual["absacc"] = abs(raw_annual["acc"])

# Feature 3: age
raw_annual['age'] = raw_annual.groupby(['gvkey']).cumcount()

# Feature 4: agr
raw_annual['agr'] = (raw_annual['at_l1'] - raw_annual['at']) / raw_annual['at_l1']

# preferred stock
raw_annual['pref_stock'] = np.where(raw_annual['pstkrv'].isnull(), raw_annual['pstkl'], raw_annual['pstkrv'])
raw_annual['pref_stock'] = np.where(raw_annual['pref_stock'].isnull(), raw_annual['pstk'], raw_annual['pref_stock'])
raw_annual['pref_stock'] = np.where(raw_annual['pref_stock'].isnull(), 0, raw_annual['pref_stock'])
raw_annual['txditc'] = raw_annual['txditc'].fillna(0)

# book equity
raw_annual['be'] = raw_annual['seq'] + raw_annual['txditc'] - raw_annual['pref_stock']
raw_annual['be'] = np.where(raw_annual['be'] > 0, raw_annual['be'], np.nan)

# Intermediate variable 1: mve_f
raw_annual["mve_f"] = raw_annual["csho"] * abs(raw_annual["prcc_f"])

# Market Equity
raw_annual['me'] = raw_annual['mve_f']  # Compustat ME
raw_annual['me'] = np.where(raw_annual['me'] == 0, np.nan, raw_annual['me'])

# Feature 5: bm
raw_annual["bm"] = raw_annual["be"] / raw_annual["me"]

# Feature 6: bm_ia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['bm'].mean()
df_temp = df_temp.rename(columns={'bm': 'bm_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['bm_ia'] = raw_annual['bm'] / raw_annual['bm_ind']

# Feature 7: cashdebt
raw_annual["cashdebt"] = (raw_annual["ib"] + raw_annual["dp"]) / ((raw_annual["lt"] + raw_annual["lt_l1"]) / 2)

# Feature 8: cashpr
raw_annual["cashpr"] = (raw_annual["mve_f"] + raw_annual["dltt"] - raw_annual["at"]) / raw_annual["che"]

# Feature 9: cfp
raw_annual["cfp"] = raw_annual["oancf"] / raw_annual["mve_f"]
raw_annual["cfp"] = np.where(raw_annual['oancf'].notna(), raw_annual["cfp"],
(raw_annual["ib"] - ((raw_annual["act"] - raw_annual["act_l1"] - (raw_annual["che"] - raw_annual["che_l1"]))
- ((raw_annual["lct"] - raw_annual["lct_l1"]) - (raw_annual["dlc"] 
- raw_annual["dlc_l1"]) - (raw_annual["txp"] - raw_annual["txp_l1"]) - raw_annual["dp"]))) / raw_annual["mve_f"])

# Feature 10: cfp_ia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['cfp'].mean()
df_temp = df_temp.rename(columns={'cfp': 'cfp_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['cfp_ia'] = raw_annual['cfp'] / raw_annual['cfp_ind']

# Intermediate variable 2: chato
raw_annual['chato'] = (raw_annual['sale']/((raw_annual['at']+raw_annual['at_l1'])/2))-(raw_annual['sale_l1']/((raw_annual['at']+raw_annual['at_l2'])/2))

# Feature 11: chatoia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['chato'].mean()
df_temp = df_temp.rename(columns={'chato': 'chato_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['chatoia'] = raw_annual['chato'] / raw_annual['chato_ind']

# Feature 12: chcsho
raw_annual["chcsho"] = (raw_annual["csho"] / raw_annual["csho_l1"]) - 1

# Feature 28: hire
raw_annual['hire'] = (raw_annual['emp'] - raw_annual['emp_l1']) / raw_annual['emp_l1']
raw_annual['hire'] = np.where((raw_annual['emp'].isnull()) | (raw_annual['emp_l1'].isnull()), 0, raw_annual['hire'])

# Feature 13: chemp_ia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['hire'].mean()
df_temp = df_temp.rename(columns={'hire': 'chemp_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['chempia'] = raw_annual['hire'] / raw_annual['chemp_ind']

# Feature 14: chinv
raw_annual["chinv"] = (raw_annual["invt"] - raw_annual["invt_l1"]) / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)

# Intermediate variable 3: chpm
raw_annual["chpm"] = (raw_annual["ib"] / raw_annual["sale"]) - (raw_annual["ib_l1"] / raw_annual["sale_l1"])

# Feature 15: chpmia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['chpm'].mean()
df_temp = df_temp.rename(columns={'chpm': 'chpm_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['chpmia'] = raw_annual['chpm'] / raw_annual['chpm_ind']

# Intermediate variable 4: dc
condlist = [raw_annual['dcvt'].isnull() & raw_annual['dcpstk'].notna() & raw_annual['pstk'].notna() & raw_annual['dcpstk'] > raw_annual['pstk'],
            raw_annual['dcvt'].isnull() & raw_annual['dcpstk'].notna() & raw_annual['pstk'].isnull()]

choicelist = [raw_annual['dcpstk'] - raw_annual['pstk'], raw_annual['dcpstk']]

raw_annual['dc'] = np.select(condlist, choicelist, default=np.nan)

raw_annual['dc'] = np.where(raw_annual['dc'].isnull(), raw_annual['dcvt'], raw_annual['dc'])

# Feature 16: convind
raw_annual["convind"] = 1
                      
raw_annual["convind"] = np.where((raw_annual['dc'].notna() & raw_annual['dc'] != 0)|(raw_annual['cshrc'].notna() & raw_annual['cshrc'] != 0), raw_annual["convind"], 0)

# Feature 17: currat
raw_annual["currat"] = raw_annual["act"] / raw_annual["lct"]

# Feature 18: depr
raw_annual["depr"] = raw_annual["dp"] / raw_annual["ppent"]

# Feature 19: divi
raw_annual["divi"] = 1
raw_annual["divi"] = np.where((raw_annual["dvt"].notna() & raw_annual["dvt"] > 0) & ((raw_annual["dvt_l1"] == 0)|(raw_annual["dvt_l1"].isnull())), raw_annual["divi"], 0)

# Feature 20: divo                        
raw_annual["divo"] = 1
raw_annual["divo"] = np.where(((raw_annual["dvt"].isnull()) & (raw_annual["dvt"] == 0)) & (((raw_annual["dvt_l1"] > 0) & (raw_annual["dvt_l1"])).notna()), raw_annual["divo"], 0) 

# Feature 21: dy
raw_annual["dy"] = raw_annual["dvt"] / raw_annual["mve_f"]

# Feature 22: egr
raw_annual["egr"] = (raw_annual["ceq"] - raw_annual["ceq_l1"]) / raw_annual["ceq_l1"]

# Feature 23: ep
raw_annual["ep"] = raw_annual["ib"] / raw_annual["mve_f"]

# Feature 24: gma
raw_annual["gma"] = (raw_annual["revt"] - raw_annual["cogs"]) / raw_annual["at_l1"]

# Feature 25: grcapx          
raw_annual['grcapx'] = (raw_annual['capx'] - raw_annual['capx_l2']) / raw_annual['capx_l2']

# Feature 26: grltnoa
raw_annual['grltnoa'] = ((raw_annual['rect']+raw_annual['invt']+raw_annual['ppent']+raw_annual['aco']+raw_annual['intan']+
                       raw_annual['ao']-raw_annual['ap']-raw_annual['lco']-raw_annual['lo'])
                        -(raw_annual['rect_l1']+raw_annual['invt_l1']+raw_annual['ppent_l1']+raw_annual['aco_l1']
                       +raw_annual['intan_l1']+raw_annual['ao_l1']-raw_annual['ap_l1']-raw_annual['lco_l1']
                       -raw_annual['lo_l1'])
                        -(raw_annual['rect']-raw_annual['rect_l1']+raw_annual['invt']-raw_annual['invt_l1']
                          +raw_annual['aco']-raw_annual['aco_l1']
                          -(raw_annual['ap']-raw_annual['ap_l1']+raw_annual['lco']-raw_annual['lco_l1'])-raw_annual['dp']))\
                       /((raw_annual['at']+raw_annual['at_l1'])/2)

# Feature 27: herf
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['sale'].sum()
df_temp = df_temp.rename(columns={'sale': 'indsale'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['herf'] = (raw_annual['sale']/raw_annual['indsale'])*(raw_annual['sale']/raw_annual['indsale'])
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['herf'].sum()
raw_annual = raw_annual.drop(['herf'], axis=1)
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])

# Feature 29: invest
raw_annual['invest'] = ((raw_annual['ppegt'] - raw_annual['ppegt_l1']) + (raw_annual['invt'] - raw_annual['invt_l1'])) / raw_annual['at_l1']
raw_annual['invest'] = np.where((raw_annual['ppegt'].isnull()), ((raw_annual['ppent'] - raw_annual['ppent_l1']) + (raw_annual['invt'] - raw_annual['invt_l1'])) / raw_annual['at_l1'], raw_annual['invest'])

# Feature 30: lev
raw_annual["lev"] = raw_annual["lt"] / raw_annual["mve_f"]

# Feature 31: lgr
raw_annual["lgr"] = (raw_annual["lt"] / raw_annual["lt_l1"]) - 1

# Feature 32: mve_ia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['mve_f'].mean()
df_temp = df_temp.rename(columns={'mve_f': 'mve_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['mve_ia'] = raw_annual['mve_f'] / raw_annual['mve_ind']

# Intermediate variable 5: xsga0
raw_annual['xsga0'] = np.where(raw_annual['xsga'].isnull(), 0, raw_annual['xsga'])

# Intermediate variable 6: xint0
raw_annual['xint0'] = np.where(raw_annual['xint'].isnull(), 0, raw_annual['xint'])

# Feature 33: operprof
raw_annual['operprof'] = (raw_annual['revt'] - raw_annual['cogs'] - raw_annual['xsga0'] - raw_annual['xint0']) / raw_annual['ceq_l1']

# Intermediate variable 7: avgat
raw_annual["avgat"] = (raw_annual["at"] + raw_annual["at_l1"]) / 2

# Intermediate variable 8: cpi_data

cpi_data = pd.read_csv("../src/cpi_data.csv")
cpi_data['DATE'] = pd.to_datetime(cpi_data['DATE'])
cpi_data['DATE'] = cpi_data.DATE.dt.year.astype('float')
cpi_data.rename(columns={'DATE':'fyear'}, inplace=True)
raw_annual = pd.merge(raw_annual, cpi_data, how='left', on='fyear')

# Feature 34: orgcap

raw_annual['orgcap_1'] = (raw_annual['xsga'] / raw_annual['CPIAUCSL']) / (0.1 + 0.15)
raw_annual['orgcap'] =  raw_annual['orgcap_1'] / raw_annual["avgat"]

# Intermediate variable 10: pchcapx
raw_annual["pchcapx"] = (raw_annual["capx"] - raw_annual["capx_l1"]) / raw_annual["capx_l1"]

# Feature 35: pchcapx_ia
df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['pchcapx'].mean()
df_temp = df_temp.rename(columns={'pchcapx': 'pchcapx_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['pchcapx_ia'] = raw_annual['pchcapx'] / raw_annual['pchcapx_ind']

# Feature 36: pchcurrat
raw_annual['pchcurrat'] = ((raw_annual['act']/raw_annual['lct'])-(raw_annual['act_l1']/raw_annual['lct_l1']))                         /(raw_annual['act_l1']/raw_annual['lct_l1'])

# Feature 37: pchdepr 
raw_annual["pchdepr"] = ((raw_annual["dp"] / raw_annual["ppent"])- (raw_annual["dp_l1"] / raw_annual["ppent_l1"]))/ (raw_annual["dp_l1"] / raw_annual["ppent_l1"])

# Feature 38: pchgm_pchsale
raw_annual['pchgm_pchsale'] = (((raw_annual['sale'] - raw_annual['cogs']) - (raw_annual['sale_l1']-raw_annual['cogs_l1']))/(raw_annual['sale_l1']-raw_annual['cogs_l1'])) - ((raw_annual['sale'] - raw_annual['sale_l1'])/raw_annual['sale'])

# Feature 39: pchquick
raw_annual["pchquick"] = ((raw_annual["act"] - raw_annual["invt"]) / raw_annual["lct"]
- ((raw_annual["act_l1"] - raw_annual["invt_l1"])) / raw_annual["lct_l1"]) / ((raw_annual["act_l1"] - raw_annual["invt_l1"])/ raw_annual["lct_l1"])

# Feature 40: pchsale_pchinvt
raw_annual['pchsale_pchinvt'] = ((raw_annual['sale'] - raw_annual['sale_l1'])/raw_annual['sale_l1'])                               - ((raw_annual['invt']-raw_annual['invt_l1'])/raw_annual['invt_l1'])

# Feature 41: pchsale_pchrect
raw_annual['pchsale_pchrect'] = ((raw_annual['sale']-raw_annual['sale_l1'])/raw_annual['sale_l1'])                               - ((raw_annual['rect']-raw_annual['rect_l1'])/raw_annual['rect_l1'])

# Feature 42: pchsale_pchxsga
raw_annual['pchsale_pchxsga'] = ((raw_annual['sale']-raw_annual['sale_l1'])/raw_annual['sale_l1'])                               - ((raw_annual['xsga']-raw_annual['xsga_l1'])/raw_annual['xsga_l1'])

# Feature 43: pchsaleinv
raw_annual['pchsaleinv'] = ((raw_annual['sale']/raw_annual['invt'])-(raw_annual['sale_l1']/raw_annual['invt_l1']))                          /(raw_annual['sale_l1']/raw_annual['invt_l1'])

# Feature 44: pctacc
condlist = [raw_annual['ib']==0,
            raw_annual['oancf'].isnull(),
            raw_annual['oancf'].isnull() & raw_annual['ib']==0]

choicelist = [(raw_annual['ib']-raw_annual['oancf'])/0.01,
              ((raw_annual['act'] - raw_annual['act_l1']) - (raw_annual['che'] - raw_annual['che_l1']))-
              ((raw_annual['lct'] - raw_annual['lct_l1']) - (raw_annual['dlc']) - raw_annual['dlc_l1']-
               ((raw_annual['txp'] - raw_annual['txp_l1']) - raw_annual['dp']))/raw_annual['ib'].abs(),
              ((raw_annual['act'] - raw_annual['act_l1']) - (raw_annual['che'] - raw_annual['che_l1'])) -
              ((raw_annual['lct'] - raw_annual['lct_l1']) - (raw_annual['dlc']) - raw_annual['dlc_l1'] -
               ((raw_annual['txp'] - raw_annual['txp_l1']) - raw_annual['dp']))]

raw_annual['pctacc'] = np.select(condlist, choicelist, default=(raw_annual['ib']-raw_annual['oancf'])/raw_annual['ib'].abs())

# Feature 45: ps

raw_annual['p_temp1'] = np.where(raw_annual['ni']>0, 1, 0)
raw_annual['p_temp2'] = np.where(raw_annual['oancf']>0, 1, 0)
raw_annual['p_temp3'] = np.where(raw_annual['ni']/raw_annual['at']>raw_annual['ni_l1']/raw_annual['at_l1'], 1, 0)
raw_annual['p_temp4'] = np.where(raw_annual['oancf']>raw_annual['ni'], 1, 0)
raw_annual['p_temp5'] = np.where(raw_annual['dltt']/raw_annual['at']<raw_annual['dltt_l1']/raw_annual['at_l1'], 1, 0)
raw_annual['p_temp6'] = np.where(raw_annual['act']/raw_annual['lct'] > raw_annual['act_l1']/raw_annual['lct_l1'], 1, 0)
raw_annual['p_temp7'] = np.where((raw_annual['sale']-raw_annual['cogs']/raw_annual['sale'])>(raw_annual['sale_l1']-raw_annual['cogs_l1']/raw_annual['sale_l1']), 1, 0)
raw_annual['p_temp8'] = np.where(raw_annual['sale']/raw_annual['at']>raw_annual['sale_l1']/raw_annual['at_l1'], 1, 0)
raw_annual['p_temp9'] = np.where(raw_annual['scstkc']==0, 1, 0)
raw_annual['ps'] = raw_annual['p_temp1']+raw_annual['p_temp2']+raw_annual['p_temp3']+raw_annual['p_temp4']                      +raw_annual['p_temp5']+raw_annual['p_temp6']+raw_annual['p_temp7']+raw_annual['p_temp8']                      +raw_annual['p_temp9']

# Feature 46: quick
raw_annual["quick"] = (raw_annual["act"] - raw_annual["invt"]) / raw_annual["lct"]

# Feature 47: rd                              
raw_annual['xrd/at_l1'] = raw_annual['xrd']/raw_annual['at_l1']                              
raw_annual['xrd/at_l1_l1'] = raw_annual.groupby(['permno'])['xrd/at_l1'].shift(1)                                
raw_annual['rd'] = np.where(((raw_annual['xrd']/raw_annual['at']) - (raw_annual['xrd/at_l1_l1']))/raw_annual['xrd/at_l1_l1'] > 0.05, 1, 0)

# Feature 48: rd_mve
raw_annual["rd_mve"] = raw_annual["xrd"] / raw_annual["mve_f"]

# Feature 49: rd_sale
raw_annual["rd_sale"] = raw_annual["xrd"] / raw_annual["sale"]

# Feature 50: realestate
raw_annual['realestate'] = (raw_annual['fatb']+raw_annual['fatl'])/raw_annual['ppegt']                                
raw_annual['realestate'] = np.where(raw_annual['ppegt'].isnull(), (raw_annual['fatb']+raw_annual['fatl'])/raw_annual['ppent'], raw_annual['realestate'])

# Feature 51: roic
raw_annual['roic'] = (raw_annual['ebit'] - raw_annual['nopi'])/(raw_annual['ceq'] + raw_annual['lt'] - raw_annual['che'])

# Feature 52: salecash
raw_annual["salecash"] = raw_annual["sale"] / raw_annual["che"]

# Feature 53: saleinv
raw_annual["saleinv"] = raw_annual["sale"] / raw_annual["invt"]

# Feature 54: salerec
raw_annual["salerec"] = raw_annual["sale"] / raw_annual["rect"]

# Feature 55: secured
raw_annual["secured"] = raw_annual["dm"] / raw_annual["dltt"]

# Feature 56: securedind
raw_annual["securedind"] = 1
raw_annual["securedind"] = np.where(raw_annual["dm"].notna() & raw_annual["dm"] != 0, raw_annual["securedind"], 0)

# Feature 57: sgr
raw_annual["sgr"] = (raw_annual["sale"] / raw_annual["sale_l1"]) - 1

# Feature 58: sin
raw_annual["sin"] = 1
raw_annual["sin"] = np.where(((raw_annual["sic"] >= 2100) & (raw_annual["sic"] <= 2199)) | 
                            ((raw_annual["sic"] >= 2080) & (raw_annual["sic"] <= 2085)) |
                            ((raw_annual["naics"].isin(['7132','71312','713210','71329','713290','72112','721120']))) , raw_annual["sin"], 0)

# Feature 59: sp
raw_annual["sp"] = raw_annual["sale"] / raw_annual["mve_f"]

# Feature 60: tang
raw_annual["tang"] = (raw_annual["che"] + raw_annual["rect"] * 0.715 + raw_annual["invt"] * 0.547 + raw_annual["ppent"] * 0.535) / raw_annual["at"]

# Intermediate variable 11: tr

condlist = ((raw_annual["fyear"] <= 1978), ((raw_annual["fyear"] >= 1978) & (raw_annual["fyear"] <= 1986)), (raw_annual["fyear"] == 1987), ((raw_annual["fyear"] >= 1988) & (raw_annual["fyear"] <= 1992)), (raw_annual["fyear"] >= 1993))
choicelist = (0.48, 0.46, 0.40, 0.34, 0.35)
raw_annual['tr'] = np.select(condlist, choicelist)

# Intermediate variable 12: tb_1

condlist = ((raw_annual["txfo"].isnull()) | (raw_annual["txfed"].isnull())) ,  (((raw_annual["txfo"] + raw_annual["txfed"] > 0) | (raw_annual["txt"] > raw_annual["txdi"])) & (raw_annual["ib"] <= 0))
choicelist = ((((raw_annual["txt"] - raw_annual["txdi"]) / raw_annual["tr"]) / raw_annual["ib"]) , 1)
raw_annual['tb_1'] = np.select(condlist, choicelist, default = ((raw_annual["txfo"] + raw_annual["txfed"]) / raw_annual["tr"]) / raw_annual["ib"])

# Feature 61: tb

df_temp = raw_annual.groupby(['datadate', 'sic2'], as_index=False)['tb_1'].mean()
df_temp = df_temp.rename(columns={'tb_1': 'tb_1_ind'})
raw_annual = pd.merge(raw_annual, df_temp, how='left', on=['datadate', 'sic2'])
raw_annual['tb'] = raw_annual['tb_1'] / raw_annual['tb_1_ind']

raw_annual = raw_annual.drop_duplicates(subset=['permno', 'datadate'])

#Export 61 annual feature data to CSV file
# raw_annual[["permno", "datadate", "sic2", "absacc", "acc", "age", "agr", "bm", "bm_ia", "cashdebt", "cashpr", "cfp", "cfp_ia", 
# "chatoia", "chcsho", "chempia", "chinv", "chpmia", "convind", "currat", "depr", "divi", "divo",
# "dy", "egr", "ep", "gma", "grcapx", "grltnoa", "herf", "hire", "invest", "lev",
# "lgr", "mve_ia", "operprof", "orgcap", "pchcapx_ia", "pchcurrat", "pchdepr", "pchgm_pchsale", "pchquick", "pchsale_pchinvt",
# "pchsale_pchrect", "pchsale_pchxsga", "pchsaleinv", "pctacc", "ps", "quick", "rd", "rd_mve", "rd_sale", "realestate",
# "roic", "salecash", "saleinv", "salerec", "secured", "securedind", "sgr", "sin", "sp", "tang",
# "tb"]].to_csv("../result/annual_features.csv", index = False)

# print("annual_features.csv saved to result folder")


# In[4]:


#Quarterly feature generation

import wrds
import pandas as pd
import numpy as np
import json
import os
import datetime
from pandas.tseries.offsets import *

# Load config file
with open('config.json') as config_file:
    data = json.load(config_file)

start_year = data['start_year']
end_year = data['end_year']
wrds_username = data['wrds_username']

parm = {"start_year": start_year, "end_year": end_year}

# Create result directory
# if not os.path.isdir('../result/'):
#     os.mkdir("../result/")

# Query data from WRDS
conn = wrds.Connection(wrds_username=wrds_username)

comp_quarter = conn.raw_sql("""
                    /*header info*/
                    select c.gvkey, f.cusip, f.datadate, f.fyearq, substr(c.sic,1,2) as sic2, c.sic, f.fqtr, f.rdq,

                    /*income statement*/
                    f.ibq, f.saleq, f.txtq, f.revtq, f.cogsq, f.xsgaq, 

                    /*balance sheet items*/
                    f.actq, f.cheq, f.lctq, f.dlcq, f.ppentq, 

                    /*others*/
                    abs(f.prccq) as prccq, abs(f.prccq)*f.cshoq as mveq, f.ceqq, f.seqq, f.pstkq, f.atq, f.ltq,
                    f.pstkrq

                    from comp.fundq as f
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

# Merge permno with compustat data

comp_crsp_link.drop(comp_crsp_link[comp_crsp_link.linktype == "LD"].index, inplace=True)

comp_crsp_link['linkdt'] = pd.to_datetime(comp_crsp_link['linkdt'])
comp_crsp_link['linkenddt'] = pd.to_datetime(comp_crsp_link['linkenddt'])
comp_crsp_link['linkenddt'] = comp_crsp_link['linkenddt'].fillna(pd.to_datetime('today'))

raw_quarter = comp_quarter.merge(comp_crsp_link[["gvkey", "permno", "linkdt", "linkenddt"]], how="left", on=["gvkey"])
raw_quarter["permno"] = raw_quarter["permno"].astype("Int64")

raw_quarter["datadate"] = pd.to_datetime(raw_quarter["datadate"])
raw_quarter = raw_quarter.sort_values(["permno", "datadate"]).drop_duplicates()

# Set link date bounds
raw_quarter = raw_quarter[(raw_quarter['datadate'] >= raw_quarter['linkdt']) & (raw_quarter['datadate'] <= raw_quarter['linkenddt'])]
raw_quarter = raw_quarter.drop(['linkdt', 'linkenddt'], axis=1)

# Calculate features

# cash
raw_quarter["cash"] = raw_quarter["cheq"] / raw_quarter["atq"]

# chtx
raw_quarter["txtq_l4"] = raw_quarter.groupby(["permno"])["txtq"].shift(4)
raw_quarter["atq_l4"] = raw_quarter.groupby(["permno"])["atq"].shift(4)
raw_quarter["chtx"] = (raw_quarter["txtq"] - raw_quarter["txtq_l4"]) / raw_quarter['atq_l4']

# roaq
raw_quarter["atq_l1"] = raw_quarter.groupby(["permno"])["atq"].shift(1)
raw_quarter["roaq"] = raw_quarter["ibq"] / raw_quarter["atq_l1"]

# roeq
raw_quarter["pstk"] = np.where(raw_quarter["pstkrq"].isnull(), raw_quarter["pstkq"], raw_quarter["pstkrq"])
condlist = [raw_quarter["seqq"].isnull(), 
            raw_quarter["seqq"].isnull() & (raw_quarter["ceqq"].isnull() | raw_quarter["pstk"].isnull())]
choicelist = [raw_quarter["ceqq"] + raw_quarter["pstk"], raw_quarter["atq"] - raw_quarter["ltq"]]
raw_quarter["scal"] = np.select(condlist, choicelist, default = raw_quarter["seqq"])
raw_quarter["scal_l1"] = raw_quarter.groupby(["permno"])["scal"].shift(1)
raw_quarter["roeq"] = raw_quarter["ibq"] / raw_quarter["scal_l1"]

# rsup
# raw_quarter["mveq"] = abs(raw_quarter["prccq"]) * raw_quarter["cshoq"]
raw_quarter["saleq_l4"] = raw_quarter.groupby(["permno"])["saleq"].shift(4)
raw_quarter["rsup"] = (raw_quarter["saleq"] - raw_quarter["saleq_l4"]) / raw_quarter["mveq"]

# cinvest
for i in range(4):
    raw_quarter["ppentq_l" + str(i+1)] = raw_quarter.groupby(["permno"])["ppentq"].shift(i+1)
    raw_quarter["saleq_l" + str(i+1)] = raw_quarter.groupby(["permno"])["saleq"].shift(i+1)

raw_quarter["cinv_1"] = (raw_quarter["ppentq_l1"] - raw_quarter["ppentq_l2"]) / raw_quarter["saleq_l1"]
raw_quarter["cinv_2"] = (raw_quarter["ppentq_l2"] - raw_quarter["ppentq_l3"]) / raw_quarter["saleq_l2"]
raw_quarter["cinv_3"] = (raw_quarter["ppentq_l3"] - raw_quarter["ppentq_l4"]) / raw_quarter["saleq_l3"]

raw_quarter["cinvest"] = ((raw_quarter["ppentq"] - raw_quarter["ppentq_l1"]) / raw_quarter["saleq"] 
                          - raw_quarter[["cinv_1", "cinv_2", "cinv_3"]].mean(axis=1))

raw_quarter["cinv_4"] = (raw_quarter["ppentq_l1"] - raw_quarter["ppentq_l2"]) / 0.01
raw_quarter["cinv_5"] = (raw_quarter["ppentq_l2"] - raw_quarter["ppentq_l3"]) / 0.01
raw_quarter["cinv_6"] = (raw_quarter["ppentq_l3"] - raw_quarter["ppentq_l4"]) / 0.01

raw_quarter["cinvest"] = np.where(raw_quarter["saleq"] <= 0, ((raw_quarter["ppentq"] - raw_quarter["ppentq_l1"]) / 0.01 - 
                                  raw_quarter[["cinv_4", "cinv_5", "cinv_6"]].mean(axis=1)), raw_quarter["cinvest"])

raw_quarter = raw_quarter.drop(["cinv_1", "cinv_2", "cinv_3", "cinv_4", "cinv_5", "cinv_6"], axis=1)

# nincr
temp_nincr = []
for i in range(8):
    raw_quarter["ibq_l" + str(i+1)] = raw_quarter.groupby(["permno"])["ibq"].shift(i+1)
    temp_nincr.append("ibq_l" + str(i+1))
    if i == 0:
        raw_quarter["nincr_t" + str(i+1)] = np.where(raw_quarter["ibq"] > raw_quarter["ibq_l1"], 1, 0)
    else:
        raw_quarter["nincr_t" + str(i+1)] = np.where(raw_quarter["ibq_l" + str(i)] > raw_quarter["ibq_l" + str(i+1)], 1, 0)
    temp_nincr.append("nincr_t" + str(i+1))

raw_quarter["nincr"] = (raw_quarter["nincr_t1"]
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"] * raw_quarter["nincr_t4"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"] * raw_quarter["nincr_t4"] * raw_quarter["nincr_t5"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"] * raw_quarter["nincr_t4"] * raw_quarter["nincr_t5"] * raw_quarter["nincr_t6"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"] * raw_quarter["nincr_t4"] * raw_quarter["nincr_t5"] * raw_quarter["nincr_t6"] * raw_quarter["nincr_t7"])
                        + (raw_quarter["nincr_t1"] * raw_quarter["nincr_t2"] * raw_quarter["nincr_t3"] * raw_quarter["nincr_t4"] * raw_quarter["nincr_t5"] * raw_quarter["nincr_t6"] * raw_quarter["nincr_t7"] * raw_quarter["nincr_t8"]))

raw_quarter = raw_quarter.drop(temp_nincr, axis=1)

# roavol
temp_roavol = []
for i in range(16):
    raw_quarter["roaq_l" + str(i)] = raw_quarter.groupby(["permno"])["roaq"].shift(i)
    temp_roavol.append("roaq_l" + str(i))

raw_quarter["roavol"] = raw_quarter[temp_roavol].std(axis=1)
raw_quarter = raw_quarter.drop(temp_roavol, axis=1) 

# stdacc
raw_quarter["actq_l1"] = raw_quarter.groupby(["permno"])["actq"].shift(1)
raw_quarter["cheq_l1"] = raw_quarter.groupby(["permno"])["cheq"].shift(1)
raw_quarter["lctq_l1"] = raw_quarter.groupby(["permno"])["lctq"].shift(1)
raw_quarter["dlcq_l1"] = raw_quarter.groupby(["permno"])["dlcq"].shift(1)

raw_quarter["sacc"] = (((raw_quarter["actq"] - raw_quarter["actq_l1"]) - (raw_quarter["cheq"] - raw_quarter["cheq_l1"])) - 
                       ((raw_quarter["lctq"] - raw_quarter["lctq_l1"]) - (raw_quarter["dlcq"] - raw_quarter["dlcq_l1"]))) / raw_quarter["saleq"]
raw_quarter["sacc"] = np.where(raw_quarter["saleq"] <= 0, ((raw_quarter["actq"] - raw_quarter["actq_l1"]) - 
                       (raw_quarter["cheq"] - raw_quarter["cheq_l1"]) - (raw_quarter["lctq"] - raw_quarter["lctq_l1"]) - 
                       (raw_quarter["dlcq"] - raw_quarter["dlcq_l1"])) / 0.01, raw_quarter["sacc"])

temp_stdacc = []
for i in range(16):
    raw_quarter["sacc_l" + str(i)] = raw_quarter.groupby(["permno"])["sacc"].shift(i)
    temp_stdacc.append("sacc_l" + str(i))

raw_quarter["stdacc"] = raw_quarter[temp_stdacc].std(axis=1)
raw_quarter = raw_quarter.drop(temp_stdacc, axis=1) 

# stdcf
raw_quarter["scf"] = (raw_quarter["ibq"] / raw_quarter["saleq"]) - raw_quarter["sacc"]
raw_quarter["scf"] = np.where(raw_quarter["saleq"] <= 0, (raw_quarter["ibq"] / 0.01) - raw_quarter["sacc"], raw_quarter["scf"])

temp_stdcf = []
for i in range(16):
    raw_quarter["scf_l" + str(i)] = raw_quarter.groupby(["permno"])["scf"].shift(i)
    temp_stdcf.append("scf_l" + str(i))

raw_quarter["stdcf"] = raw_quarter[temp_stdcf].std(axis=1)
raw_quarter = raw_quarter.drop(temp_stdcf, axis=1)

# Query in comp.funda to calculate ms

comp_annual = conn.raw_sql("""
                    select c.gvkey, f.datadate, f.fyear, substr(c.sic,1,2) as sic2, 

                    f.at, f.ni, f.oancf, f.ib, f.dp, f.xrd, f.capx, f.xad

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

conn.close()

raw_annual = comp_annual.merge(comp_crsp_link[["gvkey", "permno"]], how="left", on=["gvkey"])
raw_annual["permno"] = raw_annual["permno"].astype("Int64")
raw_annual["datadate"] = pd.to_datetime(raw_annual["datadate"])
raw_annual = raw_annual.sort_values(["permno", "datadate"]).drop_duplicates()
raw_annual["date_std"] = raw_annual["datadate"] + MonthEnd(0)

# Drop rows with no permno / fyear

raw_annual.dropna(subset=["fyear", "permno"], inplace=True)
raw_quarter.dropna(subset=["permno"], inplace=True)

# ms

# Calculate annual intermediate variables
raw_annual["at_l1"] = raw_annual.groupby(["permno"])["at"].shift(1)
raw_annual["roa"] = raw_annual["ni"] / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)
raw_annual["cfroa"] = raw_annual["oancf"] / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)
raw_annual["cfroa"] = np.where(raw_annual["oancf"].isnull(), (raw_annual["ib"] + raw_annual["dp"]) / 
                               ((raw_annual["at"] + raw_annual["at_l1"]) /2), raw_annual["cfroa"])
raw_annual["xrdint"] = raw_annual["xrd"] / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)
raw_annual["capxint"] = raw_annual["capx"] / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)
raw_annual["xadint"] = raw_annual["xad"] / ((raw_annual["at"] + raw_annual["at_l1"]) / 2)

# Calculate annual industry averages
df_temp = raw_annual.groupby(["fyear", "sic2"], as_index=False)[["roa", "cfroa", "xrdint", "capxint", "xadint"]].median()
df_temp = df_temp.rename(columns={"roa": "md_roa", "cfroa": "md_cfroa", "xrdint": "md_xrdint", 
                                  "capxint": "md_capxint", "xadint": "md_xadint"})
raw_annual = pd.merge(raw_annual, df_temp, how="left", on=["fyear", "sic2"])

# Calculate annual values
raw_annual["m1"] = np.where(raw_annual["roa"] > raw_annual["md_roa"], 1, 0)
raw_annual["m2"] = np.where(raw_annual["cfroa"] > raw_annual["md_cfroa"], 1, 0)
raw_annual["m3"] = np.where(raw_annual["oancf"] > raw_annual["ni"], 1, 0)
raw_annual["m4"] = np.where(raw_annual["xrdint"] > raw_annual["md_xrdint"], 1, 0)
raw_annual["m5"] = np.where(raw_annual["capxint"] > raw_annual["md_capxint"], 1, 0)
raw_annual["m6"] = np.where(raw_annual["xadint"] > raw_annual["md_xadint"], 1, 0)

# raw_annual["m1"] = np.select([raw_annual["roa"] > raw_annual["md_roa"], raw_annual["roa"] <= raw_annual["md_roa"]], 
#                              [1, 0], default = np.nan)
# raw_annual["m2"] = np.select([raw_annual["cfroa"] > raw_annual["md_cfroa"], raw_annual["cfroa"] <= raw_annual["md_cfroa"]], 
#                              [1, 0], default = np.nan)
# raw_annual["m3"] = np.select([raw_annual["oancf"] > raw_annual["ni"], raw_annual["oancf"] <= raw_annual["ni"]], 
#                              [1, 0], default = np.nan)
# raw_annual["m4"] = np.select([raw_annual["xrdint"] > raw_annual["md_xrdint"], raw_annual["xrdint"] <= raw_annual["md_xrdint"]], 
#                              [1, 0], default = np.nan)
# raw_annual["m5"] = np.select([raw_annual["capxint"] > raw_annual["md_capxint"], raw_annual["capxint"] <= raw_annual["md_capxint"]], 
#                              [1, 0], default = np.nan)
# raw_annual["m6"] = np.select([raw_annual["xadint"] > raw_annual["md_xadint"], raw_annual["xadint"] <= raw_annual["md_xadint"]], 
#                              [1, 0], default = np.nan)

# Merge to raw_quarter
merge_cols = ["permno", "fyear", "m1", "m2", "m3", "m4", "m5", "m6"]
raw_quarter = pd.merge(raw_quarter, raw_annual[merge_cols], how="left", 
                       left_on=["fyearq", "permno"], right_on=["fyear", "permno"])

# Calculate quarterly intermediate variables
temp_sgrvol = []
for i in range(15):
    raw_quarter["rsup_l" + str(i)] = raw_quarter.groupby(["permno"])["rsup"].shift(i)
    temp_sgrvol.append("rsup_l" + str(i))
raw_quarter["sgrvol"] = raw_quarter[temp_sgrvol].std(axis=1)
raw_quarter = raw_quarter.drop(temp_sgrvol, axis=1) 

# Calculate quarterly industry averages
df_temp = raw_quarter.groupby(["fyear", "sic2"], as_index=False)[["roavol", "sgrvol"]].median()
df_temp = df_temp.rename(columns={"roavol": "md_roavol", "sgrvol": "md_sgrvol"})
raw_quarter = pd.merge(raw_quarter, df_temp, how="left", on=["fyear", "sic2"])

# Calculate quarterly values
raw_quarter["m7"] = np.where(raw_quarter["roavol"] < raw_quarter["md_roavol"], 1, 0)
raw_quarter["m8"] = np.where(raw_quarter["sgrvol"] < raw_quarter["md_sgrvol"], 1, 0)

# raw_quarter["m7"] = np.select([raw_quarter["roavol"] < raw_quarter["md_roavol"], raw_quarter["roavol"] >= raw_quarter["md_roavol"]],
#                               [1, 0], default = np.nan)
# raw_quarter["m8"] = np.select([raw_quarter["sgrvol"] < raw_quarter["md_sgrvol"], raw_quarter["sgrvol"] >= raw_quarter["md_sgrvol"]],
#                               [1, 0], default = np.nan)

# Calculate final value
raw_quarter["ms"] = raw_quarter["m1"] + raw_quarter["m2"] + raw_quarter["m3"] + raw_quarter["m4"] +                     raw_quarter["m5"] + raw_quarter["m6"] + raw_quarter["m7"] + raw_quarter["m8"]

# raw_quarter["ms"] = np.where((raw_quarter["m1"].notnull() & raw_quarter["m2"].notnull() & raw_quarter["m3"].notnull() &
#                               raw_quarter["m4"].notnull() & raw_quarter["m5"].notnull() & raw_quarter["m6"].notnull() & 
#                               raw_quarter["m7"].notnull() & raw_quarter["m8"].notnull()), 
#                              raw_quarter["m1"] + raw_quarter["m2"] + raw_quarter["m3"] + raw_quarter["m4"] + 
#                              raw_quarter["m5"] + raw_quarter["m6"] + raw_quarter["m7"] + raw_quarter["m8"], np.nan)

# Query data from WRDS
conn = wrds.Connection(wrds_username=wrds_username)

comp = conn.raw_sql("""
                    select gvkey, datadate, rdq
                    from comp.fundq
                    where indfmt = 'INDL' 
                    and datafmt = 'STD'
                    and popsrc = 'D'
                    and consol = 'C'
                    and datadate >= '01/01/%(start_year)s'
                    """, params=parm)

comp['datadate'] = pd.to_datetime(comp['datadate'])

ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where linktype in ('LU', 'LC')
                  """)

ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])

# if linkenddt is missing then set to today date
ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])
# extract month and year of rdq
ccm1['rdq'] = pd.to_datetime(ccm1['rdq'])

# set link date bounds
ccm2 = ccm1[(ccm1['datadate'] >= ccm1['linkdt']) & (ccm1['datadate'] <= ccm1['linkenddt'])]
ccm2 = ccm2[['gvkey', 'datadate', 'rdq', 'permno']]

# Report Date of Quarterly Earnings (rdq) may not be trading day, we need to get the first trading day on or after rdq
crsp_dsi = conn.raw_sql("""
                        select distinct date
                        from crsp.dsi
                        where date >= '01/01/%(start_year)s'
                        """, params=parm)

crsp_dsi['date'] = pd.to_datetime(crsp_dsi['date'])

ccm3 = ccm2.copy()
for i in range(6):  # we only consider the condition that the day after rdq is not a trading day, which is up to 5 days
    ccm3['trad_%s' % i] = ccm3['rdq'] + pd.DateOffset(days=i)  # set rdq + i days to match trading day
    crsp_dsi['trad_%s' % i] = crsp_dsi['date']  # set the merging key
    crsp_dsi = crsp_dsi[['date', 'trad_%s' % i]]  # reset trading day columns to avoid repeat merge
    ccm3 = pd.merge(ccm3, crsp_dsi, how='left', on='trad_%s' % i)
    ccm3['trad_%s' % i] = ccm3['date']  # reset rdq + i days to matched trading day
    ccm3 = ccm3.drop(['date'], axis=1)

# fill NA from rdq + 5 days to rdq + 0 days, then get trading day version of rdq
for i in range(5, 0, -1):
    count = i-1
    ccm3['trad_%s' % count] = np.where(ccm3['trad_%s' % count].isnull(), ccm3['trad_%s' % i], ccm3['trad_%s' % count])

ccm3['rdq_trad'] = ccm3['trad_0']
ccm3['permno'] = ccm3['permno'].astype('int')
ccm3 = ccm3[['gvkey', 'permno', 'datadate','rdq', 'rdq_trad']]
ccm3.reset_index(drop=True).sort_values(["permno", "datadate"])

crsp_d = conn.raw_sql("""
                      select a.ret, a.vol, a.permno, a.date,
                      b.shrcd, b.exchcd
                      from crsp.dsf as a
                      left join crsp.dsenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date >= '01/01/%(start_year)s'
                      and b.exchcd between 1 and 3
                      and b.shrcd in (10,11)
                      """, params=parm)

crsp_d["rdq_trad"] = crsp_d["date"]
crsp_d["rdq_trad"] = pd.to_datetime(crsp_d["rdq_trad"])
crsp_d.reset_index(drop=True).sort_values(["permno", "date"])

combined_ccm3_crsp_d = pd.merge(crsp_d, ccm3, how="left", on=["permno", "rdq_trad"])

combined_ccm3_crsp_d['ret_l1'] = combined_ccm3_crsp_d['ret'].shift(1)
combined_ccm3_crsp_d['ret_l-1'] = combined_ccm3_crsp_d['ret'].shift(-1)
combined_ccm3_crsp_d['ear'] = combined_ccm3_crsp_d['ret_l-1'] + combined_ccm3_crsp_d['ret'] + combined_ccm3_crsp_d['ret_l1']

combined_ccm3_crsp_d['vol_l1'] = combined_ccm3_crsp_d['vol'].shift(1)
combined_ccm3_crsp_d['vol_l-1'] = combined_ccm3_crsp_d['vol'].shift(-1)
combined_ccm3_crsp_d['vol_l10'] = combined_ccm3_crsp_d['vol'].shift(10)
combined_ccm3_crsp_d['vol_1mth_average'] = combined_ccm3_crsp_d['vol_l10'].rolling(20).mean()
combined_ccm3_crsp_d['3d_vol_avg'] = (combined_ccm3_crsp_d['vol_l-1'] + combined_ccm3_crsp_d['vol'] + combined_ccm3_crsp_d['vol_l1']) / 3
combined_ccm3_crsp_d['aeavol'] = (combined_ccm3_crsp_d['3d_vol_avg'] - combined_ccm3_crsp_d['vol_1mth_average']) / combined_ccm3_crsp_d['vol_1mth_average']

combined_ccm3_crsp_d = combined_ccm3_crsp_d.dropna(subset=['rdq'])
df_ear_aeavol = combined_ccm3_crsp_d[['date','datadate','rdq_trad','rdq','permno','ret','vol','ear','aeavol']]
df_ear_aeavol['permno']= df_ear_aeavol['permno'].astype('int')
df_ear_aeavol.sort_values(["permno", "datadate"])

raw_quarter = pd.merge(raw_quarter, df_ear_aeavol, how="left", on=["permno", "datadate"])

raw_quarter = raw_quarter.drop_duplicates(subset=['permno', 'datadate'])

# Save to csv
# raw_quarter[["permno", "datadate", "cash", "chtx", "roaq", "roeq", "rsup", 
#              "cinvest", "nincr", "roavol", "stdacc", "stdcf", "ms", "ear", "aeavol"]].to_csv("../result/quarter_features.csv", index = False)

# print("quarter_features.csv saved to result folder")


# In[5]:


#monthly feature generation

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

# Create result directory
# if not os.path.isdir('../result/'):
#     os.mkdir("../result/")

# Query data from WRDS
conn = wrds.Connection(wrds_username=wrds_username)

# crsp_msf = conn.raw_sql("""
#                         /*identifier*/
#                         select permno, cusip,
#                         /*variables of interest*/
#                         date, ret, retx, prc, shrout, vol                      
#                         from crsp.msf 
#                         where date >= '01/01/%(start_year)s'
#                         and date <= '12/31/%(end_year)s'
#                         """, params=parm)

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

# Add delisted returns
crsp_dlret = conn.raw_sql("""
                          select permno, dlret, dlstdt 
                          from crsp.msedelist
                          """)


crsp_msf["permno"] = crsp_msf["permno"].astype("Int64")
crsp_msf["date"] = pd.to_datetime(crsp_msf["date"])
crsp_msf["date_std"] = crsp_msf["date"] + MonthEnd(0)

crsp_dlret["permno"] = crsp_dlret["permno"].astype("Int64")
crsp_dlret["dlstdt"] = pd.to_datetime(crsp_dlret["dlstdt"])
crsp_dlret["date_std"] = crsp_dlret["dlstdt"] + MonthEnd(0)

# Merge CRSP msf and dlret files
raw_month = pd.merge(crsp_msf, crsp_dlret, how="left", on=["permno", "date_std"])
raw_month.sort_values(["permno", "date"], inplace=True)

# Calculate total return
raw_month["dlret"] = raw_month["dlret"].fillna(0)
raw_month["ret"] = raw_month["ret"].fillna(0)
raw_month["total_ret"] = (1 + raw_month["ret"]) * (1 + raw_month["dlret"]) - 1

# Set link date bounds
comp_crsp_link = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """)

comp_crsp_link.drop(comp_crsp_link[comp_crsp_link.linktype == "LD"].index, inplace=True)

comp_crsp_link['linkdt'] = pd.to_datetime(comp_crsp_link['linkdt'])
comp_crsp_link['linkenddt'] = pd.to_datetime(comp_crsp_link['linkenddt'])
comp_crsp_link['linkenddt'] = comp_crsp_link['linkenddt'].fillna(pd.to_datetime('today'))

comp_crsp_link["permno"] = comp_crsp_link["permno"].astype("Int64")
raw_month = raw_month.merge(comp_crsp_link[["permno", "linkdt", "linkenddt"]], how="left", on=["permno"])

raw_month = raw_month[(raw_month['date'] >= raw_month['linkdt']) & (raw_month['date'] <= raw_month['linkenddt'])]
raw_month = raw_month.drop(['linkdt', 'linkenddt'], axis=1)

# Calculate lagged returns
for i in range(36):
    raw_month["ret_l" + str(i+1)] = raw_month.groupby(["permno"])["ret"].shift(i+1)

# Calculate features

# chmom
chmom_1 = 1
for i in range(1, 7):
    chmom_1 = chmom_1 * (1 + raw_month["ret_l" + str(i)])
chmom_2 = 1
for i in range(7, 13):
    chmom_2 = chmom_2 * (1 + raw_month["ret_l" + str(i)])
raw_month["chmom"] = (chmom_1 - 1) - (chmom_2 - 1)

# mom1m
raw_month["mom1m"] = raw_month["ret_l1"]

# mom6m
mom6m_list = 1
for i in range(2, 7):
    mom6m_list = mom6m_list * (1 + raw_month["ret_l" + str(i)])
raw_month["mom6m"] = mom6m_list - 1

# mom12m
mom12m_list = 1
for i in range(2, 13):
    mom12m_list = mom12m_list * (1 + raw_month["ret_l" + str(i)])
raw_month["mom12m"] = mom12m_list - 1

# mom36m
mom36m_list = 1
for i in range(13, 37):
    mom36m_list = mom36m_list * (1 + raw_month["ret_l" + str(i)])
raw_month["mom36m"] = mom36m_list - 1

# turn
raw_month["vol_l1"] = raw_month.groupby(["permno"])["vol"].shift(1)
raw_month["vol_l2"] = raw_month.groupby(["permno"])["vol"].shift(2)
raw_month["vol_l3"] = raw_month.groupby(["permno"])["vol"].shift(3)
raw_month["turn"] = raw_month[["vol_l1", "vol_l2", "vol_l3"]].mean(axis=1) / raw_month["shrout"]

# dolvol
raw_month["prc_l2"] = raw_month.groupby(["permno"])["prc"].shift(2)
raw_month["dolvol"] = np.log(raw_month["vol_l2"] * raw_month["prc_l2"]).replace([np.inf, -np.inf], np.nan)

# indmom
sic2_link = conn.raw_sql("""
                        select cp.gvkey, substr(cp.sic,1,2) as sic2, cr.lpermno as permno
                        from comp.company as cp left join crsp.ccmxpf_linktable as cr on cp.gvkey = cr.gvkey
                        where substr(cr.linktype,1,1)='L' and (cr.linkprim ='C' or cr.linkprim='P')
                        """)

conn.close()

sic2_link["permno"] = sic2_link["permno"].astype("Int64")
sic2_link.drop_duplicates(subset=["permno"], inplace=True)
raw_month = pd.merge(raw_month, sic2_link[["permno", "sic2"]], how="left", on=["permno"])
df_temp = raw_month.groupby(["date_std", "sic2"], as_index=False)["mom12m"].mean()
df_temp = df_temp.rename(columns={"mom12m": "indmom"})
raw_month = pd.merge(raw_month, df_temp, how="left", on=["date_std", "sic2"])

# mvel1 - note: Green's paper uses mve, Gu's paper uses mvel1. mvel1 appears to not use log, while mve does
raw_month["prc_l1"] = raw_month.groupby(["permno"])["prc"].shift(1)
raw_month["shrout_l1"] = raw_month.groupby(["permno"])["shrout"].shift(1)
# raw_month["mvel1"] = np.log(abs(raw_month["prc_l1"] * raw_month["shrout_l1"])).replace([np.inf, -np.inf], np.nan)
raw_month["mvel1"] = abs(raw_month["prc_l1"] * raw_month["shrout_l1"])

# placeholder for beta/betasq/pricedelay

raw_month["beta"] = ""
raw_month["betasq"] = ""
raw_month["pricedelay"] = ""

# Save to csv
# raw_month[["permno", "date", "date_std", "ret", "total_ret", "chmom", "mom1m", "mom6m", "mom12m", "mom36m", 
#            "turn", "dolvol", "indmom", "mvel1", "shrout", "prc", "beta", "betasq", "pricedelay"]].to_csv("../result/month_features.csv", index = False)

# print("month_features.csv saved to result folder")


# In[6]:


#daily feature generation

import wrds
import pandas as pd
import numpy as np
import os
import json
from pandas.tseries.offsets import *
from datetime import datetime

# Load config file
with open('config.json') as config_file:
    data = json.load(config_file)

start_year = data['start_year']
start_year_daily_data = data['start_year_daily_data']
end_year = data['end_year']
wrds_username = data['wrds_username']

parm = {"start_year": start_year_daily_data, "end_year": end_year}

# Create result directory
# if not os.path.isdir('../result/'):
#     os.mkdir("../result/")

# Query data from WRDS
conn = wrds.Connection(wrds_username=wrds_username)

crsp_dsf = conn.raw_sql("""
                          /*identifier*/
                          select permno, permco, cusip,
                          /*variables of interest*/
                          vol,ret,askhi,bidlo,prc,shrout,date
                          from crsp.dsf 
                          where date >= '01/01/%(start_year)s'
                          and date <= '12/31/%(end_year)s'
                          """, params=parm)

conn.close()

crsp_dsf["permno"] = crsp_dsf["permno"].astype("Int64")
crsp_dsf["date"] = pd.to_datetime(crsp_dsf["date"])
crsp_dsf.sort_values(['permno', 'date'], inplace=True)

crsp_dsf1 = crsp_dsf.copy()

crsp_dsf1["yyyy-mm"] = crsp_dsf1["date"].dt.to_period('M')

# baspread
crsp_dsf1["baspread"] = (crsp_dsf1["askhi"] - crsp_dsf1["bidlo"]) / ((crsp_dsf1["askhi"] + crsp_dsf1["bidlo"]) / 2)

# ill
crsp_dsf1["ill"] = abs(crsp_dsf1["ret"]) / (abs(crsp_dsf1["prc"]) * crsp_dsf1["vol"])

daily_features = crsp_dsf1.groupby(["permno", "yyyy-mm"], as_index=False)[["baspread", "ill"]].mean()

# maxret
temp_maxret = crsp_dsf1.groupby(["permno", "yyyy-mm"], as_index=False)["ret"].max()
temp_maxret = temp_maxret.rename(columns={"ret": "maxret"})
daily_features = pd.merge(daily_features, temp_maxret, how="left", on=["permno", "yyyy-mm"])

# std_dolvol
crsp_dsf1["std_dolvol"] = np.log(abs(crsp_dsf1["prc"] * crsp_dsf1["vol"])).replace([np.inf, -np.inf], np.nan)

# std_turn
crsp_dsf1["std_turn"] = crsp_dsf1["vol"] / crsp_dsf1["shrout"]

# retvol
temp_std = crsp_dsf1.groupby(["permno", "yyyy-mm"], as_index=False)[["ret", "std_dolvol", "std_turn"]].std()
temp_std = temp_std.rename(columns={"ret": "retvol"})
daily_features = pd.merge(daily_features, temp_std, how="left", on=["permno", "yyyy-mm"])

# zerotrade
crsp_dsf1["countzero"] = np.where(crsp_dsf1["vol"] == 0, 1, 0)
crsp_dsf1["ndays"] = 1
crsp_dsf1["turn_alt"] = crsp_dsf1["vol"] / crsp_dsf1["shrout"]
temp_zerotrade = crsp_dsf1.groupby(["permno", "yyyy-mm"], as_index=False)[["countzero", "ndays", "turn_alt"]].sum()
temp_zerotrade["zerotrade"] = (temp_zerotrade["countzero"] + ((1 / temp_zerotrade["turn_alt"]) / 480000)) * 21 / temp_zerotrade["ndays"]
daily_features = pd.merge(daily_features, temp_zerotrade[["permno", "yyyy-mm", "zerotrade"]], how="left", on=["permno", "yyyy-mm"])

#idiovol

# Query data from WRDS
conn = wrds.Connection(wrds_username=wrds_username)

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.dsedelist
                     where dlstdt >= '01/01/%(start_year)s'
                     """, params=parm)

crspsp500d = conn.raw_sql("""
                          select date, sprtrn 
                          from crsp.dsi
                          where date >= '01/01/%(start_year)s'
                          """, params=parm)

conn.close()

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])

dlret.rename(columns={"dlstdt": "date"}, inplace=True)

crsp_dsf = pd.merge(crsp_dsf, dlret, how='left', on=['permno', 'date'])

# return adjusted for delisting
crsp_dsf['retadj'] = np.where(crsp_dsf['dlret'].notna(), (crsp_dsf['ret'] + 1)*(crsp_dsf['dlret'] + 1) - 1, crsp_dsf['ret'])
crsp_dsf = crsp_dsf.sort_values(by=['date', 'permno'])

# sprtrn

crspsp500d['date'] = pd.to_datetime(crspsp500d['date'])

# abnormal return
crsp_dsf = pd.merge(crsp_dsf, crspsp500d, how='left', on='date')
crsp_dsf['abrd'] = crsp_dsf['retadj'] - crsp_dsf['sprtrn']
crsp_dsf = crsp_dsf[['date', 'permno', 'ret', 'retadj', 'sprtrn', 'abrd']]

crsp_dsf['weekly_moving_abrd_sum'] = crsp_dsf['abrd'].rolling(window=5).sum()
crsp_dsf['weekly_moving_abrd_std'] = crsp_dsf['weekly_moving_abrd_sum'].rolling(156).std()

crsp_dsf["yyyy-mm"] = crsp_dsf["date"].dt.to_period('M')

df_idiovol = crsp_dsf.groupby(["permno", "yyyy-mm"], as_index=False)[["weekly_moving_abrd_std"]].mean()
df_idiovol.rename(columns={"weekly_moving_abrd_std": "idiovol"}, inplace=True)

daily_features = pd.merge(daily_features, df_idiovol, how="left", on=["permno", "yyyy-mm"])
daily_features.sort_values(by=['permno','yyyy-mm'])

# Save to csv
# daily_features.to_csv("../result/daily_features.csv", index = False)

# print("daily_features.csv saved to result folder")


# In[7]:


#daily_features['yyyy-mm'].to_timestamp(freq='M')

#daily_features['yyyy-mm'] = daily_features['yyyy-mm'].dt.to_timestamp(freq='M')


# In[15]:


import wrds
import pandas as pd
import numpy as np
import json
from pandas.tseries.offsets import *
import datetime as dt

with open('config.json') as config_file:
    data = json.load(config_file)

start_year = data['start_year']
start_year_sp500_companies = data['start_year_sp500_companies']
end_year = data['end_year']
annual_lag = data['annual_lag']
quarterly_lag = data['quarterly_lag']
wrds_username = data['wrds_username']
sp500_companies_subset = data['S&P500_companies_only']

parm = {"start_year": start_year, "end_year": end_year}

parm_sp500 = {"start_year_sp500_companies": start_year_sp500_companies, "end_year": end_year}

conn = wrds.Connection(wrds_username=wrds_username)

# annual = pd.read_csv("../result/annual_features.csv")
# quarter = pd.read_csv("../result/quarter_features.csv")
# month = pd.read_csv("../result/month_features.csv")
# daily = pd.read_csv("../result/daily_features.csv")

annual = raw_annual
quarter = raw_quarter
month = raw_month
daily = daily_features

# Add ticker name

crsp_stocknames = conn.raw_sql("""
                    select * from crsp.stocknames 
                    """)

# Match ticker code to permno
crsp_stocknames.sort_values(["permno", "nameenddt"], inplace=True)
crsp_stocknames.drop_duplicates(subset=["permno"], keep="last", inplace=True)
crsp_stocknames["permno"] = crsp_stocknames["permno"].astype("Int64")
month = pd.merge(month, crsp_stocknames[["ticker", "permno"]], how="left", on=["permno"])

# Retrieve S&P500 constituent companies

sp500 = conn.raw_sql("""
                        select *
                        from crsp.msp500list
                        where ending >='01/01/%(start_year_sp500_companies)s'
						and start <= '12/31/%(end_year)s';
                        """, params=parm_sp500)

conn.close()

sp500["permno"] = sp500["permno"].astype("Int64")
sp500 = sp500["permno"].tolist()

# Add data lag
annual["datadate"] = pd.to_datetime(annual["datadate"])
annual["date_std"] = annual["datadate"] + MonthEnd(0)
annual["date_std"] = annual["date_std"] + MonthEnd(annual_lag + 1)

quarter["datadate"] = pd.to_datetime(quarter["datadate"])
quarter["date_std"] = quarter["datadate"] + MonthEnd(0)
quarter["date_std"] = quarter["date_std"] + MonthEnd(quarterly_lag + 1)

month["date"] = pd.to_datetime(month["date"])
month["date_std"] = pd.to_datetime(month["date_std"])

#daily["yyyy-mm"] = pd.to_datetime(daily["yyyy-mm"])
daily_features['yyyy-mm'] = daily_features['yyyy-mm'].dt.to_timestamp(freq='M')
daily["date_std"] = daily["yyyy-mm"] + MonthEnd(0)
daily["date_std"] = daily["date_std"] + MonthEnd(1)

# Merge month and daily files
combined = pd.merge(month, daily, how="left", on=["permno", "date_std"])
combined = combined.drop(["yyyy-mm"], axis=1)

# Merge quarterly files
combined = pd.merge(combined, quarter, how="left", on=["permno", "date_std"])
combined = combined.drop(["datadate"], axis=1)

# Merge annual files
combined = pd.merge(combined, annual, how="left", on=["permno", "date_std"])
combined = combined.drop(["datadate"], axis=1)

combined = combined.drop(["date_std"], axis=1)

# Fill quarterly values
quarter_cols = ["cash", "chtx", "roaq", "roeq", "rsup", "cinvest", "nincr", "roavol", "stdacc", "stdcf", "ms"]
combined[quarter_cols] = combined[quarter_cols].ffill(axis=0, limit=3)


# In[9]:


# Fill annual values
annual_cols = ["absacc", "acc", "age", "agr", "bm", "bm_ia", "cashdebt", "cashpr", "cfp", "cfp_ia", "chatoia", 
			   "chcsho", "chempia", "chinv", "chpmia", "convind", "currat", "depr", "divi", "divo", "dy", "egr", "ep", "gma", 
			   "grcapx", "grltnoa", "herf", "hire", "invest", "lev", "lgr", "mve_ia", "operprof", "orgcap", "pchcapx_ia", 
			   "pchcurrat", "pchdepr", "pchgm_pchsale", "pchquick", "pchsale_pchinvt", "pchsale_pchrect", "pchsale_pchxsga", 
			   "pchsaleinv", "pctacc", "ps", "quick", "rd", "rd_mve", "rd_sale", "realestate", "roic", "salecash", "saleinv", 
			   "salerec", "secured", "securedind", "sgr", "sin", "sp", "tang", "tb"]
combined[annual_cols] = combined[annual_cols].ffill(axis=0, limit=12)


# In[ ]:


# Save to csv (all companies)
if sp500_companies_subset == "Yes":
	sp500_subset = combined[combined["permno"].isin(sp500)]
	sp500_subset.to_csv("../result/all_features_sp500_companies.csv", index=False)
	print("all_features_sp500_companies.csv saved to result folder")
else:
	combined.to_csv("../result/all_features_all_companies.csv", index=False)
	print("all_features_all_companies.csv saved to result folder")


# In[ ]:




