# Feature generation and pre-processing of paper 10
# current issues:
#1.  Dividend: cannot find the reinvested dividends
#2.  Portfolio price: cannot find the reinvested price
#3.  d-p: based on 1. and 2.
#4.  factor: link does not work, cannot find in the source paper
#5.  CAY: same issue as 4.
#6.  DRI-Global Insight : datasets not exist
# ============================================================================

import numpy as np
import pandas as pd
from fredapi import Fred

# define start and end date
start_date = "2000-01-01"
end_date = "2020-12-31"

# Mkt-RF, SMB, HML, RF
df = pd.read_csv('F-F_Research_Data_Factors.csv')
df['Date'] = pd.to_datetime(df['Date'], format='%Y%m')
df = df.set_index('Date')
df = df.drop(df[df.index < start_date].index)
df = df.drop(df[df.index > end_date].index)

# r15-r11
df1 = pd.read_csv('6_Portfolios_2x3.csv')
df1['Date'] = pd.to_datetime(df1['Date'], format='%Y%m')
df1 = df1.set_index('Date')
df1 = df1.drop(df1[df1.index < start_date].index)
df1 = df1.drop(df1[df1.index > end_date].index)
df['r15-r11'] = df1['SMALL HiBM'] - df1['SMALL LoBM']

# UMD
df2 = pd.read_csv('10_Portfolios_Prior_12_2.csv')
df2['Date'] = pd.to_datetime(df2['Date'], format='%Y%m')
df2 = df2.set_index('Date')
df2 = df2.drop(df2[df2.index < start_date].index)
df2 = df2.drop(df2[df2.index > end_date].index)
df['umd'] = df2['Hi PRIOR'] - df2['Lo PRIOR']

# 44 industries features
df3 = pd.read_csv('48_Industry_Portfolios.csv')
df3['Date'] = pd.to_datetime(df3['Date'], format='%Y%m')
df3 = df3.set_index('Date')
df3 = df3.drop(df3[df3.index < start_date].index)
df3 = df3.drop(df3[df3.index > end_date].index)
df3 = df3.drop(['Hlth ', 'FabPr', 'Guns ', 'Gold '], axis=1)
df = df.join(df3, on='Date')

# 93 size/BM features
df4 = pd.read_csv('100_Portfolios_10x10.csv')
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

p_e = pd.read_excel('PE.xlsx')
p_e.set_index('Date', inplace=True)
p_e = p_e.squeeze()

df = df.join(yield10y)
df = df.join(yield1y)
df = df.join(trm10y_1y)
df = df.join(aaa)
df = df.join(baa)
df = df.join(DEF)
df = df.join(rrel)
df = df.join(trm10y_3m)
df = df.join(p_e)

df.to_csv('../result/paper10/common_features.csv')
