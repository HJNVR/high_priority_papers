# Feature generation and pre-processing of paper 6
# ============================================================================

import numpy as np
import pandas as pd
import yfinance as yf
import datetime
import pandas_datareader
import wrds
import json

# function to calculate all features


def cal_export(df):
    df = calculate_ma(df)
    df = calculate_momentum(df)
    df = calculate_d(df)
    df = calculate_maobv(df)
    df = ma_1_9(df)
    df = ma_2_9(df)
    df = ma_3_9(df)
    df = ma_1_12(df)
    df = ma_2_12(df)
    df = ma_3_12(df)
    df = mom9(df)
    df = mom12(df)
    df = vol_1_9(df)
    df = vol_2_9(df)
    df = vol_3_9(df)
    df = vol_1_12(df)
    df = vol_2_12(df)
    df = vol_3_12(df)
    df = cleanup(df)
    return df

# functions to calculate stock specific features

# Calculate MA


def calculate_ma(df):
    df['ma1'] = df['prc'].rolling(1, min_periods=1).mean()
    df['ma2'] = df['prc'].rolling(2, min_periods=1).mean()
    df['ma3'] = df['prc'].rolling(3, min_periods=1).mean()
    df['ma9'] = df['prc'].rolling(9, min_periods=1).mean()
    df['ma12'] = df['prc'].rolling(12, min_periods=1).mean()
    return df

# Calculate Momentum


def calculate_momentum(df):
    df['Close_l1'] = df['prc'].shift(1)
    df['Close_l9'] = df['prc'].shift(9)
    df['Close_l12'] = df['prc'].shift(12)
    return df

# Calculate D


def calculate_d(df):
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


def calculate_maobv(df):
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
# 1 MA(1,9)


def ma_1_9(df):
    ma_1_9 = []
    for i in range(len(df['prc'])):
        if df['ma1'][i] >= df['ma9'][i]:
            ma_1_9.append(1)
        else:
            ma_1_9.append(0)
    df['ma_1_9'] = ma_1_9
    return df

# 2 MA(1,12)


def ma_1_12(df):
    ma_1_12 = []
    for i in range(len(df['prc'])):
        if df['ma1'][i] >= df['ma12'][i]:
            ma_1_12.append(1)
        else:
            ma_1_12.append(0)
    df['ma_1_12'] = ma_1_12
    return df

# 3 MA(2,9)


def ma_2_9(df):
    ma_2_9 = []
    for i in range(len(df['prc'])):
        if df['ma2'][i] >= df['ma9'][i]:
            ma_2_9.append(1)
        else:
            ma_2_9.append(0)
    df['ma_2_9'] = ma_2_9
    return df

# 4 MA(2,12)


def ma_2_12(df):
    ma_2_12 = []
    for i in range(len(df['prc'])):
        if df['ma2'][i] >= df['ma12'][i]:
            ma_2_12.append(1)
        else:
            ma_2_12.append(0)
    df['ma_2_12'] = ma_2_12
    return df

# 5 MA(3,9)


def ma_3_9(df):
    ma_3_9 = []
    for i in range(len(df['prc'])):
        if df['ma3'][i] >= df['ma9'][i]:
            ma_3_9.append(1)
        else:
            ma_3_9.append(0)
    df['ma_3_9'] = ma_3_9
    return df

# 6 MA(3,12)


def ma_3_12(df):
    ma_3_12 = []
    for i in range(len(df['prc'])):
        if df['ma3'][i] >= df['ma12'][i]:
            ma_3_12.append(1)
        else:
            ma_3_12.append(0)
    df['ma_3_12'] = ma_3_12
    return df

# 7 MOM9


def mom9(df):
    mom9 = []
    for i in range(len(df['prc'])):
        if df['prc'][i] >= df['Close_l9'][i]:
            mom9.append(1)
        else:
            mom9.append(0)
    df['mom9'] = mom9
    return df

# 8 MOM12


def mom12(df):
    mom12 = []
    for i in range(len(df['prc'])):
        if df['prc'][i] >= df['Close_l12'][i]:
            mom12.append(1)
        else:
            mom12.append(0)
    df['mom12'] = mom12
    return df

# 9 VOL(1,9)


def vol_1_9(df):
    vol_1_9 = []
    for i in range(len(df['prc'])):
        if df['maobv1'][i] >= df['maobv9'][i]:
            vol_1_9.append(1)
        else:
            vol_1_9.append(0)
    df['vol_1_9'] = vol_1_9
    return df

# 10 VOL(1,12)


def vol_1_12(df):
    vol_1_12 = []
    for i in range(len(df['prc'])):
        if df['maobv1'][i] >= df['maobv12'][i]:
            vol_1_12.append(1)
        else:
            vol_1_12.append(0)
    df['vol_1_12'] = vol_1_12
    return df

# 11 VOL(2,9)


def vol_2_9(df):
    vol_2_9 = []
    for i in range(len(df['prc'])):
        if df['maobv2'][i] >= df['maobv9'][i]:
            vol_2_9.append(1)
        else:
            vol_2_9.append(0)
    df['vol_2_9'] = vol_2_9
    return df

# 12 VOL(2,12)


def vol_2_12(df):
    vol_2_12 = []
    for i in range(len(df['prc'])):
        if df['maobv2'][i] >= df['maobv12'][i]:
            vol_2_12.append(1)
        else:
            vol_2_12.append(0)
    df['vol_2_12'] = vol_2_12
    return df

# 13 VOL(3,9)


def vol_3_9(df):
    vol_3_9 = []
    for i in range(len(df['prc'])):
        if df['maobv3'][i] >= df['maobv9'][i]:
            vol_3_9.append(1)
        else:
            vol_3_9.append(0)
    df['vol_3_9'] = vol_3_9
    return df

# 14 VOL(3,12)


def vol_3_12(df):
    vol_3_12 = []
    for i in range(len(df['prc'])):
        if df['maobv3'][i] >= df['maobv12'][i]:
            vol_3_12.append(1)
        else:
            vol_3_12.append(0)
    df['vol_3_12'] = vol_3_12
    return df

# clean-up unnecessary columns


def cleanup(df):
    return df.drop(['prc', 'vol',
                    'ma1', 'ma2', 'ma3', 'ma9', 'ma12',
                    'Close_l1', 'Close_l9', 'Close_l12',
                   'd', 'obv', 'maobv1', 'maobv2', 'maobv3', 'maobv9', 'maobv12'], axis=1)


if __name__ == "__main__":
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
    stocks = pd.read_csv('S&P500 companies list (2000 to 2020).csv')
    permnos = stocks['permno'].values

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
            cal_export(v).to_csv('../result/paper6/'+str(k)+'.csv')
