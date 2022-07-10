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

#final['ticker'] = final['ticker_x'].values
#print(final)
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
        if 'ticker' not in final.columns:
            try:
                final['ticker'] = final[col].values
            except:
                final['ticker'] = final[col].values[:, 0]
        else:
            drop_list.append(col)
    if 'date' in col:
        drop_list.append(col)

final = final.drop(columns=drop_list)
final.insert(1, 'permno', final.pop('permno'))
final.insert(2, 'ticker', final.pop('ticker'))
final.to_csv('../result/consolidated/consolidated_features.csv')

with open(r'consolidated_feature_names.txt', 'w') as fp:
    fp.write('\n'.join(final.columns))

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
