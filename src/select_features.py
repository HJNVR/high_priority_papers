# select features
# ============================================================================

import numpy as np
import pandas as pd
import json
import time

start = time.time()
with open('config.json') as config_file:
    data = json.load(config_file)
stock_pool = data['stock_pool']

if stock_pool == 'sp500':
    print('Start selelecting sp500 features ...')
    consolidated_features = pd.read_csv(
        '../result/sp500/consolidated/consolidated_features.csv')

    selected_features = consolidated_features[[
        'date', 'permno', 'ticker'] + data["selected_feature_columns"]]
    selected_features.to_csv(
        '../result/sp500/selected_features/selected_features.csv')

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(
    int(hours), int(minutes), seconds))
print('Please check /result/' + stock_pool +
      '/selected_features/selected_features.csv')
