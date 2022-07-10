# select features
# ============================================================================

import numpy as np
import pandas as pd
import json
import time

start = time.time()
print('Start selelecting features ...')
consolidated_features = pd.read_csv('../result/consolidated/consolidated_features.csv')

with open('config.json') as config_file:
    data = json.load(config_file)

selected_features = consolidated_features[data["selected_feature_columns"]]
selected_features.to_csv('../result/selected_features/selected_features.csv')
end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds))
