# combine paper1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 into one dataframe
# ============================================================================

import numpy as np
import pandas as pd
import tqdm
import os

con_final = pd.DataFrame()
count = 0

for i in tqdm.tqdm(range(len(os.listdir('../result/consolidated_papers'))),
                            bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
    filename = os.listdir('../result/consolidated_papers')[i]
    if '.csv' in filename:
        df = pd.read_csv('../result/consolidated_papers/'+filename, index_col=[0])

        if count == 0:
            con_final = df
            count += 1
        else:
            con_final = pd.concat([con_final, df], ignore_index=True)

con_final.to_csv('../result/consolidated_final/consolidated_final.csv', index=False)