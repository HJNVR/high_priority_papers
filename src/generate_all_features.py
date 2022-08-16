# Feature generation and pre-processing of paper1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
# Paper 1, 8 features are dups
# Execution complete in 11 hours
# ============================================================================

from paper11 import Paper11
from paper10 import Paper10
from paper9 import Paper9
from paper7 import Paper7
from paper6 import Paper6
from paper5 import Paper5
from paper4 import Paper4
from paper3 import Paper3
from paper2 import Paper2
from paper1 import Paper1
import numpy as np
import pandas as pd
import time
import json
import sys
import warnings
warnings.filterwarnings("ignore")


start = time.time()

with open('config.json') as config_file:
    data = json.load(config_file)
stock_pool = data['stock_pool']

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
        pass  # avoid delisted
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

# clean features
print('Cleaning features ... ')

if stock_pool == 'sp500':
    final.to_csv('../result/sp500/consolidated/consolidated_features.csv')
    final = pd.read_csv(
        '../result/sp500/consolidated/consolidated_features.csv', index_col=0)
    # fix the first three columns to be date, permno, ticker
    final.insert(1, 'permno', final.pop('permno_x'))
    final.insert(2, 'ticker', final.pop('ticker'))

    drop_list = []  # list of columns to be dropped
    drop_list += ['close_y', 'hsi_y', 'sse_y', 'fchi_y', 'ftse_y', 'gdaxi_y', 'dji_y', 'ixic_y', 'aapl_y',
                  'msft_y', 'xom_y', 'ge_y', 'jnj_y', 'wfc_y', 'amzn_y', 'jpm_y', 'oil_y', 'oilpricex_y',
                  'gold_y', 'ctb3m_y', 'ctb6m_y', 'ctb1y_y', 'aaa_y', 'baa_y', 'te1_y', 'te2_y', 'te3_y',
                  'te5_y', 'te6_y', 'de1_y', 'de2_y', 'de4_y', 'de5_y', 'de6_y', 'a2me_y', 'ato_y',
                  'beme_y', 'c_y', 'cto_y', 'pi2a_pctchange_y', 'e2p_y', 'investment_y', 'prof_y', 'ep_y',
                  'roa_y', 'aaaffm_y', 'amdmnox_y', 'amdmuox_y', 'awhman_y', 'awotman_y', 'baaffm_y',
                  'businvx_y', 'busloans_y', 'ce16ov_y', 'ces0600000007_y', 'ces0600000008_y', 'ces1021000001_y',
                  'ces2000000008_y', 'ces3000000008_y', 'claimsx_y', 'clf16ov_y', 'cmrmtsplx_y', 'compapffx_y',
                  'conspi_y', 'cp3mx_y', 'cpiappsl_y', 'cpiaucsl_y', 'cpimedsl_y', 'cpitrnsl_y', 'cpiulfsl_y',
                  'cumfns_y', 'cusr0000sa0l2_y', 'cusr0000sa0l5_y', 'cusr0000sac_y', 'cusr0000sad_y',
                  'cusr0000sas_y', 'ddurrg3m086sbea_y', 'dmanemp_y', 'dndgrg3m086sbea_y', 'dpcera3m086sbea_y',
                  'dserrg3m086sbea_y', 'dtcolnvhfnm_y', 'dtcthfnm_y', 'excausx_y', 'exjpusx_y', 'exszusx_y',
                  'exusukx_y', 'fedfunds_y', 'gs1_y', 'gs10_y', 'gs5_y', 'houst_y', 'houstmw_y', 'houstne_y',
                  'housts_y', 'houstw_y', 'hwi_y', 'hwiuratio_y', 'indpro_y', 'ipb51222s_y', 'ipbuseq_y',
                  'ipcongd_y', 'ipdcongd_y', 'ipdmat_y', 'ipfinal_y', 'ipfpnss_y', 'ipfuels_y', 'ipmansics_y',
                  'ipmat_y', 'ipncongd_y', 'ipnmat_y', 'isratiox_y', 'm1sl_y', 'm2real_y', 'm2sl_y', 'manemp_y',
                  'ndmanemp_y', 'nonborres_y', 'nonrevsl_y', 'oilpricex_y', 'payems_y', 'pcepi_y', 'permit_y',
                  'permitmw_y', 'permitne_y', 'permits_y', 'permitw_y', 'ppicmm_y', 'realln_y', 'retailx_y',
                  'rpi_y', 's&p 500_y', 's&p div yield_y', 's&p pe ratio_y', 's&p: indust_y', 'srvprd_y',
                  't10yffm_y', 't1yffm_y', 't5yffm_y', 'tb3ms_y', 'tb3smffm_y', 'tb6ms_y', 'tb6smffm_y',
                  'totresns_y', 'uemp15ov_y', 'uemp15t26_y', 'uemp27ov_y', 'uemp5to14_y', 'uemplt5_y',
                  'uempmean_y', 'unrate_y', 'uscons_y', 'usfire_y', 'usgood_y', 'usgovt_y', 'ustpu_y',
                  'ustrade_y', 'uswtrade_y', 'w875rx1_y', 'oa_y', 'ol_y', 'pcm_y', 'q_y', 'r_2_1_y', 'r_12_2_y',
                  'r_12_7_y', 'r_36_13_y', 'rna_y', 's2p_y', 'sga2s_y', 'lme_y', 'lturnover_y', 'lev_y.1', 'ema10_y',
                  'ema20_y', 'ema50_y', 'ema200_y']

    for col in final.columns[3:]:
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

        # remove dup future return columns
        if 'fut_ret' in col and (col[-1] == 'x' or col[-1] == 'y'):
            drop_list.append(col)

    final = final.drop(columns=drop_list)
    # rename all columns ends with x to be original feature names
    for col in drop_list:
        if col.replace('_y', '_x') in final.columns:
            final.rename(columns={col.replace('_y', '_x'): col.replace('_y', '')}, inplace=True)

    final.to_csv('../result/sp500/consolidated/consolidated_features.csv')

    with open(r'sp500_consolidated_feature_names.txt', 'w') as fp:
        # date, permno, ticker are default columns
        for col in final.columns[3:-1]:
            fp.write(col+'\n')
        fp.write(final.columns[-1])  # last line does not need extra '\n'

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\nExecution completed in {:0>2}:{:0>2}:{:05.2f}".format(
    int(hours), int(minutes), seconds))
print('Please check /result/' + stock_pool +
      '/consolidated/consolidated_features.csv')
print('Please check /result/' + stock_pool + 'paper1/paper1_features.csv')
print('Please check /result/' + stock_pool + 'paper2/paper2_features.csv')
print('Please check /result/' + stock_pool + 'paper3/paper3_features.csv')
print('Please check /result/' + stock_pool + 'paper4/paper4_features.csv')
print('Please check /result/' + stock_pool + 'paper5/paper5_features.csv')
print('Please check /result/' + stock_pool + 'paper6/paper6_features.csv')
print('Please check /result/' + stock_pool + 'paper7/paper7_features.csv')
print('Please check /result/' + stock_pool + 'paper9/paper9_features.csv')
print('Please check /result/' + stock_pool + 'paper10/paper10_features.csv')
print('Please check /result/' + stock_pool + 'paper11/paper11_features.csv')
