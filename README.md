# Overview

This repository contains the Python code (including other related files/folders) for the generation of the features used in the following 11 selected `high priority` research papers.

# Introduction to the 11 selected high priority papers

1. Empirical asset pricing via machine learning (2020) |  
S Gu, B Kelly, D Xiu

   Summary: The paper performs a comparative analysis of machine learning methods for measuring asset risk premiums, including linear regression, generalized linear models with penalization, dimension reduction via principal components regression (PCR) and partial least squares (PLS), regression trees, and neural networks. Input features include 94 stock-level predictive characteristics from Green et al. (2017), as well as 8 macro variables from Goyal & Welch (2008).

2. A Comprehensive Look at the Empirical Performance of Equity Premium Prediction (2008) |  
I Welch, A Goyal

   Summary: The paper reexamines the performance of several variables suggested by academic literature to be good predictors of the equity premium, and finds that these models have predicted poorly both in-sample and out-of-sample (OOS) and appear unstable. Input features consist of 17 variables, including stock index characteristics and broad macroeconomic indicators.

3. Dissecting Characteristics Nonparametrically (2020) |  
J Freyberger, A Neuhierl

   Summary: The paper proposes a nonparametric method to study which characteristics provide incremental information for the cross-section of expected returns. The adaptive group LASSO is used to select characteristics and to estimate how selected characteristics affect expected returns nonparametrically. 62 firm-specific characteristics are used as inputs, which are grouped into 6 categories: past return based predictors, investment-related characteristics, profitability-related characteristics, intangibles, value-related characteristics, and trading frictions.

4. Shrinking the cross-section (2020) |  
S Kozak, S Nagel, S Santosh

   Summary: The paper constructs a robust stochastic discount factor (SDF) summarizing the joint explanatory power of a large number of cross-sectional stock return predictors. Two sets of independent characteristics are constructed, namely 50 anomaly characteristics and 70 WRDS Industry Financial Ratios (WFR).

5. FRED MD A Monthly Database for Macroeconomic Research (2016) |  
MW McCracken, S Ng

   Summary: Unlike other papers, this paper does not perform empirical stock return analysis. Instead, the paper introduces FRED-MD, a large, monthly frequency, macroeconomic database with the goal of establishing a convenient starting point for empirical analysis that requires “big data.” The publicly available dataset, comprising 134 macroeconomic variables, is designed to be updated monthly using the Federal Reserve Economic Data (FRED) database.

6. Forecasting the Equity Risk Premium - The Role of Technical Indicators (2014) |  
CJ Neely, DE Rapach, J Tu, G Zhou

   Summary: The paper compares the predictive ability of technical indicators with that of macroeconomic variables. Technical indicators were found to display statistically and economically significant in-sample and out-of-sample predictive power, matching or exceeding that of macroeconomic variables. Furthermore, technical indicators and macroeconomic variables provide complementary information over the business cycle, and combining information from both technical indicators and macroeconomic variables significantly improves equity risk premium forecasts. Input variables used include 14 macroeconomic variables from Goyal and Welch (2008), as well as 14 technical indicators.

7. Deep Learning in Asset Pricing (2019) |  
L Chen, M Pelger, J Zhu

   Summary: The paper estimates a general non-linear asset pricing model with deep neural networks for all U.S. equity data based on a substantial set of macroeconomic and firm-specific information. 46 time-varying firm-specific characteristics and 178 macroeconomic time series are used as input variables. The firm-specific variables are either from the Kenneth French Data Library and from Freyberger, Neuhierl, and Weber (2020), while macroeconomic times series are from 3 sources - 124 predictors from the FRED-MD database, the cross-sectional median time series for each of the 46 firm characteristics, and a further 8 macroeconomic predictors from Welch and Goyal (2008) not already included in the FRED-MD database.

8. The Characteristics that Provide Independent Information about Average U.S. Monthly Stock Returns (2017) |  
J Green, JRM Hand, XF Zhang

   Summary: The paper identifies the firm characteristics that provide independent information about average U.S. monthly stock returns by simultaneously including 94 characteristics in Fama-MacBeth regressions that avoid overweighting microcaps and adjust for data-snooping bias. 94 firm-specific variables from CRSP, Compustat and I/B/E/S are used as input variables.

9. Forecasting daily stock market return using dimensionality reduction (2017) |  
X Zhong, D Enke

   Summary: The paper presents a data mining process to forecast the daily direction of the S&P 500 Index ETF (SPY) return based on 60 financial and economic features. Three mature dimensionality reduction techniques, including principal component analysis (PCA), fuzzy robust principal component analysis (FRPCA), and kernel-based principal component analysis (KPCA) are applied to the whole data set to simplify and rearrange the original data structure. The 60 input features can be divided into 10 groups: the SPY return for the current day and three previous days, the relative difference in percentage of the SPY return, exponential moving averages of the SPY return, Treasury bill rates, certificate of deposit rates, financial and economic indicators, the term and default spreads, exchange rates between the USD and four other currencies, the return of seven world major indices (other than the S&P 500), SPY trading volume, and the return of eight large capitalization companies within the S&P 500.

10. The empirical risk return relation  A factor analysis approach (2007) |  
SC Ludvigson, S Ng

    Summary: The paper uses dynamic factor analysis for large datasets of macroeconomic and financial information via principal component analysis, whereby a large amount of economic information can be summarized by a few estimated factors. 2 sets of quarterly datasets are used: one comprising 209 series of macroeconomic indicators, and one comprising 172 series of financial indicators. 

11. CNNpred CNN-based stock market prediction using a diverse set of variables (2019) |  
E Hoseinzade, S Haratizadeh

    Summary: The paper introduces a CNN-based framework that can be applied on a collection of data from a variety of sources, including different markets, in order to extract features for predicting the future of those markets. The suggested framework is applied for predicting the next day’s direction of movement for the indices of S&P 500, NASDAQ, DJI, NYSE, and RUSSELL based on various sets of initial variables. 82 variables are used as inputs, consisting of technical indicators, big U.S. companies, commodities, the exchange rate of currencies, future contracts, world’s stock indices, and other variables.

# Structure of repository folders/files

- src folder (11 `generate_feature` py files to generate features for each of the 11 papers)
- config file (json file containing necessary parameters such as start/end dates for raw data query/download)
- pre-processing (standardized code for pre-processing data, if applicable)
- test folder (unit test code for code testing)
- raw_data folder (csv files, if applicable)
- result folder (output files in csv format)
- sample folder (sample src, config, raw data, pre-processing and result output files)
- reference_papers folder (pdf of all the 11 papers including supplementary online appendix, if applicable)

# Requirements and dependencies

## Databases

- `Wharton Research Data Sevices (WRDS)` account with access to Compustat, CRSP and IBES. WRDS is typically available to faculty, students, and researchers at subscribing universities.

- `Federal Reserve Economic Data (FRED)` API key to query macroeconomic data. Requesting an API key is free of charge.

## Libraries

- Install the `WRDS-Py` library.  
$ pip install wrds

- Install `Yahoo Finance` library  
$ pip install yahoo-finance

- Install `unittest` library (for code testing)   
$ pip install unittest

## APIs

- `fredapi`  
$ pip install fredapi

- `investpy`  
$ pip install investpy

# How to run
- Input user-defined parameters into config.json. See the `Configuration settings` section below for more details.
- Run the specific research paper's `py` file to download raw data and generate features for the paper.
- Result/output file (csv) can be found in the `result` folder.

# Configuration settings

The following configuration settings are available within the config.json file:

- wrds_username: Enter the WRDS username. This allows for a pgpass file to be created such that usernames and passwords do not need to be typed in repeatedly.
- start_year and end_year: Start and end year of raw data query for Yahoo Finance, Compustat and CRSP databases.

# Outputs
The output of each `py` file will be saved in the `result` folder in csv format.

# Python code standard and best practices
All the python code in this repository will follow the best practices as stated in the following guide/documentation in terms of formatting, code layout and styling -

`PEP 8 documentation` (Link: https://peps.python.org/pep-0008/)

# References
- Original reference paper (with supplementary appendix, if applicable) can be accessed from the reference_papers folder

# Contact information

Please feel free to contact this repo's owner at the email below for clarifications, further information or collaboration - 

Repo owner: Noor  
Repo owner email: muhammadnag.2021@mitb.smu.edu.sg

