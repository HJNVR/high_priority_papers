import unittest
import numpy as np 
import pandas as pd
from numpy.testing import assert_array_equal
import yfinance as yf
from fredapi import Fred

class Testing(unittest.TestCase):

    start_date = '2000-01-01'
    end_date = '2020-12-31'
    
    # Fred API key: ab766afb0df13dba8492403a7865f852
    fred = Fred(api_key = 'ab766afb0df13dba8492403a7865f852')

    T1 = fred.get_series('DTB4WK', observation_start=start_date, observation_end=end_date)
    T3 = fred.get_series('DTB3', observation_start=start_date, observation_end=end_date)
    T6 = fred.get_series('DTB6', observation_start=start_date, observation_end=end_date)
    T60 = fred.get_series('DGS5', observation_start=start_date, observation_end=end_date)
    T120 = fred.get_series('DGS10', observation_start=start_date, observation_end=end_date)

    #Yahoo Finance
    test = pd.read_csv('test.csv').loc[1:, ::]
    test.reset_index(drop=True)
    test = test.fillna(0)

    df = yf.download("SPY", start=start_date, end=end_date)
    SPYt = df['Adj Close'].pct_change()[1:]
    SPYt1 = df['Adj Close'].shift(1).pct_change()[1:].fillna(0)
    SPYt2 = df['Adj Close'].shift(2).pct_change()[1:].fillna(0)
    SPYt3 = df['Adj Close'].shift(3).pct_change()[1:].fillna(0)

    def setUp(self):
        self.vars = [self.T1, self.T3, self.T6, self.T60, self.T120]
                     #self.CD1, self.CD3, self.CD6, self.CTB3M]
                     #self.CTB6M, self.CTB1Y, self.CTB5Y, self.CTB10Y, self.AAA, self.BAA]sel

        self.var_cal = [self.SPYt, self.SPYt1, self.SPYt2, self.SPYt3]
        self.var_cal_str = ['SPYt', 'SPYt1', 'SPYt2', 'SPYt3']
        
    def test_date(self):
        for var in self.vars:
            self.assertTrue(var.index[0].year >= int(self.start_date[:4]))
            self.assertTrue(var.index[-1].year <= int(self.end_date[:4]))

    def test_calculation(self):
        for i in range(len(self.var_cal)):
            assert_array_equal(self.var_cal[i].round(2).values, self.test[self.var_cal_str[i]].round(2).values)
            
if __name__ == '__main__':
    unittest.main()
        
