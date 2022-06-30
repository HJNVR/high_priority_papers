# This test is for paper 9
# test #1: date 
# test #2: number of common features
# ==================================================

import unittest
import pandas as pd


class Testing(unittest.TestCase):

    def setUp(self):
        self.common_features = pd.read_csv(
            '../result/paper9/common_features.csv')
        self.start_year = 2000
        self.end_year = 2020

    def test_date(self):
        # From 2000 to 2020
        self.assertTrue(
            self.common_features['Date'].values[0] >= self.start_year)
        self.assertTrue(
            self.common_features['Date'].values[-1] <= self.end_year)

    def test_features_num(self):
        # Paper 9 should have total 62 common features + 1 month variable
        # Date & Date.1: Year & Month
        self.assertEqual(self.common_features.shape[1], 63)


if __name__ == '__main__':
    unittest.main()
