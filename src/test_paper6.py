# This test is for paper 6
# test #1: date 
# test #2: number of features of randomly chosen stock
# ==================================================

import os
import random
import unittest
import pandas as pd


class Testing(unittest.TestCase):

    def setUp(self):
        # randomly choose a stock
        self.stock = pd.read_csv(
            '../result/paper6/' + random.choice(os.listdir('../result/paper6')))
        self.start_year = 2000
        self.end_year = 2020

    def test_date(self):
        # From 2000 to 2020
        self.assertTrue(
            int(self.stock['date'].values[0][:4]) >= self.start_year)
        self.assertTrue(
            int(self.stock['date'].values[-1][:4])<= self.end_year)

    def test_features_num(self):
        # each stock should have 14 features + permno + date
        self.assertEqual(self.stock.shape[1], 16)


if __name__ == '__main__':
    unittest.main()
