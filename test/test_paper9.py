import unittest
import pandas as pd

class Testing(unittest.TestCase):
    
    def setUp(self):
        self.common_features = pd.read_csv('datasets/paper9/common_features.csv')
        self.start_year = 2000
        self.end_year = 2022

    def test_date(self):
        # From 2000 to 2022
        self.assertTrue(int(self.common_features['Date'].values[0][:4]) >= self.start_year)
        self.assertTrue(int(self.common_features['Date'].values[-1][:4]) <= self.end_year)

    def test_features_num(self):
        # Paper 9 should have total 62 common features
        self.assertEqual(self.common_features.shape[1], 62)

if __name__ == '__main__':
    unittest.main()
