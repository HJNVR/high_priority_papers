import unittest
import json
import pandas as pd

class Testing(unittest.TestCase):
    
    def setUp(self):
        self.features = pd.read_csv('../result/sp500/consolidated/consolidated_features.csv', index_col = 0)
        with open('../src/config.json') as config_file:
            data = json.load(config_file)
        self.start_date = data['start_date']
        self.end_date = data['end_date']

    def test_date(self):
        self.assertTrue(self.features['date'].values[0] >= self.start_date)
        self.assertTrue(self.features['date'].values[-1] <= self.end_date)

    def test_features_num(self):
        # consolidated file should have 615 columns
        self.assertEqual(self.features.shape[1], 615)

if __name__ == '__main__':
    unittest.main()
