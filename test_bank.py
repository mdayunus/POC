import unittest
import pandas as pd
import numpy as np
import bank


class TestBank(unittest.TestCase):

    def test_newtransactioncolumn(self):
        data1 = [np.nan, 1000]
        df1 = pd.Series(data1)
        data2 = [1000, np.nan]
        df2 = pd.Series(data2)
        data3 = [np.nan, np.nan]
        df3 = pd.Series(data3)
        data4 = [1000, 1000]
        df4 = pd.Series(data4)
        self.assertAlmostEqual(
            bank.newtransactioncolumn(df1, df1[0], df1[1]), 1000)

        self.assertAlmostEqual(
            bank.newtransactioncolumn(df2, df2[0], df2[1]), 1000)

        self.assertRaises(
            Exception, bank.newtransactioncolumn, df3, df3[0], df3[1])

        self.assertRaises(
            Exception, bank.newtransactioncolumn, df4, df4[0], df4[1])

    def tes_group_and_save(self):
        df1 = pd.read_csv('testBank.csv')
        gby1 = ['TransactionType']
        getg = 'CR'
        self.assertRaises(Exception, bank.group_and_save, df1, gby1, getg)
        df2 = pd.read_csv('updatedBankTest.csv')
        gby2 = 'TransactionType'
        self.assertRaises(Exception, bank.group_and_save, df2, gby2, getg)
