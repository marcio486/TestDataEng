import unittest
import DataFormat
import pandas as pd

res =  pd.read_csv('Pagamentos.csv',header =None,names = ['id','data','valorPago','tipoPlano'])      
FAILURE = 'incorrect value'

class TestStringMethods(unittest.TestCase):

    def test_loadDataDfRows(self): #Verifica se o numero de linhas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.loadDataDf(res).shape[0], 95476,FAILURE)
        
    def test_loadDataDfColumns(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.loadDataDf(res).shape[1], 10,FAILURE)


if __name__ == '__main__':
    unittest.main()