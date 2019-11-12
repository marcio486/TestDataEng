import unittest
import DataFormat
import pandas as pd

res =  pd.read_csv('Pagamentos.csv',header =None,names = ['id','data','valorPago','tipoPlano']) 
loadedDF = pd.read_csv('loadedDF.csv')  
loadedDFFormatted = pd.read_csv('loadedDFFormatted.csv')  
failureCols = 'número de colunas incorreto'
failureRows = 'número de linhas incorreto'

class TestDataFormattingMethods(unittest.TestCase):
    def test_loadDataDfRows(self): #Verifica se o numero de linhas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.loadDataDf(res).shape[0], 95476,failureRows)
        
    def test_loadDataDfColumns(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.loadDataDf(res).shape[1], 10,failureCols)
        
    def test_formatDataDfRows(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.formatDataDf(loadedDF).shape[0], 5000,failureRows)    
        
    def test_formatDataDfColumns(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        self.assertEqual(DataFormat.formatDataDf(loadedDF).shape[1], 82,failureCols)

    

if __name__ == '__main__':
    unittest.main()