import unittest
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 
import DataFormat
import pandas as pd
from pandas.util.testing import assert_frame_equal#,assert_series_equal 

resTest =  pd.read_csv('pagamentosTeste500.csv',header =None,names = ['id','data','valorPago','tipoPlano']) 
loadedDFTest = pd.read_csv('loadedDFTest.csv',parse_dates=['data'])  
loadedDfFormatedTest = pd.read_csv('loadedDfFormatedTest.csv',keep_default_na=False)  
exportDfTest = pd.read_csv('exportDfTest.csv')  
clientsInfoTest = pd.read_csv('clientsInfoTest.csv')
failuraDF = 'dataframes diferentes'

class TestDataFormattingMethods(unittest.TestCase):

    def test_loadDataDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.loadDataDf(resTest), loadedDFTest,failuraDF)
        
    def test_formatDataDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.formatDataDf(loadedDFTest), loadedDfFormatedTest,failuraDF)
        
    def test_clientsInfoResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.loadClientsInfo(), clientsInfoTest,failuraDF)
        
    def test_exportedDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        loadedDfFormatedTestParm =loadedDfFormatedTest.copy()#names are references*
        assert_frame_equal(DataFormat.finalFormat(loadedDfFormatedTestParm,clientsInfoTest), exportDfTest,failuraDF)

if __name__ == '__main__':
    unittest.main()
    
     #assert_series_equal( exportDf1.iloc[:,8],exportDf.iloc[:, 8])
     #exportDf1.iloc[:,8]