import unittest
import os,sys,inspect
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir) 
import DataFormat
import pandas as pd
from pandas.util.testing import assert_frame_equal#,assert_series_equal 

res =  pd.read_csv('Pagamentos.csv',header =None,names = ['id','data','valorPago','tipoPlano']) 
loadedDF = pd.read_csv('loadedDF.csv',parse_dates=['data'])  
loadedDfFormated = pd.read_csv('loadedDfFormated.csv',keep_default_na=False)  
exportDf = pd.read_csv('exportDf.csv')  
clientsInfo = pd.read_csv('clientsInfo.csv')
failuraDF = 'dataframes diferentes'

class TestDataFormattingMethods(unittest.TestCase):

    def test_loadDataDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.loadDataDf(res), loadedDF,failuraDF)
        
    def test_formatDataDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.formatDataDf(loadedDF), loadedDfFormated,failuraDF)
        
    def test_clientsInfoResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.loadClientsInfo(), clientsInfo,failuraDF)
        
    def test_exportedDfResult(self): #Verifica se o numero de colunas para o DF se manteve o mesmo
        assert_frame_equal(DataFormat.finalFormat(loadedDfFormated,clientsInfo), exportDf,failuraDF)

if __name__ == '__main__':
    unittest.main()
    
     #assert_series_equal( exportDf1.iloc[:,8],exportDf.iloc[:, 8])
     #exportDf1.iloc[:,8]