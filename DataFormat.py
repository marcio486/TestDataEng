import pandas as pd
import numpy as np
import requests
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="ProjetoTeste-b0a2e18f5677.json"
#%%
def loadDataDf(res):
    '''cria/formata alguns campos iniciais'''
    res['vlrpago'] = res.valorPago.apply(lambda x: float(x[2:].replace(',','.'))) 
    res['TipoPlano'] = res.tipoPlano.apply(lambda x:x.split('/')[0]) 
    res['MesesPlano'] = res.tipoPlano.apply(lambda x:int(x.split('/')[1]))
    res['data'] = pd.to_datetime(res.data,dayfirst=True)
    res['mes'] = res.data.apply(lambda x: x.month)
    res['dia'] = res.data.apply(lambda x: x.day)
    res['ano'] = res.data.apply(lambda x: x.year)
    
    return res

#%%
def formatDataDf(startDf):
    '''Com base nos nos dados do CSV gera DataFrame com os dados de compra do cliente '''
    DadosClientes = pd.DataFrame()
    DadosClientes['id'] = startDf.id.unique()
    DadosClientes['Mt_8_2016'] = 0 # Para utilizar como mes 0
    
    DadosClientes = createColumnsCl(startDf,DadosClientes)
    DadosClientes = DadosClientes.set_index('id')
     
    gbc = startDf.groupby('id')#Agrupando por id's para auxiliar na sumarização por mês

    for keyC in gbc.groups.keys():
        grupoCliente = gbc.get_group(keyC)
        DadosClientes = formatCl(keyC,grupoCliente,DadosClientes)#Popular colunas com valores de compras formatados
            
    DadosClientes = removeNaN(DadosClientes)
    
    DadosClientes = DadosClientes.reset_index()  
    
    return DadosClientes

def createColumnsCl(startDf,cl):
    '''Cria colunas dos meses para os clientes '''
    for i in sorted(startDf.ano.unique()):#Criando colunas necessárias
        for x in sorted(startDf[startDf.ano == i].mes.unique()):
            cl['Mt_'+str(x)+'_'+str(i)] = 0
            cl['Tipo_'+str(x)+'_'+str(i)] = ''
    return cl   
  
def formatCl(keyC,grupoC,DadosClientes):
    ''' Popula o dataframe que possui todas as informações de compra do cliente
    verifica o e adiciona os valores a serem pagos e tipo do plano no mês
    # Valor ComprasCliente ex ->
    #id                              0 [0] 
    #data          2018-12-05 00:00:00 [1]
    #valorPago               R$ 300,00 [2] 
    #tipoPlano                Bronze/3 [3]
    #vlrpago                       300 [4]
    #TipoPlano                  Bronze [5]
    #MesesPlano                      3 [6]
    #mes                            12 [7] 
    #dia                             5 [8] 
    #ano                          2018 [9]  '''
    for i in grupoC.iterrows():
            # i[1][x] = descrições da compra
        ComprasCliente = i[1]
        for l in range(0,int(ComprasCliente.MesesPlano)):
            x = (ComprasCliente.mes+l)%12 #resto da divisão por 12
            if x == 0 : x = 12            #caso seja divisivel por 12 é o mês 12
            if ComprasCliente.mes+l >= 13:#Caso seja maior que 13 significa que são dados do ano seguinte 
                      #ex:'Mt_'+str(1)+'_'+str(2018+1) = Mt_1_2019 -> dados de janeiro de 2019
                DadosClientes.at[keyC,'Mt_'+str(x)+'_'+str(ComprasCliente.ano+1)] = (
                        #Ex: 300RS em um Plano de 3 meses = 100 reais por mês
                        ComprasCliente.vlrpago/int(ComprasCliente.MesesPlano) )
                
                DadosClientes.at[keyC,'Tipo_'+str(x)+'_'+str(ComprasCliente.ano+1)] =(
                        ComprasCliente.TipoPlano )
            else:                                                        
                DadosClientes.at[keyC,'Mt_'+str(x)+'_'+str(ComprasCliente.ano)] = (
                        ComprasCliente.vlrpago/int(ComprasCliente.MesesPlano) )
                
                DadosClientes.at[keyC,'Tipo_'+str(x)+'_'+str(ComprasCliente.ano)] =(
                        ComprasCliente.TipoPlano )
    return DadosClientes

def removeNaN(DadosClientes):
    '''Remove valores NaN '''
    for col in DadosClientes.columns:
        if 'Tipo' in col: DadosClientes[col]= DadosClientes[col].fillna('')
        else: DadosClientes[col]= DadosClientes[col].fillna(0)  
    return DadosClientes
#%%

def loadClientsInfo():
    # pega informações extras do cliente
    r = requests.get(url = 'https://demo4417994.mockable.io/clientes/')
    return pd.DataFrame(r.json())

#%%
    
def finalFormat(loadedDfFormated,clientsInfo):
    aux = ''
    exportDf = pd.DataFrame()
    loadedDfFormated.index  = loadedDfFormated.id.values
    #df2 = pd.DataFrame()
    for count,colun in enumerate(loadedDfFormated.columns):#Percorrer as colunas em ordem de mês
       
        if colun == 'Mt_8_2016' :
            aux = colun
            continue
        if 'Mt_' in colun:
            chosen_month = colun #Mês em em que os calculos estão sendo realizadso
            previous_month = aux #Mês anterior

            dfMes = pd.DataFrame()

            dfMes['id'] = loadedDfFormated.id.unique()
            dfMes = dfMes.set_index('id')
            
            dfMes['MRR'] = loadedDfFormated[chosen_month]
            dfMes['Expansion'] = (loadedDfFormated[chosen_month] - loadedDfFormated[previous_month]).apply(lambda x: x if x > 0 else 0)
            dfMes['Contraction'] = (loadedDfFormated[chosen_month] - loadedDfFormated[previous_month]).apply(lambda x: x if x < 0 else 0)
            dfMes['Canceled'] = np.where(loadedDfFormated[chosen_month] == 0,loadedDfFormated[previous_month],0)
            
            dfMes['aux'] = 0
            
            dfMes = checkPrevious(loadedDfFormated,dfMes,previous_month)  
           
            dfMes['Ressurection'] = np.where(loadedDfFormated[previous_month] == 0,loadedDfFormated[chosen_month],0)
            dfMes['Ressurection'] = np.where(dfMes['aux'] == 0,0,dfMes['Ressurection'])      #Caso não existam vendas anteriorer ('aux') não faz parte da metrica Ressurection
            dfMes['Contraction'] = -np.where(dfMes['Canceled'] == 0,dfMes['Contraction'],0)
            dfMes['Ativos'] = dfMes.MRR.apply(lambda x: 1 if x > 0 else 0)
 
            dfMes['Data'] = str(pd.to_datetime('/'.join(chosen_month.split('_')[1:3])).date())
            

            dfMes = appendClientsInfo(dfMes,clientsInfo)
            
            dfMes = appendPlanInfo(dfMes,chosen_month,loadedDfFormated)
    
    
            dfMes = dfMes.reset_index()
            dfMes = dfMes.fillna(0)
            dfMes = adjustTypes(dfMes)

            exportDf = exportDf.append(dfMes)
            #d = {'mesAno':' '.join(chosen_month.split('_')[1:3]),'MRR':df.MRR.sum(),'Expansion':df.Expansion.sum(),'Contraction':df.Contraction.sum(),'Canceled':df.Canceled.sum(),'TotalAnt':df.aux.sum(),'Ressurection':df.Ressurection.sum(),'Ativos':df.Ativos.sum()}
            #df2 = df2.append(pd.DataFrame(data =d,index = [colun]))
            aux = colun
    
    return exportDf.reset_index(drop = True)

def checkPrevious(loadedDfFormated,dfMes,previous_month):
    '''  verificar se existem vendas anteriores ao mês atual '''
    for col in loadedDfFormated.iteritems():           
                if col[0] == previous_month: break
                elif 'Mt_' in col[0]:
                    dfMes['aux'] = dfMes['aux'] + col[1].reindex(dfMes.index)
    return dfMes

def appendClientsInfo(dfMes,clientsInfo):
    '''Insere informações sobre o plano '''
    dfMes['cidade'] = clientsInfo.cidade
    dfMes['estado'] = clientsInfo.estado
    dfMes['nome'] = clientsInfo.nome
    dfMes['segmento'] = clientsInfo.segmento
    return dfMes

def appendPlanInfo(dfMes,chosen_month,loadedDfFormated):
    '''Insere informações adicionais de cada cliente '''
    dfMes['bronze'] = loadedDfFormated[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Bronze' else 0)
    dfMes['prata'] = loadedDfFormated[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Prata' else 0)
    dfMes['ouro'] = loadedDfFormated[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Ouro' else 0)
    dfMes['platina'] = loadedDfFormated[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Platina' else 0)
    return dfMes

def adjustTypes(dfMes):
    '''Ajusta tipos para exportação (só pra garantir) '''
    dfMes.Ressurection = dfMes.Ressurection.astype('int64')
    dfMes.Contraction = dfMes.Contraction.astype('int64')
    dfMes.MRR = dfMes.MRR.astype('int64')
    dfMes.Expansion = dfMes.Expansion.astype('int64')
    dfMes.Canceled = dfMes.Canceled.astype('int64')
    dfMes.aux = dfMes.aux.astype('int64')
    return dfMes

#%%
    
def exportToBg(exportDf):
    '''Envia os dados formatadas para o GBQ '''
    exportDf.to_gbq(destination_table ='COnjuntoTeste.DadosMesesClientesSaaS',project_id ='projetoteste-256620',if_exists = 'replace')
    
#%%       
    
if __name__ == '__main__':  
    res =  pd.read_csv('Pagamentos.csv',header =None,names = ['id','data','valorPago','tipoPlano'])      
    loadedDf = loadDataDf(res)
    #loadedDf.to_csv('loadedDfTest.csv',index=False)
    
    loadedDfFormated = formatDataDf(loadedDf)
    #loadedDfFormated.to_csv('loadedDfFormatedTest.csv',index=False)
    
    clientsInfo = loadClientsInfo()
    #clientsInfo.to_csv('clientsInfoTest.csv',index=False)
    
    exportDf = finalFormat(loadedDfFormated,clientsInfo)
    #exportDf.to_csv('exportDfTest.csv',index=False) 
    
    exportToBg(exportDf)
    #usado nos testes
    #res =  pd.read_csv('Pagamentos.csv',nrows = 500,header =None,names = ['id','data','valorPago','tipoPlano']) 
    #res.to_csv('pagamentosTeste500.csv',index = False,header = False)
    
    






