import pandas as pd
import numpy as np
import requests

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="ProjetoTeste-b0a2e18f5677.json"
def loadDataDf():
    
    res =  pd.read_csv('Pagamentos.csv',header =None,names = ['id','data','valorPago','tipoPlano'])
                                                     #Id                                                   #Data
    res['vlrpago'] = res.valorPago.apply(lambda x: float(x[2:].replace(',','.'))) #valor pago                                                 #Plano
    res['TipoPlano'] = res.tipoPlano.apply(lambda x:x.split('/')[0])              #TipoDoPlano 
    res['MesesPlano'] = res.tipoPlano.apply(lambda x:x.split('/')[1])             # Meses Plano
    res['data'] = pd.to_datetime(res.data,dayfirst=True)
    res['mes'] = res.data.apply(lambda x: x.month)
    res['dia'] = res.data.apply(lambda x: x.day)
    res['ano'] = res.data.apply(lambda x: x.year)
    return res

def formatDataDf(startDf):
    cl = pd.DataFrame()
    cl['id'] = startDf.id.unique()
    cl['Mt_8_2016'] = 0 # Para utilizar como mes 0
    
    for i in sorted(startDf.ano.unique()):#Criando colunas necessárias
        for x in sorted(startDf[startDf.ano == i].mes.unique()):
            cl['Mt_'+str(x)+'_'+str(i)] = 0
            cl['Tipo_'+str(x)+'_'+str(i)] = ''
            
    cl = cl.set_index('id')
     
    gbc = startDf.groupby('id')#Agrupando por id's para auxiliar na sumarização por mês
    # Valor i ex ->
    #id                              0 i[1][0] 
    #data          2018-12-05 00:00:00 i[1][1]
    #valorPago               R$ 300,00 i[1][2] 
    #tipoPlano                Bronze/3 i[1][3]
    #vlrpago                       300 i[1][4]
    #TipoPlano                  Bronze i[1][5]
    #MesesPlano                      3 i[1][6]
    #mes                            12 i[1][7] 
    #dia                             5 i[1][8] 
    #ano                          2018 i[1][9] 
    for keyC in gbc.groups.keys():
        grupoC = gbc.get_group(keyC)
        for i in grupoC.iterrows():
            for l in range(0,int(i[1][6])):
                x = (i[1][7]+l)%12
                if x == 0 : x = 12
                if i[1][7]+l >= 13:
                    try:
                        cl.at[keyC,'Mt_'+str(x)+'_'+str(i[1][9]+1)] = i[1][4]/int(i[1][6]) + [cl.at[keyC,str(x)+'/'+str(i[1][9]+1)]][0]
                    except:
                        cl.at[keyC,'Mt_'+str(x)+'_'+str(i[1][9]+1)] = i[1][4]/int(i[1][6])
                    try:
                        cl.at[keyC,'Tipo_'+str(x)+'_'+str(i[1][9]+1)] = i[1][5] + cl.at[keyC,str(x)+'/'+str(i[1][9]+1)+' Tipo'][0]
                    except:
                        cl.at[keyC,'Tipo_'+str(x)+'_'+str(i[1][9]+1)] = i[1][5]
                else:
                    try:
                        cl.at[keyC,'Mt_'+str(x)+'_'+str(i[1][9])] = i[1][4]/int(i[1][6]) + cl.at[keyC,str(x)+'/'+str(i[1][9])][0]
                    except:
                        cl.at[keyC,'Mt_'+str(x)+'_'+str(i[1][9])] = i[1][4]/int(i[1][6])
                    try:
                        cl.at[keyC,'Tipo_'+str(x)+'_'+str(i[1][9])] = i[1][5] + cl.at[keyC,str(x)+'/'+str(i[1][9])+' Tipo'] [0]
                    except:
                        cl.at[keyC,'Tipo_'+str(x)+'_'+str(i[1][9])] = i[1][5]

    for col in cl.columns:
        if 'Tipo' in col: cl[col]= cl[col].fillna('')
        else: cl[col]= cl[col].fillna(0)

    cl = cl.reset_index()   
    return cl

def loadClientsInfo():
    r = requests.get(url = 'https://demo4417994.mockable.io/clientes/')
    return pd.DataFrame(r.json())

def formatAndExportToBg(startDF,clientsInfo):
    aux = ''
    #df2 = pd.DataFrame()
    for count,colun in enumerate(startDf.columns):
       
        if colun == 'Mt_8_2016' :
            aux = colun
            continue
        if 'Mt_' in colun:
            chosen_month = colun
            back_month = aux
           
           
            df = pd.DataFrame()
            df['id'] = startDf.id.unique()
            df = df.set_index('id').sort_index()
            df['MRR'] = startDf[chosen_month]
            df['Expansion'] = (startDf[chosen_month] - startDf[back_month]).apply(lambda x: x if x > 0 else 0)
            df['Contraction'] = (startDf[chosen_month] - startDf[back_month]).apply(lambda x: x if x < 0 else 0)
            df['Canceled'] = np.where(startDf[chosen_month] == 0,startDf[back_month],0)
           
            df['aux'] = 0
    
            for col in startDf.iteritems():            # verificar se existem vendas anteriores
                if col[0] == back_month: break
                elif 'Mt_' in col[0]:
                    df['aux'] = df['aux'] + col[1].reindex(df.index)
               
            df['Ressurection'] = np.where(startDf[back_month] == 0,startDf[chosen_month],0)
            df['Ressurection'] = np.where(df['aux'] == 0,0,df['Ressurection'])
            df['Contraction'] = -np.where(df['Canceled'] == 0,df['Contraction'],0)
            df['Ativos'] = df.MRR.apply(lambda x: 1 if x > 0 else 0)
 
            df['Data'] = pd.to_datetime('/'.join(chosen_month.split('_')[1:3])).date()
            
            df['cidade'] = clientsInfo.cidade
            df['estado'] = clientsInfo.estado
            df['nome'] = clientsInfo.nome
            df['segmento'] = clientsInfo.segmento
            
            df['bronze'] = startDf[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Bronze' else 0)
            df['prata'] = startDf[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Prata' else 0)
            df['ouro'] = startDf[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Ouro' else 0)
            df['platina'] = startDf[chosen_month.replace('Mt','Tipo')].apply(lambda x : 1 if x == 'Platina' else 0)
    
            df = df.reset_index()
            df.Ressurection = df.Ressurection.astype('int64')
            df.Contraction = df.Contraction.astype('int64')
            df.MRR = df.MRR.astype('int64')
            df.Expansion = df.Expansion.astype('int64')
            df.Canceled = df.Canceled.astype('int64')
            df.aux = df.aux.astype('int64')
            
            if chosen_month == 'Mt_9_2016': df.to_gbq(destination_table ='COnjuntoTeste.DadosMesesClientesSaaS',project_id ='projetoteste-256620',if_exists = 'replace')
            else: df.to_gbq(destination_table ='COnjuntoTeste.DadosMesesClientesSaaS',project_id ='projetoteste-256620',if_exists = 'append')
    
            #d = {'mesAno':' '.join(chosen_month.split('_')[1:3]),'MRR':df.MRR.sum(),'Expansion':df.Expansion.sum(),'Contraction':df.Contraction.sum(),'Canceled':df.Canceled.sum(),'TotalAnt':df.aux.sum(),'Ressurection':df.Ressurection.sum(),'Ativos':df.Ativos.sum()}
            #df2 = df2.append(pd.DataFrame(data =d,index = [colun]))
            aux = colun
            
            
startDf = loadDataDf()
startDf = formatDataDf(startDf)
clientsInfo = loadClientsInfo()
formatAndExportToBg(startDf,clientsInfo)






