# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import pandas as pd
import gzip
from datetime import date, timedelta, datetime
import os


def brasil_io():     
    url = 'https://data.brasil.io/dataset/covid19/caso_full.csv.gz'
    r = requests.get(url, allow_redirects=True)
    open('casos.csv', 'wb').write(r.content)
    with open('casos.csv', 'rb') as fd:
        gzip_fd = gzip.GzipFile(fileobj=fd)
        data = pd.read_csv(gzip_fd)
    os.remove("casos.csv") 
         
    data.dropna(axis=0, subset=['city_ibge_code'], inplace=True)
    data.loc[data['new_confirmed'] < 0, 'new_confirmed'] = 0
    data.loc[data['new_deaths'] < 0, 'new_deaths'] = 0
     
    df_mun = data.loc[~(data['city'].isnull())].copy()
     
    df_mun_cases = df_mun.loc[df_mun['new_confirmed']>0].groupby(['state','city'])['date'].min().to_frame().reset_index()
    df_mun_cases.rename(columns={'date':'min_date_cases'}, inplace=True)
     
     
    df_mun = df_mun.merge(df_mun_cases, how='inner', on=['state','city'])
    df_mun['date'] = df_mun['date'].astype('datetime64')
    df_mun['min_date_cases'] = df_mun['min_date_cases'].astype('datetime64')
    df_mun['date_diff_cases'] = (df_mun['date'] - df_mun['min_date_cases']).dt.days
    
    print('Extração dos dados brasil_io realizada com sucesso \n')    
    return df_mun

#######################################################

def select_columns(df, col = ['city','city_ibge_code' ,
                                     'date','new_confirmed','new_deaths',
                                     'state']
    ,state='SP'):
    
    if col == None:
      df_semanal = df
    else:
      df_semanal = df[col]
    if state == None:
      return df_semanal
    else:
      df_semanal = df_semanal[df_semanal.state==state]
      df_semanal = df_semanal.drop(columns='state')
      print('Seleção de colunas \n')
            
      return df_semanal

#######################################################

def calc_media(df, starts=['2020-04-01','2020-08-01','2020-12-01'], 
               ends=['2020-04-30','2020-08-31','2020-12-31'], sufxs = ['_apr','_ago','_dec']):
    i = 0
    for start,end,sufx in zip(starts,ends,sufxs):
        
        list_datas = pd.date_range(start,end)
        
        df_temp = df.loc[df['date'].isin(list_datas)].copy()
    
        df_temp = df_temp.groupby(['city','city_ibge_code'])[['new_confirmed','new_deaths']].mean().reset_index()
        df_temp = df_temp.rename(columns={'new_confirmed':'cases_media'+sufx, 'new_deaths':'deaths_media'+sufx})
        df_temp['city_ibge_code'] = df_temp['city_ibge_code'].astype('int64')
        
        if i == 0: 
           df_final = df_temp.copy()
        else:
           df_final = df_final.merge(df_temp,on=['city','city_ibge_code'], how='inner')
        i+=1
    print('Cálculo média de casos/óbitos \n')
    
    return df_final

########################################################

def renomear_mun(df,mun_errado = ["Aparecida D'Oeste",'Embu',
                                  'Biritiba-Mirim','Florínia',"Estrela D'Oeste",
                                  'Itaóca','Moji Mirim',"Guarani D'Oeste",
                                  "Palmeira D'Oeste","Santa Bárbara D'Oeste",
                                  "Santa Clara D'Oeste","Santa Rita D'Oeste",
                                  "São João do Pau D'Alho","São Luís do Paraitinga"]
                 ,mun_correto = ["Aparecida d'Oeste", "Embu das Artes",
                                 "Biritiba Mirim", "Florínea","Estrela d'Oeste",
                                 "Itaoca","Mogi Mirim","Guarani d'Oeste","Palmeira d'Oeste",
               "Santa Bárbara d'Oeste", "Santa Clara d'Oeste", "Santa Rita d'Oeste",
               "São João do Pau d'Alho","São Luiz do Paraitinga"] ):

  for m in mun_errado:
    for n in mun_correto:
      if mun_errado.index(m) == mun_correto.index(n):
        df.loc[df['nome_municipio_uf']==m,'nome_municipio_uf'] = n

  df['populacao_estimada_2020'] = ((((df['tx_geometrica_cresc_anual']/100) +1)**2) * df['populacao_2018'])
  df.drop(columns=['populacao_2018','tx_geometrica_cresc_anual','area_km2_2019'],inplace=True)
  df.rename(columns={'nome_municipio_uf':'city'}, inplace=True)
  print('Renomear municípios\n')

  return df 

########################################################

def select_columns_estatico(df,col = ['nome_municipio_uf','População - 2018', 'Taxa Geométrica de Crescimento Anual da População - 2010/2020 (Em % a.a.) 2020',
             'IVS (2010)','IDHM (2010)','Área (Em km2) 2019'],
             new_col = ['nome_municipio_uf','populacao_2018','tx_geometrica_cresc_anual','IVS (2010)', 'IDHM','area_km2_2019']):
    
    df = df[col].copy()
    df.columns = new_col
    print('Seleção de colunas (tabela_indicadores)\n')

    return df

#######################################################

def convert_to_100k(df,cols = ['cases_media_apr', 'deaths_media_apr', 'cases_media_ago',
       'deaths_media_ago', 'cases_media_dec', 'deaths_media_dec']):
    for col in df[cols]:
        df.loc[range(df.shape[0]),col] =  (df[col] * 100000) / (df['populacao_estimada_2020'])
    
    print('Converter média para 100k Hab. \n')
    return df

    