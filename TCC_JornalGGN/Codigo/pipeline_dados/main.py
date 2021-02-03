# -*- coding: utf-8 -*-
"""
Created on Mon Feb  1 21:35:40 2021

@author: User
"""
import pandas as pd
from functions import brasil_io, select_columns,calc_media, select_columns_estatico, renomear_mun, convert_to_100k



def main():
    ## PIPE BRASIL_IO ##
    df = (brasil_io().pipe(select_columns)
                     .pipe(calc_media)
         )
    
    ## PIPE_FINAL ## 
    path = 'input/dados_br.xlsx'
    return (pd.read_excel(path).pipe(select_columns_estatico)
                               .pipe(renomear_mun)
                               .pipe(pd.merge, df, on='city',how='inner')
                               .pipe(convert_to_100k) 
                               ).to_csv('output/dados_final.csv', encoding='latin1', sep=';')
    

if __name__ == "__main__":
    main()
    
    
    