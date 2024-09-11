#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import dotenv
import datetime
import time
import pandas as pd
import numpy as np
import requests
import ssl
import io 
import warnings


print("-------------------------------------------------------------------------")
print("                             Bundesbank                                  ")
print("-------------------------------------------------------------------------")


# Package sdmx1 usage:
# https://sdmx1.readthedocs.io/en/latest/example.html
# https://sdmx1.readthedocs.io/en/latest/sources.html

# --- start
time_start = time.time()


####################################################################################################
#####                                     GET and WRITE MetaData
####################################################################################################

# --- read series in CSV
# seriesList = pd.read_csv("loader.csv",sep=",",encoding ='cp1252')
seriesList = pd.read_csv("loader.csv",sep=",",encoding ='utf-8-sig')
if seriesList.shape[0]==0:
    sys.exit("Error: No time series to load!")


today = datetime.date.today()
day   = int( today.strftime("%d") )

countries = pd.read_csv("countries.csv",sep=",", keep_default_na=False)
dict_cnt   = dict(zip(countries.source_id, countries.country_id))

headers = {'accept': 'text/csv',
           'accept-language': 'en'}

all_symbols = pd.DataFrame()

# seriesList = seriesList[seriesList.symbol=='BBK01']

for index, row in seriesList.iterrows():
  print(row["symbol"])
  key = row["symbol"]
  resp_bundes = requests.get(f"https://api.statistiken.bundesbank.de/rest/data/{key}",headers=headers)

  try:
    resp_bundes = requests.get(f"https://api.statistiken.bundesbank.de/rest/data/{key}",headers=headers)
    
    nb_while = 0
    while (resp_bundes.status_code == 503) & (nb_while < 10):
      time.sleep(60)
      resp_bundes = requests.get(f"https://api.statistiken.bundesbank.de/rest/data/{key}",headers=headers)
      nb_while += 1

    if resp_bundes.status_code == 404:
      print("Error")
      continue
  except:
    print("Error")
    continue
  
  # PARTIE META
  data_bundes = pd.read_csv(io.StringIO(resp_bundes.text), delimiter=',', encoding='utf-8')
  
  # Si pas de pays, on passe à la suite
  try:
    ref_area_colname = data_bundes.columns[data_bundes.columns.str.contains('AREA')].tolist()[0]
  except:
    print("Pas de pays")
    continue

  # data_bundes.to_csv('test3.csv')
  # Création du symbol en concaténant les colonnes
  if "BBK_ID" in data_bundes.columns:
    symbols = data_bundes["BBK_ID"]
  else:
    start_col = data_bundes.columns.get_loc('BBK_STD_FREQ')
    end_col = data_bundes.columns.get_loc('TIME_PERIOD')
    cols_to_concat = data_bundes.columns[start_col:end_col]
    symbols = key + "." +  data_bundes[cols_to_concat].astype(str).agg('.'.join, axis=1)
    
  if "BBK_STD_FREQ" in data_bundes.columns:
      frequency = data_bundes['BBK_STD_FREQ']
  elif "TIME_FORMAT" in data_bundes.columns:
      frequency = data_bundes['TIME_FORMAT']
      if frequency == 'P1M':
        frequency = 'M'
      elif frequency == 'P3M':
        frequency = 'Q'
      elif frequency == 'P1Y':
        frequency = 'A'
  else:
    print("No frequency found")
    continue
    
  
  symbols = symbols.str.replace(".", "_")
  symbols = symbols.str.replace("__", "_")
  
  metadata = pd.DataFrame(data_bundes)
  metadata['country_id']= data_bundes[ref_area_colname]
  
  # metadata["symbol"] = metadata.apply(lambda row: row["BBK_ID"] , axis=1)
  metadata['symbol'] = symbols # BBK01 ne posséde pas de colonne BBK_STD_FREQ
  symbols = metadata.apply(lambda row: '_'.join([part for part in row['symbol'].split('_') if part != row['country_id']]), axis=1)
  
  if 'BBK_TITLE_ENG' in metadata.columns:
    # Si 'BBK_TITLE_ENG' est présent, l'utiliser
    metadata['name'] = metadata['BBK_TITLE_ENG']
  else:
    # Sinon, utiliser 'BBK_TITLE'
    metadata['name'] = metadata['BBK_TITLE']
    
  metadata["symbol"] = symbols
  metadata["description"] = metadata.apply(lambda row: row["name"], axis=1)
  columns = ['symbol',"name", "description", "BBK_UNIT"] # Sélection des colonnes qui nous intérésse
  metadata = metadata[columns] # Création du nouveau dataframe 
  metadata = metadata.rename(columns={ "BBK_UNIT" : "unit"}) # Rename des colonnes
  metadata["frequency"] = frequency
  metadata['adjustment']   = None # Ajout de adjustment category and country_list
  metadata['category']     = None 
  metadata['country_list'] = None
  metadata.replace({np.nan:None}, inplace=True) # Remplacé les NA par none
  metadata = metadata.drop_duplicates()
  
  # PARTIE DATA
  data = pd.DataFrame(data_bundes, columns=['TIME_PERIOD', 'OBS_VALUE',ref_area_colname])
 
  data = data.rename(columns={'TIME_PERIOD':'timestamp','OBS_VALUE':'value',ref_area_colname : 'country_id'})
  data['symbol'] = symbols 
  # data.replace('None', np.nan, inplace=True)
  data.dropna(inplace=True)
  data['timestamp'] = data['timestamp'].astype(str)
  
  symbol_tmp = metadata[metadata.frequency == "M"]["symbol"].to_list()
  data.loc[data.symbol.isin(symbol_tmp), 'timestamp'] = data.loc[data.symbol.isin(symbol_tmp), 'timestamp'] + '-01'
  
  symbol_tmp = metadata[metadata.frequency == "Q"]["symbol"].to_list()
  data.loc[data.symbol.isin(symbol_tmp), 'timestamp'] = pd.to_datetime(data.loc[data.symbol.isin(symbol_tmp), 'timestamp'].str.split('-Q').apply(lambda x: 'Q'.join(x[::-1])))

  symbol_tmp = metadata[metadata.frequency == "A"]["symbol"].to_list()
  data.loc[data.symbol.isin(symbol_tmp), 'timestamp'] = data.loc[data.symbol.isin(symbol_tmp), 'timestamp'] + '-01-01'

  unitmult = data_bundes['BBK_UNIT_MULT'].apply(lambda x: 10 ** int(x) if pd.notna(x) and x != '0' else 1)   
  
  if not data['country_id'].isna().all():
    data['country_id'] = data['country_id'].replace(dict_cnt) # Correspondance entre le code du providers et le code ISO 3
    data = data[data['country_id'].isin(countries.country_id)] # Suppression de tous les datasets qui ne corresponde pas à un pays # data.dropna(inplace=True) # Enlever les NA
  else:
    data = data[data['country_id'].isin(countries.country_id)]
  data["value"] = pd.to_numeric(data["value"], errors= 'coerce') * unitmult # Mettre en numérique Value, erros = 'coerce' pour prendre en compte les NA
  data.dropna(inplace=True)

  all_symbols = pd.concat([all_symbols, metadata['symbol']])
  
    


