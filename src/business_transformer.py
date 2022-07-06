#%%
from dataclasses import replace
import string
import pandas as pd
import os
import glob
from joblib import Parallel, delayed
import json
# from read import ExcelReader
import numpy as np
pd.options.display.max_columns = 99

#%%
class BusinessTransformer():


    def __init__(self, data):
        self.raw_data = data
        self.data = data.copy()

    def _tame_text(self, text):
        text = text.lower()
        pass_characters = list(set(string.ascii_lowercase))+[str(e) for e in range(10)]+['_']
        for char in text:
            if char not in pass_characters:
                text = text.replace(char,'_')
        text = text.strip('_')
        text = '_'.join([t for t in text.split('_') if len(t)>0])
        return text
    
    def map_keep_columns(self, config_file):
        for name_i , mapper_i in config_file.items():
            self.data[name_i] = self.data[name_i].rename(columns=mapper_i)\
                [mapper_i.values()]\
                .drop_duplicates()\
                .reset_index(drop=True) 

    def map_column_values(self, config_file):
        for name_i , list_map in config_file.items():
            for dict_map_i in list_map:
                for column_name,map_i in dict_map_i.items():
                    assert column_name in self.data[name_i].columns, 'Column does not exist in the data'
                    self.data[name_i][column_name] = self.data[name_i][column_name].replace(map_i)

    def map_scraped_column_values(self, config_file):

        def scrape_text(text, mapper):
            list_mapping = [y for x,y in mapper.items() if x in text]
            if len(list_mapping)>0:
                return(list_mapping[0])
            return text

        for name_i , list_map in config_file.items():
                for dict_map_i in list_map:
                    for column_name,map_i in dict_map_i.items():

                        self.data[name_i][column_name]= self.data[name_i][column_name].fillna('null').apply(lambda x: scrape_text(x, mapper=map_i))
        

    def map_by_threshold(self, X, mappings):

        for colname, threshold in mappings.items():
            map_other = X[colname].value_counts()[X[colname].value_counts()<threshold]
            X[f"MapT_{colname}"] = X[colname].replace({e:'other_threshold' for e in map_other.index})
            return X

    @classmethod
    def tame_all(cls,data_dict):
        assert isinstance(data_dict,dict), "Input must be a dictionary"

        tamed_data_dic = {}
        for name_i,df_i in data_dict.items():
            df_i_new = cls().tame_colnames(df_i)
            tamed_data_dic[name_i] = df_i_new
        return tamed_data_dic
 
    @classmethod
    def tame_one(cls,X):
        assert isinstance(X,pd.DataFrame), "Input must be a pd.DataFrame"

        X = cls().tame_colnames(X)
        return X
#%%
if __name__ == '__main__':
    #%%
    # INPUT_PATH = '../data/raw/test'
    # excel_reader = ExcelReader(root_dir=INPUT_PATH)
    # raw_data = excel_reader.read_data()

    # city_mappings = pd.read_feather('../../../mappings/city_mappings_lite.feather')

    map_date_columns = {
        'date_1':'%Y-%m-%d',
        'date_2':'%d/%m/%Y',
        'date_3':'%Y/%d/%m',
        'date_4':'%Y-%m-%d',
        'data_di_nascita_1':'%Y-%m-%d'
        }
        
    map_column_names = {
        'cd_ndg_debitore':'borrower_id',
        'rapporto':'loan_id',
        'gbv':'gbv',
        'stato_bilancio':'type_debt',
        'descrizione_forma_tecnica':'type_loan'

    }
    map_column_values ={
        'type_debt' : {
            'npl':'bad',
            'utp':'utp'
            },
    }


    scrape_column_values = {
        'type_loan' :  {
            'mutui' : 'mortgages',
            'c/c' : 'bank accounts',
            'descrizione non definita':'other loans',
            'carta di credito':'credit_cards',
            'mutuo ipotecario':'mortgages',
            'conto corrente':'bank_accounts'
            }
    }

    threshold_column_values = {
        'MapS_type_loan' : 600
    }

    df_raw = raw_data['test_data_1_loan_data'].copy()
    # display(df_raw.head(2))

    df_raw = Transform().tame_colnames(df_raw)
    df_raw = Transform().tame_strings(df_raw)
    df_raw = Transform().tame_dates(df_raw,  mappings=map_date_columns)
    df_raw = Transform().map_column_names(df_raw,  mappings=map_column_names)
    df_raw = Transform().map_column_values(df_raw,  mappings=map_column_values)
    df_raw = Transform().map_scraped_column_values(df_raw,  mappings=scrape_column_values)
    df_raw = Transform().map_by_threshold(df_raw,  mappings=threshold_column_values)

    display(df_raw.head())


