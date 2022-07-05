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
    date_formats = ["%Y-%m-%d","%Y-%d-%m","%d-%m-%Y","%m-%d-%mY", "%Y/%m/%d","%Y/%d/%m","%d/%m/%Y","%m/%d/%mY"]

    def __init__(self, raw_data={}):
        self.raw_data = raw_data
        self.data = raw_data.copy()

    def _tame_text(self, text):
        text = text.lower()
        pass_characters = list(set(string.ascii_lowercase))+[str(e) for e in range(10)]+['_']
        for char in text:
            if char not in pass_characters:
                text = text.replace(char,'_')
        text = text.strip('_')
        text = '_'.join([t for t in text.split('_') if len(t)>0])
        return text
  
    
    def tame_colnames(self, X_raw):
        X = X_raw.copy()        

        # clean duplicated colnames
        column_names = X.columns
        new_column_names = []
        for column_name in column_names:
            if column_name not in new_column_names:
                k = 1
                new_column_names.append(column_name)
            else:
                new_column_names.append(column_name+f'_{k}')
                k += 1
        X.columns = new_column_names
        # tame letters
        map_colnames = {col:self._tame_text(col) for col in  X.columns}
        X =  X.rename(columns=map_colnames)

        return X

    @classmethod
    def d_tame_colnames(self,cls,data_dict):
        assert isinstance(data_dict,dict), "Input must be a dictionary"
        self.data = {}
        for name_i,df_i in data_dict.items():
            df_i_new = cls().tame_colnames(df_i)
            self.data[name_i] = df_i_new
        return self.data


    def tame_dates(self, X_raw, mappings):
        X = X_raw.copy()
        for colname, date_format in mappings.items():
            try:
                X[colname] = pd.to_datetime(X[colname],format=date_format,errors='coerce')
            except:
                pass
        return X

    def tame_strings(self, X_raw):
        X = X_raw.copy()
        X = X.fillna('NULL').applymap(lambda x: x.lower().strip()).replace('null',np.nan)
        return X

    def map_column_names(self, X_raw, mappings={}):
        X = X_raw.copy()
        X = X.rename(columns=mappings)[mappings.values()]
        return X

    def map_column_values(self, X_raw, mappings):
        X = X_raw.copy()
        for colname, mapping_i in mappings.items():
            X[f"MapV_{colname}"] = X[colname].replace(mapping_i)
        return X

    def map_scraped_column_values(self, X_raw, mappings):
        X = X_raw.copy()
        
        def scrape_text(text, mapper):
            list_mapping = [y for x,y in mapper.items() if x in text]
            if len(list_mapping)>0:
                return(list_mapping[0])
            return text

        for colname, mapping_i in mappings.items():
            X[f"MapS_{colname}"] = X[colname].fillna('null').apply(lambda x: scrape_text(x, mapper=mapping_i))
        
        return X

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


