#%%
from dataclasses import replace
import string
import pandas as pd
import os
import glob
from joblib import Parallel, delayed
import json
import numpy as np
pd.options.display.max_columns = 99

#%%
class BaseTransformer():
    date_formats = ["%Y-%m-%d","%Y-%d-%m","%d-%m-%Y","%m-%d-%mY", "%Y/%m/%d","%Y/%d/%m","%d/%m/%Y","%m/%d/%mY"]

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
  
    def tame_colnames(self):
        for name_i,df_i in self.data.items():
            df_i_new = df_i.copy()
            # clean duplicated colnames
            column_names = df_i_new.columns
            new_column_names = []
            for column_name in column_names:
                if column_name not in new_column_names:
                    k = 1
                    new_column_names.append(column_name)
                else:
                    new_column_names.append(column_name+f'_{k}')
                    k += 1
            df_i_new.columns = new_column_names
            # tame letters
            map_colnames = {col:self._tame_text(col) for col in  df_i_new.columns}
            df_i_new =  df_i_new.rename(columns=map_colnames)
            self.data[name_i] = df_i_new
        
    
    def reformat_dates(self, config_file):
        for name_i ,dict_map_i in config_file.items():
            for colname_i, date_format_i in dict_map_i.items():
                self.data[name_i][colname_i] = pd.to_datetime(self.data[name_i][colname_i], format=date_format_i ,errors='coerce')

    def reformat_dtypes(self, config_file):
        for name_i ,dict_map_i in config_file.items():
            self.data[name_i] = self.data[name_i].astype(dict_map_i)


    def tame_strings(self, X_raw):
        X = X_raw.copy()
        X = X.fillna('NULL').applymap(lambda x: x.lower().strip()).replace('null',np.nan)
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
    INPUT_PATH = '../data/raw/date_2'
    from read import ExcelReader
    excel_reader = ExcelReader(root_dir=INPUT_PATH)
    # raw_data = excel_reader.read_data()

    # city_mappings = pd.read_feather('../../../mappings/city_mappings_lite.feather')

    map_date_columns = {
        'date_1':'%Y-%m-%d',
        'date_2':'%d/%m/%Y',
        'date_3':'%Y/%d/%m',
        'date_4':'%Y-%m-%d',
        'data_di_nascita_1':'%Y-%m-%d'
        }
    
    df_raw = raw_data['test_data_1_loan_data'].copy()
    # display(df_raw.head(2))

    df_raw = Transform().tame_colnames(df_raw)
    


