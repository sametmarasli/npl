import string
import pandas as pd
import os
import glob
from joblib import Parallel, delayed
import json
from .base_reader import BaseReader
import pandas as pd

class FeatherReader(BaseReader):

    def __init__(self, data = {}, file_name = None, root_dir = None):
        super().__init__(data, file_name)
        self.root_dir = root_dir
        self.data_dict = None
    
    def find_file_name(self):
        self.file_name = self.root_dir.strip('/').split('/')[-1] + '_feather'

    def list_raw_files(self):
        """List files at the directory"""
        raw_file_list = os.listdir(self.root_dir)
        return raw_file_list

    def read_feather_files(self):
        for file_name, file_dir in self.data_dict.items():
            df = pd.read_feather(file_dir)
            self.data["{}".format(self._tame_text(file_name))] = df
            
    def read_data(self):
        self.find_file_name()
        if self.file_name in os.listdir(FeatherReader.INTERIM_PATH):
            print(f"{self.file_name} is already written!")
            self.pickle_to_dict()
            
            
        else:
            self.data_dict = self.list_files_by_extention(
                root_dir=self.root_dir, 
                extentions = FeatherReader.FEATHER_EXTENTIONS
                )
            self.read_feather_files()
            self.dict_to_pickle()
            print(f"{self.file_name} is initialized.")
        
