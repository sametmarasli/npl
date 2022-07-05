#%%
import string
import pandas as pd
import os
import glob
from joblib import Parallel, delayed
import json
from .base_reader import BaseReader

#%%

class ExcelReader(BaseReader):
    extentions = ["*.xlsx", "*.xls", "*.xlsb"]

    def __init__(self, data = {}, file_name = None, root_dir = None):
        super().__init__(data, file_name)
        self.root_dir = root_dir
        self.data_dict = None
    
    def find_file_name(self):
        self.file_name = self.root_dir.strip('/').split('/')[-1] + '_excel'

    def list_raw_files(self):
        """List files at the directory"""
        raw_file_list = os.listdir(self.root_dir)
        return raw_file_list


    def list_files_by_extention(self, extentions:list = extentions) -> dict :
        """Creates a {file name:path} for a given file extension list in the root path"""
        root_dir_and_extension = [os.path.join(self.root_dir,extension) for extension in extentions]

        file_dirs = [y_i.replace("\\", "/")
                for raw_file_dirs in [glob.glob(x_i) 
                for x_i in root_dir_and_extension]
                for y_i in raw_file_dirs
                if "$" not in y_i]

        file_names = ['_'.join(file.split('/')[-1].split('.')[:-1]) for file in file_dirs]
        self.data_dict = dict(zip(file_names,file_dirs))
        

    def _tame_text(self, text):
        text = text.lower()
        pass_characters = list(set(string.ascii_lowercase))+[str(e) for e in range(10)]+['_']
        for char in text:
            if char not in pass_characters:
                text = text.replace(char,'_')
        text = text.strip('_')
        text = '_'.join([t for t in text.split('_') if len(t)>0])
        return text

    def read_excel_files(self):
        for file_name, file_dir in self.data_dict.items():
            sheet_names = pd.ExcelFile(file_dir).sheet_names      
            list_excel = Parallel(n_jobs=-1)(delayed(pd.read_excel)(file_dir, 
                                                            sheet_name = name,
                                                            keep_default_na=False,
                                                            na_values='',
                                                            dtype=str, 
                                                            )
                                                            for name in sheet_names)
            for j, sheet_name in enumerate(sheet_names):
                self.data["{}_{}".format(self._tame_text(file_name), self._tame_text(sheet_name))]= list_excel[j]


    def read_data(self):
        self.find_file_name()
        if self.file_name in os.listdir(ExcelReader.INTERIM_PATH):
            print(f"{self.file_name} is already written!")
            self.pickle_to_dict()
            
        else:
            self.list_files_by_extention()
            self.read_excel_files()
            self.dict_to_pickle()
        print(f"{self.file_name} is initialized.")
        



if __name__ == "__main__":

    INPUT_PATH = '../data/raw/date_2'
    test_excel_reader = ExcelReader(root_dir=INPUT_PATH)
    # test_excel_reader.list_files_by_extention()
    # test_excel_reader.find_file_name()
    # test_excel_reader.read_excel_files()
    # test_excel_reader.dict_to_pickle()
    test_excel_reader.read_data()


    # test_excel_reader.clean_interim_folder()
    test_excel_reader.remove_file_from_interim_folder('date_2')
    test_excel_reader.check_interim_directory()

# %%
