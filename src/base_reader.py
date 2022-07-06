#%% 
import pickle
import os
import glob
import string

class BaseReader():
    FEATHER_EXTENTIONS = ["*.f", "*.feather"]
    EXCEL_EXTENTIONS = ["*.xlsx", "*.xls", "*.xlsb"]

    INTERIM_PATH = './data/interim/'

    def __init__(self, data, file_name):
        self.data = data
        self.file_name = file_name

    def list_files_by_extention(self, root_dir, extentions:list) -> dict :
        """Creates a {file name:path} for a given file extension list in the root path"""
        
        root_dir_and_extension = [os.path.join(root_dir,extension) for extension in extentions]
        file_dirs = [y_i.replace("\\", "/")
                for raw_file_dirs in [glob.glob(x_i) 
                for x_i in root_dir_and_extension]
                for y_i in raw_file_dirs
                if "$" not in y_i]
        file_names = ['_'.join(file.split('/')[-1].split('.')[:-1]) for file in file_dirs]
        return dict(zip(file_names,file_dirs))

    def dict_to_pickle(self):
        pickle_out = open(os.path.join(BaseReader.INTERIM_PATH,self.file_name), 'wb')
        pickle.dump(self.data, pickle_out)
        pickle_out.close()

    def pickle_to_dict(self):
        pickle_in = open(os.path.join(BaseReader.INTERIM_PATH,self.file_name), 'rb')
        self.data = pickle.load(pickle_in)
        pickle_in.close()
        # return pickle.load(pickle_in)

    def check_interim_folder(self):
        return(os.listdir(BaseReader.INTERIM_PATH))

    def clean_interim_folder(self):
        for file in os.listdir(BaseReader.INTERIM_PATH):
            os.remove(os.path.join(BaseReader.INTERIM_PATH,file))
        print("Interim folder is cleaned")
        return

    def remove_file_from_interim_folder(self, filename):
        try:
            os.remove(os.path.join(BaseReader.INTERIM_PATH,filename))
            print(f"{filename} is removed.")
            return
        except Exception as e:
            print(e)

    def unite_data(self, data_list):
        assert self.data == {}, 'Reinitialize the class BaseReader'
        for data_i in data_list:
            for key, value in data_i.items():
                self.data[key] = value
        print(f"{self.file_name} is initialized.")

    
    def _tame_text(self, text):
        text = text.lower()
        pass_characters = list(set(string.ascii_lowercase))+[str(e) for e in range(10)]+['_']
        for char in text:
            if char not in pass_characters:
                text = text.replace(char,'_')
        text = text.strip('_')
        text = '_'.join([t for t in text.split('_') if len(t)>0])
        return text

# %%
