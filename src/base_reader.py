#%% 
import pickle
import os

class BaseReader():

    INTERIM_PATH = './data/interim/'

    def __init__(self, data, file_name):
        self.data = data
        self.file_name = file_name

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
# class SBaseReader(Base    Reader):
#     def __init__(self,data={},name='deneme'):
#         super().__init__(data,name)
        

#     def play_data(self):
#         self.data = {'data':'somedata'}


# #%%
# if __name__ == '__main__':
#     reader = SReader()
#     reader.check_interim_directory()
#     reader.play_data()
#     print(reader.data)
#     print(reader.name)
#     reader.dict_to_pickle()

# %%
