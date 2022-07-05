#%%
import pandas as pd
from src.base_reader import BaseReader
from src.excel_reader import ExcelReader
from src.profiler import Profiler
from src.base_transformer import BaseTransformer
#%%
# READ

# initialize a base raw data
raw_data = BaseReader(data = {}, file_name = 'raw_data')

# read excel files and write to interim
excel_reader_1= ExcelReader(root_dir='./data/raw/date_1/')
excel_reader_1.read_data()
excel_reader_2 = ExcelReader(root_dir='./data/raw/date_2/')
excel_reader_2.read_data()

raw_data.unite_data([excel_reader_1.data,excel_reader_2.data])
# %%
# PROFILE
profile_raw = Profiler(data=raw_data.data)
profile_raw.check_all_tables()
profile_raw.explore_column('test_data_1_borrower_data','STATO_BILANCIO')

# %%
# TRANSFORM
transformed = BaseTransformer(data=raw_data.data)
transformed.tame_colnames()

profile_transform = Profiler(data=transformed.data)
profile_transform.check_all_tables()