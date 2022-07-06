# %%
from src.base_reader import BaseReader
from src.excel_reader import ExcelReader
from src.base_transformer import BaseTransformer
from src.business_transformer import BusinessTransformer
from src.profiler import Profiler
from src.feather_reader import FeatherReader

# %%
base_reader = BaseReader(data={}, file_name='all_data')
# base_reader.clean_interim_folder()

excel_reader_1 = ExcelReader(root_dir='./data/raw/date_1')
excel_reader_1.read_data()

excel_reader_2 = ExcelReader(root_dir='./data/raw/date_2')
excel_reader_2.read_data()

feather_reader = FeatherReader(root_dir='./data/raw/date_3/')
feather_reader.read_data()



base_reader.unite_data([excel_reader_1.data,excel_reader_2.data, feather_reader.data])
base_reader.check_interim_folder()
base_reader.data


# %%

profiler = Profiler(data=base_reader.data)
profiler.check_all_tables()
