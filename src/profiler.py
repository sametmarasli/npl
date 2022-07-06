
from IPython.display import display
import pandas as pd
import numpy as np

class Profiler():

    def __init__(self, data={}):
        self.data = data

    def check_all_tables(self):
        for name, data in self.data.items():
            display(
                name,
                data.head())
            print('----')

    def explore_column(self, data_name, column_name, level=0):
        print(f'Profiling for data {data_name} column {column_name}\n')
        df = self.data[data_name]
        x = df[column_name].value_counts() 

        if level == 0:
            print('Top 5 Values by max count')
            display(x.head(20))
    

        if level == 1:
            print('Top 5 Values by max count')
            display(x.head(5))
            print()
            print('Ratio of unique values:' ,round(len(x)/df.shape[0],2))
            print()
            print('Count grouped by elements');print(x.value_counts().head())
            print()
            
            
            x = x[x>1] 
            if len(x)>0:
                print('Some examples')
                random_id = np.random.choice (x.index) ; display(df[df[column_name] == random_id].head(5))

    def dtypes(self):
        for name, data in self.data.items():
            display(
                name,
                data.dtypes)
            print('----')

