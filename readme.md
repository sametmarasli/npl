# 💰npl 

A platform for data analytics in the NPL business.

Install the package with:

    pip install -e .
# Documentation
## BaseReader
    convert data to pickle and write to interim folder
    read pickle file from interim folder
    clean the interim folder
    remove a file from interim folder
    unite dictionaries of data and create a new data dictionary with a name

## ExcelReader
    inherits the BaseReader
    reads the excel files with several sheets at a given directory

## BaseTransformer
    tame-soft clean the column names 
    drop columns
    reformat the columns (dates, strings, floats, integers, categoricals, bools)

## BusinessTransformer
    rename and keep the columns
    remap the column values
    scrape and remap the column values
    remap the column values as 'other' given a threshold
    remap the cities
    create clusters from a numeric column

## Profiler
    profile a single column and print:
        5 most frequent values
        the uniqueness of the column
        some examples from duplicated values
    compare 2 columns of the same table
    compare 2 columns of 2 different tables
    compare several columns of several different tables
    


    
