from ETL_module.load import load_funct
from ETL_module.transform import transform_func
from ETL_module.extract.extract import extract_func


if __name__ == '__main__':
    extract_func()

    dfs  = transform_func()

    load_funct(dfs)