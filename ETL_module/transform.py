from datetime import datetime

import pandas as pd
import numpy as np
from pandas import DataFrame

import locale
locale.setlocale(locale.LC_ALL, 'es-ES')
import logging
logging.basicConfig(level=logging.INFO)





def __to_a_single_dataframe(category):
    """This private function generate a personalized path with datetime functions and category to the
        to fin in RAW DATA the specific csv that you find and transform it in a pandas DataFrame 

    Args:
        category (str): category of the dataframe you are looking for 

    Returns:
        pd.DataFrame: Returns the dataframe of the category you were looking for
    """
    global _date_today
    _month_today = datetime.today().strftime('%Y-%B')
    _date_today = datetime.today().strftime('%d-%m-%Y')

    logging.info(f'transforming {category} data from {_date_today} to DataFrame')
    
    df_cultura = pd.read_csv(f'raw_data/{category}/{_month_today}/{category}-{_date_today}.csv', encoding= 'UTF-8')
    return df_cultura


def _to_a_dict_dataframe():
    """This function use __to_a_single_dataframe to automatically find the tree dataframes

    Returns:
        dicts: Returns a dictionary with category-dataframe pairs 
    """
    CATEGORIES = ['Bibliotecas', 'Cines', 'Museos']
    dfs_dict = {}

    for category in CATEGORIES:
        s = __to_a_single_dataframe(category)    
        dfs_dict[category] = s

    return dfs_dict


def __upload_date_column(df: DataFrame):
    """Add the updated_date column to the dataframe and return it """
    df['fecha_actualizacion'] = _date_today

    return df


# First Consign of Trnsformation
def _normalize_dfs_table_one(dfs_dict: dict):
    """This this function leaves our dataframes with only the columns
       of interest in order to later be able to join them without problems 

    Args:
        dfs_dict (dict): A dict with the category-dataframe pairs  

    Returns:
        dict: A dict with the category-dataframe pairs BUT THE COLUMNS ARE THE SAME IN ALL
    """

    dfs_dict_copy = dfs_dict.copy()
   

    for category, df_cultura in dfs_dict_copy.items():
        logging.info(f'Normalizing columns of the {category}\'s DataFrame')

        df_cultura = df_cultura.iloc[:,[0,1,2,4,6,7,8,9,11,12,13,14,15]]

        a = df_cultura.columns
        df_cultura.rename = df_cultura.rename({a[0]:'cod_loc',
                                               a[1]:'idprovincia',
                                               a[2]:'iddepartamento',
                                               a[3]:'categoria',
                                               a[4]:'provincia',
                                               a[5]:'localidad',
                                               a[6]:'nombre',
                                               a[7]:'direccion',
                                               a[8]:'cp',
                                               a[9]:'cod_area',
                                               a[10]:'telefono',
                                               a[11]:'mail',
                                               a[12]:'web',})
        dfs_dict_copy[category] = df_cultura
    
    return dfs_dict_copy                                  


def _unique_dataframe(dfs_dict: dict):
    """Unifiqued all of the dataframes in the dictionary"""
      
    _unifiqued_dataframe = pd.concat(dfs_dict.values(), axis=0)

    return _unifiqued_dataframe 


def _process_normalize_unique_df(df: DataFrame):
    """Concat 'telefono' and 'cod_area' columns to can drop 'cod_area' """

    df['telefono'] = df.apply(lambda x: str(x['cod_area']).replace('.0', '')+ ' ' + str(x['telefono']) 
                                                    if( x['telefono'] != 'nan' and str(x['cod_area']) != 'nan') 
                                                    else str(x), axis =1)

    df.drop(['cod_area'], axis=1, inplace=True)                  
    
    return df


# Second Consign of Transformation
def _normalize_dfs_table_quantity(dfs_dict: dict):
    """removes capital letters and accents"""

    dfs_dict_copy = dfs_dict.copy()

    a = 'áéíóúüñÁÉÍÓÚÜÑ'
    b = 'aeiouunAEIOUUN'

    for category, df_cultura in dfs_dict_copy.items():
        logging.info(f'Normalizing columns of the {category}\'s DataFrame')

        df_cultura = df_cultura.rename(columns= lambda x: x.lower())
        df_cultura = df_cultura.rename(columns= lambda x: x.translate( x.maketrans(a,b)))
        df_cultura = df_cultura.loc[:,['provincia','categoria','fuente']]

        dfs_dict_copy[category] = df_cultura
    
    return dfs_dict_copy


def _groupby_qualities(dataframe: DataFrame):
    """group by qualities and concatenate the tables results 
       as requested in the slogan IDK really hahahha, sorry
       because you have to see this"""

    df_quantity_categ = dataframe.groupby(['categoria']).count()
    df_quantity_fuente = dataframe.groupby(['fuente']).count()
    df_quantity_provin_categ = dataframe.groupby(['provincia','categoria']).count()

    df = pd.concat([df_quantity_categ,
                    df_quantity_fuente,
                    df_quantity_provin_categ])

    return df


# Third Consign of Transformation
def _normalize_df_cines():
    """Use the private function __to_a_single_dataframe to get the specific cinemas
        dataframe, then take only the columns that interest for the study and normalize it"""
    logging.info(f'Normalizing columns of the cinemas\'s DataFrame')
    df_cine = __to_a_single_dataframe('Cines')
    df_cine = df_cine.loc[:,['Provincia','espacio_INCAA','Pantallas','Butacas']]
    
    df_cine['espacio_INCAA'].replace('0', np.nan, inplace=True)
    df_cine['espacio_INCAA'].replace(('SI','Si','SÍ','Sí','si'), 1, inplace=True)

    return df_cine


def _groupby_provincias(df: DataFrame):
    """Group by provincias to implement the aggregation functions"""
    df_cine = df.groupby('Provincia').agg(sum)

    return df_cine


## Finall Functions
def consolidated_data_frame(dfs: dict):
    logging.info('Creating the consolidated table with the information of cinemas, libraries and museums')

    normalized_dfs = _normalize_dfs_table_one(dfs)
    unifiqued_df = _unique_dataframe(normalized_dfs)
    df = _process_normalize_unique_df(unifiqued_df)
    df = __upload_date_column(df)

    logging.info('TABLE CREATED SUCCESSFULLY')
    return [df,'tabla_consolidada']


def quantities_data_frame(dfs: dict):
    logging.info('Creating the quantities table')

    normalized_dfs = _normalize_dfs_table_quantity(dfs)
    unifiqued_df = _unique_dataframe(normalized_dfs)
    df = _groupby_qualities(unifiqued_df)
    df = __upload_date_column(df)

    logging.info('TABLE CREATED SUCCESSFULLY')

    return [df,'tabla_cantidades']


def cines_data_frame():
    logging.info('Creating the cinemas table')

    normalized_cines = _normalize_df_cines()
    df = _groupby_provincias(normalized_cines)
    df = __upload_date_column(df)

    logging.info('TABLE CREATED SUCCESSFULLY')

    return [df,'tabla_cines']


### MAIN FUNTION
def transform_func():
    logging.info('STARTING TRANSFORM INFORMATION PROCESS')
    logging.info('\n')

    dfs_dict = _to_a_dict_dataframe()
    df_consolidated = consolidated_data_frame(dfs_dict)
    df_quantities = quantities_data_frame(dfs_dict)
    df_cines = cines_data_frame()

    logging.info('TRANSFORM INFORMATION SUCCEEDED')
    logging.info('\n')
    logging.info('----'*15)
    return [df_consolidated, df_quantities, df_cines] 





