from typing import List

from decouple import config
from sqlalchemy import create_engine
import logging
logging.basicConfig(level=logging.INFO)


def conect_to_bbdd():
    """This function will be connect to the database using the database
        credentials in .env files:
            DB_ENGINE= postgresql+psycopg2
            DB_HOST=
            DB_USER=
            DB_PASSWORD=
            DB_PORT=
            DB_NAME=
    """
    logging.info('Connecting to the database')
    global engine
    engine = create_engine(f'{config("DB_ENGINE")}://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}:{config("DB_PORT")}/{config("DB_NAME")}')
    logging.info('Successfully connected')


def updaload_dataframe_to_bbdd(df, tabla_name):
    """This function will be upload a specific dataframe to a our database
        with the engine initialized, DONT FORGET TO START THE ENGINE!!!
        USE FOR THAT => conect_to_bbdd()


    Args:
        df (pd.DataFrame): This is the pandas DataFrame what will be updated like table in ddbb 
        tabla_name (str): This str is so important to conser the integrity of your database, DONT PLAY WITH IT
    """
    logging.info(f'Uploadin {tabla_name} to DataBase')
    
    df.to_sql(tabla_name, con=engine, if_exists="replace")
    

def load_funct(matriz: List):
    """MAIN LOAD FUNCTION to connect and load your dataframes to a database 

    Args:
        matriz (List): this list contains in each of its indices a sublist with a dataframe-table_name pair 
    """
    logging.info('STARTING LOAD INFORMATION PROCESS')
    logging.info('\n')

    df_consolidated = matriz[0]
    df_quantities = matriz[1]
    df_cines = matriz[2]

    conect_to_bbdd()
    updaload_dataframe_to_bbdd(df_quantities[0], df_quantities[1])
    updaload_dataframe_to_bbdd(df_cines[0], df_cines[1])
    updaload_dataframe_to_bbdd(df_consolidated[0], df_consolidated[1])

    logging.info('TRANSFORM INFORMATION SUCCEEDED')
    logging.info('\n')
    logging.info('----'*15)
    logging.info('\n')
    logging.info('FINISHED PROCESS, CONGRATULATIONS')