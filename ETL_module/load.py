from typing import List

from decouple import config
from sqlalchemy import create_engine
import logging
logging.basicConfig(level=logging.INFO)


def _conect_to_bbdd():
    logging.info('Connecting to the database')
    global engine
    engine = create_engine(f'{config("DB_ENGINE")}://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}:{config("DB_PORT")}/{config("DB_NAME")}')
    logging.info('Successfully connected')


def updaload_dataframe_to_bbdd(df, tabla_name):
    logging.info(f'Uploadin {tabla_name} to DataBase')
    
    df.to_sql(tabla_name, con=engine, if_exists="replace")
    

def load_funct(matriz: List):
    logging.info('STARTING LOAD INFORMATION PROCESS')
    logging.info('\n')

    df_consolidated = matriz[0]
    df_quantities = matriz[1]
    df_cines = matriz[2]

    _conect_to_bbdd()
    updaload_dataframe_to_bbdd(df_quantities[0], df_quantities[1])
    updaload_dataframe_to_bbdd(df_cines[0], df_cines[1])
    updaload_dataframe_to_bbdd(df_consolidated[0], df_consolidated[1])

    logging.info('TRANSFORM INFORMATION SUCCEEDED')
    logging.info('\n')
    logging.info('----'*15)
    logging.info('\n')
    logging.info('FINISHED PROCESS, CONGRATULATIONS')