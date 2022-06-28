
import os
from datetime import datetime
import locale
locale.setlocale(locale.LC_ALL, 'es-ES')

import requests
import logging
import yaml
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.INFO)



def _get_config():

    logging.info('Getting URLs')
    with open('ETL_module\extract\webs.yaml', mode='r') as f:
        webs = yaml.load(f, Loader=yaml.FullLoader)
    
    return webs


def _order_download_files(category, response):

    month_today = datetime.today().strftime('%Y-%B')
    date_today = datetime.today().strftime('%d-%m-%Y')

    if not os.path.exists(f'raw_data/{category}/{month_today}'):
        os.mkdir(f'raw_data/{category}/{month_today}')

    with open (f'raw_data/{category}/{month_today}/{category}-{date_today}.csv', 'wb') as file:
        file.write(response.content)
    logging.info(f'The new {category} file was uploaded to the raw_data folder')
    

def get_csv_file(category, URL):

    try:
        response = requests.get(URL)
        logging.info(f'Tryin to get the {category} information')
        if response.status_code == 200:
            soup_response = BeautifulSoup(response.content, 'lxml')
            csv_download_button = soup_response.find('a', attrs={'class': 'btn-green'})
            csv_download_url = csv_download_button.get('href')
            
            ##  Try to download the csv file from the web 
            with requests.get(csv_download_url) as response:
                if response.status_code == 200:
                    _order_download_files(category,response)
                else:
                    logging.info(f'Problem importing the data file from {URL}')
                    logging.info(f'The status code is {response.status_code}')
        else:
            logging.info(f'Status code for access to {URL} is {response.status_code}')

    except Exception as e:
        logging.info(f'Can\'t find the url: {URL}')
        logging.info(e)


def extract_func():
    logging.info('STARTING EXTRACT INFORMATION PROCESS')
    logging.info('\n')

    webs = _get_config()
    for category, url in webs['webs'].items():
        get_csv_file(category, url)

    logging.info('EXTRACT INFORMATION SUCCEEDED')
    logging.info('\n')
    logging.info('----'*15)




    