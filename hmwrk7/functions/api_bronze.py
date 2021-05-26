import requests
from requests import HTTPError
import yaml
import json
import os
from hdfs import InsecureClient
import logging
from datetime import datetime
from airflow.hooks.base_hook import BaseHook

#if run locally
# def load_config():
#     logging.info('Loading config')
#     config_path = 'api_config.yaml'
#     with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
#         config = yaml.safe_load(yaml_file)
#         logging.info('Config loaded')
#         return config
#if run from airflow
def load_config():
    connection = BaseHook.get_connection('oltp_api')

    config = {
        'url': connection.host
        , 'username': connection.login
        , 'password': connection.password
    }
    logging.info('Load config ok')
    return config

def auth():
    logging.info('Starting auth')
    conf = load_config()
    url = conf['url']+'/auth'
    data = json.dumps({'username': conf['username'], 'password': conf['password']})
    headers = {"content-type": "application/json"}

    try:
        result = requests.post(url, data=data, headers=headers)
        result.raise_for_status()
        token = 'JWT ' + result.json()['access_token']
        logging.info('Auth success')
        return request(token)

    except HTTPError:
        logging.error('Exception with:'+url)

def get(url, date, headers):
    logging.info('Start geting')
    try:
        result = requests.get(url
                              , data=json.dumps({'date': date})
                              , headers=headers
                              , timeout=10)
        logging.info('Getting ok')
        return result.json()

    except HTTPError:
        logging.error('HTTP Error')


def save(inp):
    logging.info('Start saving')
    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    name = inp[0]['date']
    path = f'/bronze/{datetime.now().strftime("%Y-%m-%d")}/'

    client.makedirs(path)

    client.write(f'{path}/out_of_stock.json', data=json.dumps(inp))
    logging.info('Saving ok')


def request(token):
    logging.info('Start requesting')
    conf = load_config()
    url = conf['url'] + '/out_of_stock'
    start_date = datetime.now().strftime('%Y-%m-%d')
    headers = {"content-type": "application/json", "authorization": token}
    headers['authorization'] = token

    result = get(url, start_date, headers)
    if isinstance(result, dict) \
            and (result['message'] == 'No out_of_stock items for this date'):
        logging.error('Empty Date')
    else:
        logging.info('Requesting ok')
        save(result)


if __name__ == '__main__':
    logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Start loading')
    total_start = datetime.now()
    auth()
    total_finish = datetime.now()
    logging.info('Total elapsed: ')
    logging.info(total_finish - total_start)

