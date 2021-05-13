import requests
from requests import HTTPError
import yaml
import json
import os
from hdfs import InsecureClient
import logging
from datetime import datetime


def load_config():
    logging.info('Loading config')
    config_path = 'api_config.yaml'
    with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        logging.info('Config loaded')
        return config

def auth():
    logging.info('Starting auth')
    conf = load_config()['api_handle']
    url = conf['url']+conf['endpoint_auth']
    data = json.dumps(conf['credentials'])
    headers = {"content-type": "application/json"}
    try:
        result = requests.post(url, data=data, headers=headers)
        result.raise_for_status()
        token = "JWT " + result.json()['access_token']
        logging.info('Auth success')
        return request(token)

    except HTTPError:
        logging.info('Exception with:')
        logging.info(conf['url']+conf['endpoint'])

def get(url, date, headers):
    logging.info('Start geting')
    try:
        result = requests.get(url
                              , data=json.dumps({"date": date})
                              , headers=headers
                              , timeout=10)
        logging.info('Getting ok')
        return result.json()

    except HTTPError:
        logging.info('HTTP Error')


def save(inp):
    logging.info('Start saving')
    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    name = inp[0]['date']
    path = f'/bronze/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}/'

    client.makedirs(path)

    client.write(f'{path}/out_{name}.json', data=json.dumps(inp))
    logging.info('Saving ok')


def request(token):
    logging.info('Start requesting')
    conf = load_config()['api_handle']
    url = conf['url'] + conf['endpoint_get']
    start_date = conf['start_date']
    headers = conf['headers']
    headers['authorization'] = token

    result = None
    if isinstance(result, dict) \
            and (result['message'] == 'No out_of_stock items for this date'):
        logging.info('Empty Date')
    else:
        result = get(url, start_date, headers)
    logging.info('Requesting ok')
    save(result)


if __name__ == '__main__':
    logging.basicConfig(filename=f'TT_parse_{datetime.now()}.log',
                        filemode='w',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Start loading')
    total_start = datetime.now()
    auth()
    total_finish = datetime.now()
    logging.info('Total elapsed: ')
    logging.info(total_finish - total_start)

