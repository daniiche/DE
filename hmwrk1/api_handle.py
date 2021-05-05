import requests
from requests import HTTPError
import yaml
import json
import datetime as dt
from datetime import datetime
# import time
from operator import add, sub
import os


def load_config(config_path):
    with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config

def auth():
    path = '/api_auth_config.yaml'
    conf = load_config(path)['api_handle']
    url = conf['url']+conf['endpoint']
    data = json.dumps(conf['payload'])
    headers = conf['headers']
    try:
        result = requests.post(url, data=data, headers=headers)
        result.raise_for_status()
        token = conf['token_type'] + result.json()['access_token']
        return token

    except HTTPError:
        print('Exception with:')
        print(conf['url']+conf['endpoint'])

def get(url, date, headers):
    try:
        result = requests.get(url
                              , data=json.dumps({"date": date})
                              , headers=headers
                              , timeout=10)

        return result.json()

    except HTTPError:
        print('Error')

def loop(url, date, headers, operation):
    response_list = []
    flag = True
    while flag:
        result = get(url, date, headers)
        if isinstance(result, dict) \
                and (result['message'] == 'No out_of_stock items for this date'):
            flag = False
        else:
            print(result[0]['date'])
            response_list.append(result)
        date = str(
            operation(
                datetime.strptime(date, "%Y-%m-%d")
                , dt.timedelta(days=1)
            ).date()
        )
        #time.sleep(1)
    return response_list


def save(inp):
    name = inp[0]['date']
    path = os.path.join(os.getcwd(), f'/data/{name}')

    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s" % path)

    with open(f'{path}/outdated_{name}.json', 'w') as json_file:
        json.dump(inp, json_file)


def request(token):
    path = '/api_config.yaml'
    conf = load_config(path)['api_handle']
    url = conf['url'] + conf['endpoint']
    start_date = conf['start_date']
    headers = conf['headers']
    headers['authorization'] = token
    # from 2021-01-01 to  2021-04-16
    # assume the date was set to be 2021-01-02
    joined_list = loop(url, start_date, headers, sub) \
        + loop(url, start_date, headers, add)

    for item in joined_list:
        save(item)


if __name__ == '__main__':
    request(auth())
