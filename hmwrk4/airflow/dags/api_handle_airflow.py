import requests
from requests import HTTPError
import yaml
import json
import os


def load_config():
    config_path = 'api_config.yaml'
    with open(os.path.join(os.getcwd(), config_path), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config

def auth():
    conf = load_config()['api_handle']
    url = conf['url']+conf['endpoint_auth']
    data = json.dumps(conf['credentials'])
    headers = {"content-type": "application/json"}
    try:
        result = requests.post(url, data=data, headers=headers)
        result.raise_for_status()
        token = "JWT " + result.json()['access_token']
        return request(token)

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


def save(inp):
    name = inp[0]['date']
    path = os.path.join(os.getcwd(), f'data/{name}')

    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s" % path)

    with open(f'{path}/out_{name}.json', 'w') as json_file:
        json.dump(inp, json_file)


def request(token):
    conf = load_config()['api_handle']
    url = conf['url'] + conf['endpoint_get']
    start_date = conf['start_date']
    headers = conf['headers']
    headers['authorization'] = token

    result = None
    if isinstance(result, dict) \
            and (result['message'] == 'No out_of_stock items for this date'):
        print('Empty Date')
    else:
        result = get(url, start_date, headers)

    save(result)


if __name__ == '__main__':
    auth()
