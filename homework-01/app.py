from config import get_config
import requests
import datetime
import os


def run():
    config = get_config()
    jwt = auth(config)

    api_config = config['api']
    start_date = datetime.date.fromisoformat(api_config['startDate'])
    end_date = datetime.date.fromisoformat(api_config['endDate'])
    delta = datetime.timedelta(days=1)
    current_date = start_date
    url = config['baseUrl'] + api_config['endpoint']
    headers = {'Authorization': 'JWT '+jwt}
    while current_date <= end_date:
        response = requests.get(url=url, headers=headers, json={'date': f'{current_date}'})
        if response.status_code == 200:
            output_dir = os.path.join(config['outputDir'], f'{current_date}')
            os.makedirs(output_dir, exist_ok=True)
            res_data = []
            for item in response.json():
                res_data.append(item['product_id'])
            with open(os.path.join(output_dir, 'out-of-stock-products.json'), 'w') as output_file:
                output_file.write(str(res_data))
        current_date = current_date + delta


def auth(config):
    auth_config = config['auth']
    response = requests.post(url=config['baseUrl']+auth_config['endpoint'],
                             json={'username': auth_config['username'], 'password': auth_config['password']}
                             )
    response.raise_for_status()
    return response.json()['access_token']


if __name__ == '__main__':
    run()
