import requests
import os


def run(config, current_date):
    jwt = auth(config)

    api_config = config['api']
    url = config['baseUrl'] + api_config['endpoint']
    headers = {'Authorization': 'JWT ' + jwt}
    response = requests.get(url=url, headers=headers, json={'date': f'{current_date}'})
    if response.status_code == 200:
        output_dir = os.path.join(config['outputDir'], f'{current_date}')
        os.makedirs(output_dir, exist_ok=True)
        res_data = []
        for item in response.json():
            res_data.append(item['product_id'])
        with open(os.path.join(output_dir, 'out-of-stock-products.json'), 'w') as output_file:
            output_file.write(str(res_data))


def auth(config):
    auth_config = config['auth']
    response = requests.post(url=config['baseUrl'] + auth_config['endpoint'],
                             json={'username': auth_config['username'], 'password': auth_config['password']}
                             )
    response.raise_for_status()
    return response.json()['access_token']


if __name__ == '__main__':
    test_config = {
        'baseUrl': 'https://robot-dreams-de-api.herokuapp.com',
        'outputDir': '/home/serg/export-data',
        'auth': {'endpoint': '/auth',
                 'username': 'rd_dreams',
                 'password': 'djT6LasE'},
        'api': {'endpoint': '/out_of_stock'}
    }
    run(test_config, '2021-01-01')
