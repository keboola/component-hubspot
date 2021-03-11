'''
Hubspot Writer

'''
import logging
import os
import sys
from pathlib import Path
import json
import pandas as pd
import requests

from keboola.component import CommonInterface

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_ENDPOINT = 'endpoint'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []

# Request Mapping
ENDPOINT_MAPPING = {
    'create_contact': {
        'endpoint': 'contact',
        'required_column': []
    },
    'create_list': {
        'endpoint': 'lists',
        'required_column': ['name']
    },
    'add_contact_to_list': {
        'endpoint': 'lists/{list_id}/add',
        'required_column': ['list_id', 'vids', 'emails']
    },
    'remove_contact_from_list': {
        'endpoint': 'lists/{list_id}/remove',
        'required_column': ['list_id', 'vids']
    }
}

APP_VERSION = '0.0.1'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


class Component(CommonInterface):
    def __init__(self, debug=False):
        # for easier local project setup
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)

        debug = self.configuration.parameters.get(KEY_DEBUG)
        # override debug from config
        if debug:
            debug = True
        else:
            debug = False
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info('Running version %s', APP_VERSION)
            logging.info('Loading configuration...')

        try:
            # validation of required parameters. Produces ValueError
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters
        endpoint = params.get(KEY_ENDPOINT)
        api_token = params.get(KEY_API_TOKEN)
        logging.info(f'Selected Endpoint: [{endpoint}]')

        # Input tables
        in_tables = self.configuration.tables_input_mapping

        # Validate user inputs
        self.validate_user_input(params, in_tables)

        # Base parameters for the requests
        self.base_url = 'https://api.hubapi.com/contacts/v1/'
        self.base_headers = {
            'Content-Type': 'application/json'
        }
        self.base_params = {
            'hapikey': api_token
        }

        # Looping all the input tables
        for table in in_tables:

            logging.info(f'Processing input table: {table["destination"]}')

            for data_in in pd.read_csv(f'{self.tables_in_path}/{table["destination"]}', chunksize=500, dtype=str):

                self._construct_request_body(
                    endpoint, data_in)

    def validate_user_input(self, params, in_tables):

        # 1 - Ensure there is a configuration
        if params == {}:
            logging.error('Empty configuration. Please configure your writer.')
            sys.exit(1)

        # 2 - Ensure API token is entered
        if params.get(KEY_API_TOKEN) == '':
            logging.error('API token is missing.')
            sys.exit(1)

        # 3 - Ensure an endpoint is selected and valid
        if params.get(KEY_ENDPOINT) not in ENDPOINT_MAPPING:
            logging.error(
                f'{params.get(KEY_ENDPOINT)} is not a valid endpoint.')
            sys.exit(1)

        # 4 - Ensure there are input files
        if len(in_tables) < 1:
            logging.error('Input tables are missing.')
            sys.exit(1)

        # 5 - Ensure all required columns are in the input files
        # for the respective endpoint.
        # Comparing this information with the file's manifest
        required_columns = ENDPOINT_MAPPING[params.get(
            KEY_ENDPOINT)]['required_column']

        for table in in_tables:

            with open(f'{self.tables_in_path}/{table["destination"]}.manifest', 'r') as f:
                table_manifest = json.load(f)

            logging.info(table_manifest)
            table_columns = table_manifest['columns']
            missing_columns = []

            for r in required_columns:

                if r not in table_columns:
                    missing_columns.append(r)

            if missing_columns:
                logging.error(
                    f'Missing columns in input table [{table["destination"]}]: {missing_columns}')
                sys.exit(1)

    def _construct_request_body(self, endpoint, data_in):

        if endpoint == 'create_contact':

            headers = list(data_in.columns)

            for index, user in data_in.iterrows():

                request_body = {
                    'properties': []
                }

                for i in headers:

                    if not pd.isnull(user[i]):
                        tmp = {
                            'property': i,
                            'value': str(user[i])
                        }
                        request_body['properties'].append(tmp)

                # Requests handler
                response = requests.post(
                    f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}', headers=self.base_headers,
                    params=self.base_params, json=request_body)

                if response.status_code not in (200, 201):
                    response_json = response.json()
                    logging.info(
                        f'[{response.status_code}] - {response_json["message"]} - {request_body["properties"]}')

        elif endpoint == 'create_list':

            for index, contact_list in data_in.iterrows():

                request_body = {
                    'name': str(contact_list['name'])
                }

                # Requests handler
                response = requests.post(
                    f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}', headers=self.base_headers,
                    params=self.base_params, json=request_body)

                if response.status_code not in (200, 201):
                    response_json = response.json()
                    logging.info(
                        f'[{response.status_code}] - {response_json["message"]} - {contact_list["name"]}')

        elif endpoint == 'add_contact_to_list':

            # distinct list_ids
            distinct_list_id = data_in['list_id'].unique().tolist()

            # Grouping requests by list_id
            data_in_by_list_id = data_in.groupby('list_id')

            for list_id in distinct_list_id:

                if list_id == '':
                    # Ensuring all list_id inputs are not empty
                    logging.error('Column [list_id] cannot be empty')
                    sys.exit(1)

                # Fetching datagroup belong to the list_id
                list_id_sorted = data_in_by_list_id.get_group(list_id)

                # Checking available headers
                header = list(list_id_sorted.columns)

                # Request parameters
                endpoint_url = ENDPOINT_MAPPING[endpoint]['endpoint'].replace(
                    '{list_id}', str(list_id))
                request_body = {
                    'vids': [],
                    'emails': []
                }

                # Constructing request body
                for index, row in list_id_sorted.iterrows():

                    # Always prioritize pushing VIDS than emails
                    added_bool = False
                    if 'vids' in header:
                        if not pd.isnull(row['vids']):
                            request_body['vids'].append(str(row['vids']))
                        added_bool = True

                    if 'emails' in header and not added_bool:
                        if not pd.isnull(row['emails']):
                            request_body['emails'].append(str(row['emails']))

                # Requests handler
                response = requests.post(
                    f'{self.base_url}{endpoint_url}', headers=self.base_headers,
                    params=self.base_params, json=request_body)

                if response.status_code not in (200, 201):
                    response_json = response.json()
                    logging.info(
                        f'[{response.status_code}] - {response_json["message"]}')

        elif endpoint == 'remove_contact_from_list':

            # distinct list_ids
            distinct_list_id = data_in['list_id'].unique().tolist()

            # Grouping requests by list_id
            data_in_by_list_id = data_in.groupby('list_id')

            for list_id in distinct_list_id:

                if list_id == '':
                    # Ensuring all list_id inputs are not empty
                    logging.error('Column [list_id] cannot be empty')
                    sys.exit(1)

                # Grouping requests by the list_id
                list_id_sorted = data_in_by_list_id.get_group(list_id)

                # Checking available headers
                header = list(list_id_sorted.columns)

                # Request parameters
                endpoint_url = ENDPOINT_MAPPING[endpoint]['endpoint'].replace(
                    '{list_id}', str(list_id))
                request_body = {
                    'vids': []
                }

                # Constructing request body
                for index, row in list_id_sorted.iterrows():

                    if 'vids' in header:
                        if not pd.isnull(row['vids']):
                            request_body['vids'].append(str(row['vids']))

                # Requests handler
                response = requests.post(
                    f'{self.base_url}{endpoint_url}', headers=self.base_headers,
                    params=self.base_params, json=request_body)

                if response.status_code not in (200, 201):
                    response_json = response.json()
                    logging.info(
                        f'[{response.status_code}] - {response_json["message"]}')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
