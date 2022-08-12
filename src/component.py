'''
Hubspot Writer

'''
import logging
import sys
import json
import pandas as pd
import requests
import csv

from keboola.component.base import ComponentBase

from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session

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
    },
    'update_contact': {
        'endpoint': 'contact/vid/{vid}/profile',
        'required_column': ['vid']
    },
    'update_contact_by_email': {
        'endpoint': 'contact/email/{email}/profile',
        'required_column': ['email']
    }
}


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        if self.configuration.parameters.get(KEY_DEBUG):
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info('Loading configuration...')

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.params = self.configuration.parameters
        api_token = self.params.get(KEY_API_TOKEN)

        # Base parameters for the requests
        self.base_url = 'https://api.hubapi.com/contacts/v1/'
        self.base_headers = {
            'Content-Type': 'application/json'
        }
        self.base_params = {
            'hapikey': api_token
        }

    def run(self):
        """
        Main execution code
        """

        endpoint = self.params.get(KEY_ENDPOINT)

        logging.info(f"Selected Endpoint: [{endpoint}]")

        # Input tables
        in_tables = self.configuration.tables_input_mapping

        # Validate user inputs
        self.validate_user_input(in_tables)

        # Looping all the input tables
        for table in in_tables:
            logging.info(f'Processing input table: {table["destination"]}')
            with open(f'{self.tables_in_path}/{table["destination"]}') as csvfile:
                reader = csv.DictReader(csvfile)
                # Construct endpoint body & post request
                self._construct_request_body(endpoint, reader)

        """
        for data_in in pd.read_csv(f'{self.files_in_path}/{table["destination"]}', chunksize=500, dtype=str):

            print(data_in)
            continue

            # Construct endpoint body & post request
            self._construct_request_body(
                endpoint, data_in)
        """

    def validate_user_input(self, in_tables):

        # 1 - Ensure there is a configuration
        if self.params == {}:
            logging.error('Empty configuration. Please configure your writer.')
            sys.exit(1)

        # 2 - Ensure API token is entered
        if self.params.get(KEY_API_TOKEN) == '':
            logging.error('API token is missing.')
            sys.exit(1)

        # 3 - Ensure an endpoint is selected and valid
        if self.params.get(KEY_ENDPOINT) not in ENDPOINT_MAPPING:
            logging.error(
                f'{self.params.get(KEY_ENDPOINT)} is not a valid endpoint.')
            sys.exit(1)

        # 4 - Ensure there are input files
        if len(in_tables) < 1:
            logging.error('Input tables are missing.')
            sys.exit(1)

        # 5 - Ensure all required columns are in the input files
        # for the respective endpoint.
        # Comparing this information with the file's manifest
        required_columns = ENDPOINT_MAPPING[self.params.get(
            KEY_ENDPOINT)]['required_column']

        for table in in_tables:
            with open(f'{self.tables_in_path}/{table["destination"]}.manifest', 'r') as f:
                table_manifest = json.load(f)
            table_columns = table_manifest['columns']
            missing_columns = []

            for r in required_columns:
                if r not in table_columns:
                    missing_columns.append(r)
            if missing_columns:
                logging.error(
                    f'Missing columns in input table [{table["destination"]}]: {missing_columns}')
                sys.exit(1)

        # 6 - Authentication Check to ensure the API token is valid
        auth_url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent'
        auth_param = {
            'count': 1,
            'hapikey': self.params.get(KEY_API_TOKEN)
        }

        auth_test = requests.get(auth_url, params=auth_param)
        if auth_test.status_code not in (200, 201):
            expected_error_msg = f'This hapikey ({self.params.get(KEY_API_TOKEN)}) does not exist.'
            if auth_test.json()['message'] == expected_error_msg:
                logging.error(
                    'Authentication Error. Please check your API token.')
                sys.exit(1)

            else:
                err_msg = 'Unexpected error. Please contact support - [{0}] - {1}'.format(
                    auth_test.status_code, auth_test.json()["message"])
                logging.error(err_msg)
                sys.exit(1)

    def make_post(self, url, request_body):
        """
        Sets max retries and backoff factor for POSTs and makes POST call.
        TODO: Can this be placed elsewhere?
        Args:
            url: complete target url
            request_body: json that will be sent in POST

        Returns:
            None
        """

        s = Session()
        s.mount('https://',
                HTTPAdapter(
                    max_retries=Retry(
                        total=5,
                        backoff_factor=1,
                        status_forcelist=[500, 502, 503, 504, 521],
                        allowed_methods=frozenset(['GET', 'POST']))))

        response = s.post(
            url, headers=self.base_headers,
            params=self.base_params, json=request_body)

        if response.status_code not in (200, 201):
            response_json = response.json()
            try:
                logging.info(
                    f'{response_json["message"]} - {request_body["properties"]}')
            except KeyError:
                logging.info(response_json["message"])

    def _construct_request_body(self, endpoint, data_in):

        if endpoint == 'create_contact':
            for row in data_in:
                request_body = {
                    'properties': []
                }
                for k, v in row.items():
                    tmp = {
                        'property': k,
                        'value': str(v)
                    }
                    request_body['properties'].append(tmp)

                self.make_post(
                    url=f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}',
                    request_body=request_body
                )

        elif endpoint == 'create_list':
            for row in data_in:
                request_body = {
                    'name': str(row['name'])
                }

                self.make_post(
                    url=f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}',
                    request_body=request_body
                )

        elif endpoint == 'add_contact_to_list':

            ordered_dict = list(data_in)
            unique_list_ids = set()

            for row in ordered_dict:
                unique_list_ids.add(row["list_id"])

            for list_id in unique_list_ids:
                vids = []
                emails = []

                if list_id == "":
                    # Ensuring all list_id inputs are not empty
                    logging.error("Column [list_id] cannot be empty")
                    sys.exit(1)

                temp_list_of_dicts = ([x for x in ordered_dict if x["list_id"] == list_id])
                for dic in temp_list_of_dicts:
                    if dic["vids"]:
                        vids.append(dic["vids"])
                    else:
                        emails.append(dic["emails"])

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace(
                    '{list_id}', str(list_id))

                request_body = {
                    "vids": vids,
                    "emails": emails
                }

                self.make_post(
                    url=f'{self.base_url}{endpoint_path}',
                    request_body=request_body
                )

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
                        f'{response_json["message"]}')

        elif endpoint == 'update_contact' or endpoint == 'update_contact_by_email':

            wildcard = 'vid' if endpoint == 'update_contact' else 'email'

            headers = list(data_in.columns)
            headers.remove(wildcard)

            for index, row in data_in.iterrows():

                logging.info(f'Updating contact [{row[wildcard]}]')

                if row[wildcard] == '' or pd.isnull(row[wildcard]):
                    logging.error(row)
                    logging.error(f'[{wildcard}] cannot be empty.')
                    continue

                request_body = {
                    'properties': []
                }

                # Request parameters
                endpoint_url = ENDPOINT_MAPPING[endpoint]['endpoint'].replace(
                    f'{{{wildcard}}}', str(row[wildcard]))

                for h in headers:
                    temp_json = {
                        'property': h,
                        'value': row[h] if not pd.isnull(row[h]) else ''
                    }

                    request_body['properties'].append(temp_json)

                # Requests handler
                response = requests.post(
                    f'{self.base_url}{endpoint_url}', headers=self.base_headers,
                    params=self.base_params, json=request_body)

                if response.status_code not in (200, 201, 204):
                    response_json = response.json()
                    logging.info(
                        f'{response_json["message"]}')


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
