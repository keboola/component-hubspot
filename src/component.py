'''
Hubspot Writer

'''
import logging
import json
import requests
import csv

from keboola.component.base import ComponentBase

from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session
from endpoint_mapping import ENDPOINT_MAPPING

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_ENDPOINT = 'endpoint'
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []


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
        self.base_url = 'https://api.hubapi.com/'
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

    def validate_user_input(self, in_tables):

        # 3 - Ensure an endpoint is selected and valid
        if self.params.get(KEY_ENDPOINT) not in ENDPOINT_MAPPING:
            raise Exception(f'{self.params.get(KEY_ENDPOINT)} is not a valid endpoint.')

        # 4 - Ensure there are input files
        if len(in_tables) < 1:
            raise Exception('Input tables are missing.')

        # 5 - Ensure all required columns are in the input files
        # for the respective endpoint.
        # Comparing this information with the file's manifest
        required_columns = ENDPOINT_MAPPING[self.params.get(KEY_ENDPOINT)]['required_column']

        for table in in_tables:
            with open(f'{self.tables_in_path}/{table["destination"]}.manifest', 'r') as f:
                table_manifest = json.load(f)
            table_columns = table_manifest['columns']
            missing_columns = []

            for r in required_columns:
                if r not in table_columns:
                    missing_columns.append(r)
            if missing_columns:
                raise Exception(f'Missing columns in input table {table["destination"]}: {missing_columns}')

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
                raise Exception('Authentication Error. Please check your API token.')

            else:
                err_msg = 'Unexpected error. Please contact support - [{0}] - {1}'.format(
                    auth_test.status_code, auth_test.json()["message"])
                raise Exception(err_msg)

    def make_post(self, url, request_body, method):
        """
        Sets max retries and backoff factor for POSTs and makes POST call.
        TODO: Can this be placed elsewhere?
        Args:
            url: complete target url
            request_body: json that will be sent in POST
            method: blabla

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

        if method == "post":
            response = s.post(
                url, headers=self.base_headers,
                params=self.base_params, json=request_body)
        elif method == "put":
            response = s.put(
                url, headers=self.base_headers,
                params=self.base_params, json=request_body)
        elif method == "delete":
            response = s.delete(
                url, headers=self.base_headers,
                params=self.base_params, json=request_body)
        else:
            raise f"Method {method} not allowed."

        if response.status_code not in (200, 201, 204):
            response_json = None
            try:
                response_json = response.json()
                logging.error(
                    f'{response_json["message"]} - {request_body["properties"]}')
            except KeyError:
                logging.error(response_json["message"])
            except Exception as e:
                logging.error(f"Unresolvable error: {e} with response {response}.")
                logging.error(response.text)

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
                    request_body=request_body,
                    method="post")

        elif endpoint == 'create_list':
            for row in data_in:
                request_body = {
                    'name': str(row['name'])
                }

                self.make_post(
                    url=f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}',
                    request_body=request_body,
                    method="post")

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
                    raise Exception("Column [list_id] cannot be empty")

                temp_list_of_dicts = ([x for x in ordered_dict if x["list_id"] == list_id])
                for dic in temp_list_of_dicts:
                    if dic["vids"]:
                        vids.append(dic["vids"])
                    else:
                        emails.append(dic["emails"])

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{list_id}', str(list_id))

                request_body = {
                    "vids": vids,
                    "emails": emails
                }

                self.make_post(
                    url=f'{self.base_url}{endpoint_path}',
                    request_body=request_body,
                    method="post")

        elif endpoint == 'remove_contact_from_list':

            ordered_dict = list(data_in)
            unique_list_ids = set()

            for row in ordered_dict:
                unique_list_ids.add(row["list_id"])

            for list_id in unique_list_ids:
                vids = []

                if list_id == '':
                    raise Exception('Column [list_id] cannot be empty.')

                temp_list_of_dicts = ([x for x in ordered_dict if x["list_id"] == list_id])
                for dic in temp_list_of_dicts:
                    if dic["vids"] == "":
                        raise Exception(f"Cannot process list with empty records in [vids] column. {dic}")
                    vids.append(dic["vids"])

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{list_id}', str(list_id))
                request_body = {
                    "vids": vids
                }

                self.make_post(
                    url=f'{self.base_url}{endpoint_path}',
                    request_body=request_body,
                    method="post")

        elif endpoint == 'update_contact':

            for row in data_in:
                if row["vid"] == "":
                    raise Exception(f"Cannot process list with empty records in [vid] column. {row}")
                request_body = {
                    'properties': []
                }
                for k, v in row.items():
                    if k != "vid":
                        tmp = {
                            'property': k,
                            'value': str(v)
                        }
                        request_body['properties'].append(tmp)

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{vid}', str(row["vid"]))

                self.make_post(
                    url=f'{self.base_url}{endpoint_path}',
                    request_body=request_body,
                    method="post")

        elif endpoint == 'update_contact_by_email':
            for row in data_in:
                if row["email"] == "":
                    raise Exception(f"Cannot process list with empty records in [email] column. {row}")
                request_body = {
                    'properties': []
                }
                for k, v in row.items():
                    if k != "email":
                        tmp = {
                            'property': k,
                            'value': str(v)
                        }
                        request_body['properties'].append(tmp)

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{email}', str(row["email"]))

                self.make_post(
                    url=f'{self.base_url}{endpoint_path}',
                    request_body=request_body,
                    method="post")

        elif endpoint == 'create_company':
            for row in data_in:
                if row["name"] == "":
                    raise Exception(f"Cannot process list with empty records in [name] column. {row}")
                request_body = {
                    'properties': []
                }
                for k, v in row.items():
                    tmp = {
                        "name": k,
                        "value": str(v)
                    }
                    request_body['properties'].append(tmp)

                self.make_post(
                    url=f'{self.base_url}{ENDPOINT_MAPPING[endpoint]["endpoint"]}',
                    request_body=request_body,
                    method="post")

        elif endpoint == 'update_company':
            for row in data_in:
                if row["company_id"] == "":
                    raise Exception(f"Cannot process list with empty records in [company_id] column. {row}")
                request_body = {
                    'properties': []
                }
                for k, v in row.items():
                    if k != "company_id":
                        tmp = {
                            'name': k,
                            'value': str(v)
                        }
                        request_body['properties'].append(tmp)

                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{company_id}', str(row["company_id"]))
                url = f'{self.base_url}{endpoint_path}'

                self.make_post(
                    url=url,
                    request_body=request_body,
                    method="put")

        elif endpoint == 'remove_company':
            unique_list_ids = set()
            for row in data_in:
                if row["company_id"] == "":
                    raise Exception(f"Cannot process list with empty records in [company_id] column. {row}")
                unique_list_ids.add(row["company_id"])

            for company_id in unique_list_ids:
                endpoint_path = ENDPOINT_MAPPING[endpoint]['endpoint'].replace('{company_id}', company_id)
                url = f'{self.base_url}{endpoint_path}'

                self.make_post(
                    url=url,
                    request_body=None,
                    method="delete")


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
