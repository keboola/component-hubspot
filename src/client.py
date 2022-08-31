from abc import ABC, abstractmethod
import logging
import csv

from requests import Session, get
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from exceptions import UserException
from endpoint_mapping import ENDPOINT_MAPPING
from typing import Literal


class HubSpotClient(ABC):
    """Template for classes handling communication with Hubspot API"""

    def __init__(self, endpoint: str, hapikey: str):
        # Base parameters for the requests
        self.base_url = 'https://api.hubapi.com/'
        self.endpoint = endpoint
        self.hapikey = hapikey
        self.base_headers = {
            'Content-Type': 'application/json'
        }
        self.base_params = {
            'hapikey': self.hapikey
        }

        self.s = Session()
        self.s.mount('https://',
                     HTTPAdapter(
                         max_retries=Retry(
                             total=10,
                             backoff_factor=1,  # {backoff factor} * (2 ** ({number of total retries} - 1))
                             status_forcelist=[500, 502, 503, 504, 521, 404],
                             allowed_methods=frozenset(['POST', 'PUT', 'DELETE']))))

    @abstractmethod
    def process_requests(self, data_in: csv.DictReader) -> None:
        """
        Handles the assembly of URLs to call and request bodies to send.
        Args:
            data_in: csv.DictReader with loaded csv

        Returns:
            None
        """

    def make_request(self, url: str, request_body: dict, method: Literal["post", "put", "delete"]) -> None:
        """
        Makes Post/Put/Delete calls to target url.
        Args:
            url: complete target url
            request_body: dict that will be sent in POST
            method: post/put/delete defined in endpoint_mapping.py

        Returns:
            None
        """

        if method in ["post", "put", "delete"]:
            response = self.s.request(method,
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


class CreateContact(HubSpotClient):
    """Creates contacts"""

    def process_requests(self, data_in):
        for row in data_in:
            request_body = {
                'properties': []
            }
            for k, v in row.items():
                tmp = {
                    "property": k,
                    "value": str(v)
                }
                request_body['properties'].append(tmp)

            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateList(HubSpotClient):
    """Creates a new contact list"""

    def process_requests(self, data_in):
        for row in data_in:
            request_body = {
                'name': str(row['name'])
            }

            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class AddContactToList(HubSpotClient):
    """Adds contacts to list"""

    def process_requests(self, data_in):
        ordered_dict = list(data_in)
        unique_list_ids = set()

        for row in ordered_dict:
            unique_list_ids.add(row["list_id"])

        for list_id in unique_list_ids:
            vids = []
            emails = []

            if list_id == "":
                # Ensuring all list_id inputs are not empty
                raise UserException("Column [list_id] cannot be empty")

            temp_list_of_dicts = ([x for x in ordered_dict if x["list_id"] == list_id])
            for dic in temp_list_of_dicts:
                if dic["vids"]:
                    vids.append(dic["vids"])
                else:
                    emails.append(dic["emails"])

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{list_id}', str(list_id))

            request_body = {
                "vids": vids,
                "emails": emails
            }

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveContactFromList(HubSpotClient):
    """Removes contacts from lists"""

    def process_requests(self, data_in):
        ordered_dict = list(data_in)
        unique_list_ids = set()

        for row in ordered_dict:
            unique_list_ids.add(row["list_id"])

        for list_id in unique_list_ids:
            vids = []

            if list_id == '':
                raise UserException('Column [list_id] cannot be empty.')

            temp_list_of_dicts = ([x for x in ordered_dict if x["list_id"] == list_id])
            for dic in temp_list_of_dicts:
                if dic["vids"] == "":
                    raise UserException(f"Cannot process list with empty records in [vids] column. {dic}")
                vids.append(dic["vids"])

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{list_id}', str(list_id))
            request_body = {
                "vids": vids
            }

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateContact(HubSpotClient):
    """Updates contacts"""

    def process_requests(self, data_in):
        for row in data_in:
            if row["vid"] == "":
                raise UserException(f"Cannot process list with empty records in [vid] column. {row}")
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{vid}', str(row["vid"]))

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateContactByEmail(HubSpotClient):
    """Updates contacts using email as ID"""

    def process_requests(self, data_in):
        for row in data_in:
            if row["email"] == "":
                raise UserException(f"Cannot process list with empty records in [email] column. {row}")
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{email}', str(row["email"]))

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateCompany(HubSpotClient):
    """Creates company"""

    def process_requests(self, data_in):
        for row in data_in:
            if row["name"] == "":
                raise UserException(f"Cannot process list with empty records in [name] column. {row}")
            request_body = {
                'properties': []
            }
            for k, v in row.items():
                tmp = {
                    "name": k,
                    "value": str(v)
                }
                request_body['properties'].append(tmp)

            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateCompany(HubSpotClient):
    """Updates company using company ID"""

    def process_requests(self, data_in):
        for row in data_in:
            if row["company_id"] == "":
                raise UserException(f"Cannot process list with empty records in [company_id] column. {row}")
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{company_id}', str(row["company_id"]))
            url = f'{self.base_url}{endpoint_path}'

            self.make_request(
                url=url,
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveCompany(HubSpotClient):
    """Removes company using company_id"""

    def process_requests(self, data_in):
        unique_list_ids = set()
        for row in data_in:
            if row["company_id"] == "":
                raise UserException(f"Cannot process list with empty records in [company_id] column. {row}")
            unique_list_ids.add(row["company_id"])

        for company_id in unique_list_ids:
            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{company_id}', company_id)
            url = f'{self.base_url}{endpoint_path}'

            self.make_request(
                url=url,
                request_body=None,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


def test_credentials(hapikey: str) -> bool:
    """
    Uses 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent' endpoint to check the validity of hapikey.
    Returns:
        True if auth check succeeds
    Raises:
        UserException when auth fails.
    """
    # Authentication Check to ensure the API token is valid
    auth_url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent'
    auth_param = {
        'count': 1,
        'hapikey': hapikey
    }

    auth_test = get(auth_url, params=auth_param)
    if auth_test.status_code not in (200, 201):
        expected_error_msg = f'This hapikey ({hapikey}) does not exist.'
        if auth_test.json()['message'] == expected_error_msg:
            raise UserException('Authentication Error. Please check your API token.')
        else:
            err_msg = 'Unexpected error. Please contact support - [{0}] - {1}'.format(
                auth_test.status_code, auth_test.json()["message"])
            raise Exception(err_msg)
    return True


def get_factory(endpoint: str, hapikey: str) -> HubSpotClient:
    """Constructs an exporter factory based on endpoint selection"""

    endpoints = {
        "contact_create": CreateContact(endpoint, hapikey),
        "list_create": CreateList(endpoint, hapikey),
        "contact_add_to_list": AddContactToList(endpoint, hapikey),
        "contact_remove_from_list": RemoveContactFromList(endpoint, hapikey),
        "contact_update": UpdateContact(endpoint, hapikey),
        "contact_update_by_email": UpdateContactByEmail(endpoint, hapikey),
        "company_create": CreateCompany(endpoint, hapikey),
        "company_update": UpdateCompany(endpoint, hapikey),
        "company_remove": RemoveCompany(endpoint, hapikey)
    }

    if endpoint in endpoints:
        return endpoints[endpoint]
    raise UserException(f"Unknown endpoint option: {endpoint}.")


def run(endpoint: str, data_in: csv.DictReader, hapikey: str) -> None:
    """
    Args:
        endpoint: Hubspot API endpoint set in config.json
        data_in: csv.DictReader object with data from input csv
        hapikey: API key for Hubspot API

    Returns:
        None
    """

    factory = get_factory(endpoint, hapikey)
    factory.process_requests(data_in)
