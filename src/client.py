from abc import ABC, abstractmethod
import logging
import csv

from requests import Session, get
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.exceptions import HTTPError


from exceptions import UserException
from endpoint_mapping import ENDPOINT_MAPPING
from typing import Literal, Union


class HubSpotClient(ABC):
    """Template for classes handling communication with Hubspot API"""

    def __init__(self, endpoint: str, token: str, auth_type: Literal["API Key", "Private App Token"]):
        # Base parameters for the requests
        self.base_url = 'https://api.hubapi.com/'
        self.endpoint = endpoint
        self.base_headers = {
            'Content-Type': 'application/json'
        }
        if auth_type == 'API Key':
            self.base_params = {
                'hapikey': token
            }
            self.base_headers = {}
        else:
            self.base_params = {}
            self.base_headers = {'Authorization': f'Bearer {token}'}

        self.s = Session()
        self.s.mount('https://',
                     HTTPAdapter(
                         max_retries=Retry(
                             total=5,
                             backoff_factor=0.3,  # {backoff factor} * (2 ** ({number of total retries} - 1))
                             status_forcelist=[500, 502, 503, 504, 521],
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

    def make_request(self, url: str, request_body: Union[dict, None], method: Literal["post", "put", "delete"]) -> None:
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
                    f'{response.text} - {response_json["message"]} - {request_body["properties"]}')
            except KeyError:
                raise UserException(f"Error: {response_json['message']}")
            except Exception as e:
                logging.error(response.text)
                raise ValueError(f"Unresolvable error: {e} with response {response}.")


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


def test_credentials(token: str, auth_type: Literal["API Key", "Private App Token"]) -> bool:
    """
    Uses 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent' endpoint to check the validity of token.
    Returns:
        True if auth check succeeds
    Raises:
        UserException when auth fails.
    """
    # Authentication Check to ensure the API token is valid
    auth_url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent'

    if auth_type == 'API Key':
        auth_param = {
            'count': 1,
            'hapikey': token
        }
        auth_headers = {}
    else:
        auth_param = {
            'count': 1
        }
        auth_headers = {'Authorization': f'Bearer {token}'}

    try:
        auth_test = get(auth_url, params=auth_param, headers=auth_headers)
        auth_test.raise_for_status()
    except HTTPError as e:
        raise UserException(f"Cannot reach Hubspot API. Error code: {auth_test.status_code}.") from e

    if auth_test.status_code not in (200, 201):
        raise UserException(f"Auth check was not successful. Error: {auth_test.json()}")
    return True


def get_factory(endpoint: str, token: str, auth_type: Literal["API Key", "Private App Token"]) -> HubSpotClient:
    """Constructs an exporter factory based on endpoint selection

    Args:
        endpoint: Hubspot API endpoint set in config.json
        token: API key for Hubspot API
        auth_type: "API Key" or "Private App Token"
    """

    endpoints = {
        "contact_create": CreateContact(endpoint, token, auth_type),
        "list_create": CreateList(endpoint, token, auth_type),
        "contact_add_to_list": AddContactToList(endpoint, token, auth_type),
        "contact_remove_from_list": RemoveContactFromList(endpoint, token, auth_type),
        "contact_update": UpdateContact(endpoint, token, auth_type),
        "contact_update_by_email": UpdateContactByEmail(endpoint, token, auth_type),
        "company_create": CreateCompany(endpoint, token, auth_type),
        "company_update": UpdateCompany(endpoint, token, auth_type),
        "company_remove": RemoveCompany(endpoint, token, auth_type)
    }

    if endpoint in endpoints:
        return endpoints[endpoint]
    raise UserException(f"Unknown endpoint option: {endpoint}.")


def run(endpoint: str, data_in: csv.DictReader, token: str, auth_type: Literal["API Key", "Private App Token"]) -> None:
    """
    Main entrypoint to call.
    Args:
        auth_type: "API Key" or "Private App Token"
        token: API key for Hubspot API
        endpoint: Hubspot API endpoint
        data_in: csv.DictReader object with data from input csv

    Returns:
        None
    """

    factory = get_factory(endpoint, token, auth_type)
    factory.process_requests(data_in)
