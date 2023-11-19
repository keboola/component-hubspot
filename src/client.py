from abc import ABC, abstractmethod
import csv
from functools import wraps
import time
from collections import defaultdict

from requests import Session, get
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.exceptions import RequestException, HTTPError

from exceptions import UserException
from endpoint_mapping import ENDPOINT_MAPPING
from typing import Literal, Union

import logging

BATCH_SIZE = 100
LOGGING_INTERVAL = 200
SLEEP_INTERVAL = 0.1  # https://developers.hubspot.com/docs/api/usage-details#rate-limits


def batched(batch_size=BATCH_SIZE, logging_interval=LOGGING_INTERVAL, sleep_interval=SLEEP_INTERVAL):
    def wrapper(func):
        @wraps(func)
        def inner(self, data):
            data_batch = []
            for i, record in enumerate(data, start=1):
                data_batch.append(record)
                if not i % batch_size:
                    func(self, data_batch)
                    time.sleep(sleep_interval)
                    data_batch = []
                if not i % logging_interval:
                    logging.info(f'Processed {i} rows.')
            if data_batch:
                func(self, data_batch)
        return inner
    return wrapper


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
                             status_forcelist=[500, 502, 503, 504, 521, 429],
                             allowed_methods=frozenset(['POST', 'PUT', 'DELETE']))))

    @abstractmethod
    def process_requests(self, data_reader: csv.DictReader) -> None:
        """
        Handles the assembly of URLs to call and request bodies to send.
        Args:
            data_reader: csv.DictReader with loaded csv

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

        if method in ["post", "put", "delete", "patch"]:
            response = self.s.request(method, url, headers=self.base_headers,
                                      params=self.base_params, json=request_body)
            try:
                response.raise_for_status()
            except RequestException as e:
                response_content = response.content if response is not None else None
                raise UserException(
                    f"Cannot process record {request_body}, HTTP error: {e}, response content: {response_content}"
                )
        else:
            raise UserException(f"Method {method} not allowed.")

    def make_batch_request(self, inputs: list):
        """
        Makes a batch request with the HubSpot specified data body.
        Args:
            inputs: list of individual HubsSpot objects to be created/updated

        Returns:
            None
        """
        self.make_request(
            url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
            request_body={'inputs': inputs},
            method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateContact(HubSpotClient):
    """Creates contacts in batches"""
    @batched()
    def process_requests(self, data_reader):
        inputs = [{"properties": {k: str(v) for k, v in row.items()}} for row in data_reader]
        self.make_batch_request(inputs)


class CreateList(HubSpotClient):
    """Creates a new contact list"""

    def process_requests(self, data_reader):
        for row in data_reader:
            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body={'name': str(row['name'])},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class AddContactToList(HubSpotClient):
    """Adds contacts to list"""

    def process_requests(self, data_reader):
        rows_by_list_id = defaultdict(list)
        for row in data_reader:
            if not row['list_id']:
                raise UserException('Column [list_id] cannot be empty.')
            rows_by_list_id[row['list_id']].append(row)

        for list_id, rows in rows_by_list_id.items():
            vids = []
            emails = []
            for row in rows:
                if row["vids"]:
                    vids.append(row["vids"])
                else:
                    emails.append(row["emails"])

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{list_id}', str(list_id))
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body={"vids": vids, "emails": emails},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveContactFromList(HubSpotClient):
    """Removes contacts from lists"""

    def process_requests(self, data_reader):
        rows_by_list_id = defaultdict(list)
        for row in data_reader:
            if not row['list_id']:
                raise UserException('Column [list_id] cannot be empty.')
            rows_by_list_id[row['list_id']].append(row)

        for list_id, rows in rows_by_list_id.items():
            vids = []
            for row in rows:
                if not row['vids']:
                    raise UserException(f"Cannot process list with empty records in [vids] column. {row}")
                vids.append(row['vids'])

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(list_id=list_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body={'vids': vids},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateContact(HubSpotClient):
    """Updates contacts"""

    @batched()
    def process_requests(self, data_reader):
        inputs = []
        for row in data_reader:
            if not row["vid"]:
                raise UserException(f"Cannot process list with empty records in [vid] column. {row}")

            inputs.append({
                "id": row.pop('vid'),
                "properties": {k: str(v) for k, v in row.items()}
            })
        self.make_batch_request(inputs)


class UpdateContactByEmail(HubSpotClient):
    """Updates contacts using email as ID"""

    def process_requests(self, data_reader):
        for row in data_reader:
            if not row["email"]:
                raise UserException(f"Cannot process list with empty records in [email] column. {row}")

            email = row.pop('email')
            request_body = {'properties': [{'property': k, 'value': str(v)} for k, v in row.items()]}
            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(email=email)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateCompany(HubSpotClient):
    """Creates company"""

    @batched()
    def process_requests(self, data_reader):
        inputs = []
        for row in data_reader:
            if not row["name"]:
                raise UserException(f"Cannot process company with empty records in [name] column. {row}")
            properties = {k: str(v) for k, v in row.items()}
            inputs.append({"properties": properties})
        self.make_batch_request(inputs)


class UpdateCompany(HubSpotClient):
    """Updates company using company ID"""

    @batched()
    def process_requests(self, data_reader):
        inputs = []
        for row in data_reader:
            if not row["company_id"]:
                raise UserException(f"Cannot process list with empty records in [company_id] column. {row}")

            inputs.append({
                "id": row.pop("company_id"),
                "properties": {k: str(v) for k, v in row.items()}
            })
        self.make_batch_request(inputs)


class RemoveCompany(HubSpotClient):
    """Removes company using company_id"""

    def process_requests(self, data_reader):
        company_ids = set(row['company_id'] for row in data_reader)
        if '' in company_ids:
            UserException(f"Cannot process list with empty records in [company_id] column.")

        for company_id in company_ids:
            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(company_id=company_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=None,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateDeal(HubSpotClient):
    """Creates deals"""

    @batched()
    def process_requests(self, data_reader):
        # /crm/v3/objects/deals/batch/create
        inputs = []
        for row in data_reader:
            if not row["hubspot_owner_id"]:
                raise UserException(f"Cannot process deal with empty record in [hubspot_owner_id] column. {row}")

            inputs.append({"properties": row})
        self.make_batch_request(inputs)


class UpdateDeal(HubSpotClient):
    """Updates company using Deal ID"""

    @batched()
    def process_requests(self, data_reader):
        # /crm/v3/objects/deals/batch/update
        inputs = []
        for row in data_reader:
            if not row["deal_id"]:
                raise UserException(f"Cannot process deal with empty records in [deal_id] column. {row}")

            inputs.append({
                "id": row.pop('deal_id'),
                "properties": row
            })
        self.make_batch_request(inputs)


class RemoveDeal(HubSpotClient):
    """Removes Deal using Deal ID"""
    def process_requests(self, data_reader):
        deal_ids = set(row["deal_id"] for row in data_reader)
        if '' in deal_ids:
            raise UserException(f"Cannot process deal with empty records in [deal_id] column.")

        for deal_id in deal_ids:
            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(deal_id=deal_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
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
    auth_param = {'count': 1}
    auth_headers = {}

    if auth_type == 'API Key':
        auth_param['hapikey'] = token
    else:
        auth_headers['Authorization'] = f'Bearer {token}'

    try:
        auth_test = get(auth_url, params=auth_param, headers=auth_headers)
        auth_test.raise_for_status()
    except HTTPError as e:
        raise UserException(f"Cannot reach Hubspot API, please check your credentials. "
                            f"Error code: {auth_test.status_code}. "
                            f"Message: {auth_test.json()['message']}") from e

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
        "contact_create": CreateContact,
        "list_create": CreateList,
        "contact_add_to_list": AddContactToList,
        "contact_remove_from_list": RemoveContactFromList,
        "contact_update": UpdateContact,
        "contact_update_by_email": UpdateContactByEmail,
        "company_create": CreateCompany,
        "company_update": UpdateCompany,
        "company_remove": RemoveCompany,
        "deal_create": CreateDeal,
        "deal_update": UpdateDeal,
        "deal_remove": RemoveDeal
    }

    if endpoint in endpoints:
        return endpoints[endpoint](endpoint, token, auth_type)
    raise UserException(f"Unknown endpoint option: {endpoint}.")


def run(endpoint: str, data_reader: csv.DictReader, token: str, auth_type: Literal["API Key", "Private App Token"]) -> None:
    """
    Main entrypoint to call.
    Args:
        auth_type: "API Key" or "Private App Token"
        token: API key for Hubspot API
        endpoint: Hubspot API endpoint
        data_reader: csv.DictReader object with data from input csv

    Returns:
        None
    """

    factory = get_factory(endpoint, token, auth_type)
    factory.process_requests(data_reader)
