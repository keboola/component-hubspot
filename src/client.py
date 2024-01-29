from abc import ABC, abstractmethod
import csv
from functools import wraps
import time
from collections import defaultdict

from requests.models import Response
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
ERRORS_TABLE_COLUMNS = ['status', 'category', 'message', 'context']


def batched(batch_size=BATCH_SIZE, logging_interval=LOGGING_INTERVAL, sleep_interval=SLEEP_INTERVAL):
    def wrapper(func):
        @wraps(func)
        def inner(self, data, *args, **kwargs):
            data_batch = []
            for i, record in enumerate(data, start=1):
                data_batch.append(record)
                if not i % batch_size:
                    func(self, data_batch, *args, **kwargs)
                    time.sleep(sleep_interval)
                    data_batch = []
                if not i % logging_interval:
                    logging.info(f'Processed {i} rows.')
            if data_batch:
                func(self, data_batch, *args, **kwargs)
        return inner
    return wrapper


def get_rows_by_list_id(data_reader):
    rows_by_list_id = defaultdict(list)
    for row in data_reader:
        if not row['list_id']:
            raise UserException('Column [list_id] cannot be empty.')
        rows_by_list_id[row['list_id']].append(row)
    return rows_by_list_id


def get_vids_from_rows(rows):
    vids = []
    for row in rows:
        if not row['vids']:
            raise UserException(f"Cannot process list with empty records in [vids] column. {row}")
        vids.append(row['vids'])
    return vids


class HubSpotClient(ABC):
    """Template for classes handling communication with Hubspot API"""

    def __init__(self, endpoint: str, token: str, error_writer: csv.DictWriter):
        # Base parameters for the requests
        self.base_url = 'https://api.hubapi.com/'
        self.endpoint = endpoint
        self.base_params = {}
        self.base_headers = {'Authorization': f'Bearer {token}'}
        self.error_writer = error_writer

        self.s = Session()
        self.s.mount('https://',
                     HTTPAdapter(
                         max_retries=Retry(
                             total=5,
                             backoff_factor=0.3,  # {backoff factor} * (2 ** ({number of total retries} - 1))
                             status_forcelist=[500, 502, 503, 504, 521],
                             allowed_methods=frozenset(['POST', 'PUT', 'DELETE']))))

    @abstractmethod
    def process_requests(self, data_reader) -> None:
        """
        Handles the assembly of URLs to call and request bodies to send.
        Args:
            data_reader: csv.DictReader with loaded csv
        Returns:
            None
        """

    def log_batch_errors(self, response):
        self.error_writer.errors = True
        for error in response.json()['errors']:
            self.error_writer.writerow(error)

    def log_errors(self, response):
        self.error_writer.errors = True
        error = response.json()
        error_row = {
            field: error.get(field)
            for field in ERRORS_TABLE_COLUMNS
        }
        self.error_writer.writerow(error_row)

    def make_request(self, url: str, request_body: Union[dict, None],
                     method: Literal["post", "put", "delete"]) -> Response:
        """
        Makes Post/Put/Delete calls to target url.
        Args:
            url: complete target url
            request_body: dict that will be sent in POST
            method: post/put/delete defined in endpoint_mapping.py

        Returns:
            response
        """

        if method not in ["post", "put", "delete", "patch"]:
            raise UserException(f"Method {method} not allowed.")

        response = self.s.request(method, url, headers=self.base_headers,
                                  params=self.base_params, json=request_body)
        try:
            response.raise_for_status()
        except RequestException:
            self.log_errors(response)

        return response

    def make_batch_request(self, inputs: list):
        """
        Makes a batch request with the HubSpot specified data body.
        Args:
            inputs: list of individual HubsSpot objects to be created/updated
        Returns:
            None
        """
        url = f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}'
        method = ENDPOINT_MAPPING[self.endpoint]["method"]
        response = self.make_request(url=url, request_body={'inputs': inputs}, method=method)

        if response.status_code == 207:
            logging.error(f"{method} request to {url} partially failed with status code 207")
            self.log_batch_errors(response)


class CreateContact(HubSpotClient):
    """Creates contacts in batches"""

    @batched()
    def process_requests(self, data_reader):
        inputs = [{"properties": {k: str(v) for k, v in row.items()}} for row in data_reader]
        self.make_batch_request(inputs)


class CreateContactList(HubSpotClient):
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


class CreateCustomList(HubSpotClient):
    """Creates list for custom objects specified in the input table via object_type column"""

    def process_requests(self, data_reader):
        object_types_to_id = {
            'contact': '0-1',
            'company': '0-2',
            'deal': '0-3'
        }
        for row in data_reader:
            request_body = {
                'name': str(row['name']),
                'processingType': 'MANUAL',
                'objectTypeId': object_types_to_id[row['object_type']]
            }
            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class AddContactToList(HubSpotClient):
    """Adds contacts to list"""

    def process_requests(self, data_reader):
        rows_by_list_id = get_rows_by_list_id(data_reader)

        for list_id, rows in rows_by_list_id.items():
            vids = []
            emails = []
            for row in rows:
                if row["vids"]:
                    vids.append(row["vids"])
                else:
                    emails.append(row["emails"])

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(list_id=list_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body={"vids": vids, "emails": emails},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveContactFromList(HubSpotClient):
    """Removes contacts from lists"""

    def process_requests(self, data_reader):
        rows_by_list_id = get_rows_by_list_id(data_reader)

        for list_id, rows in rows_by_list_id.items():
            vids = get_vids_from_rows(rows)

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


class AddObjectToList(HubSpotClient):
    """Parent class for adding Objects to list using List ID and Object ID"""

    def process_requests(self, data_reader) -> None:
        rows_by_list_id = get_rows_by_list_id(data_reader)

        for list_id, rows in rows_by_list_id.items():
            vids = get_vids_from_rows(rows)

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(list_id=list_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body={'recordIdsToAdd': vids},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class AddCompanyToList(AddObjectToList):
    """Adds companies to list"""


class AddDealToList(AddObjectToList):
    """Adds deals to list"""


class RemoveObjectFromList(HubSpotClient):
    """Parent class for removing Objects from list using List ID and Object ID"""

    def process_requests(self, data_reader):
        rows_by_list_id = get_rows_by_list_id(data_reader)

        for list_id, rows in rows_by_list_id.items():
            vids = get_vids_from_rows(rows)

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].format(list_id=list_id)
            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body={'recordIdsToRemove': vids},
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveCompanyFromList(RemoveObjectFromList):
    """Removes companies from list"""


class RemoveDealFromList(RemoveObjectFromList):
    """Removes deals from list"""


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


class CreateAssociatedObject(HubSpotClient):
    """Parent class to CRM objects with association - creates objects"""

    @batched()
    def process_requests(self, data_reader):
        inputs = []
        for row in data_reader:
            if not row["association_id"]:
                raise UserException(f"Cannot process object with empty record in [association_id] column. {row}")
            associations = [{
                'to': {'id': str(row.pop('association_id'))},
                'types': [{
                    'associationCategory': row.pop('association_category'),
                    'associationTypeId': row.pop('association_type_id')
                }]
            }]
            inputs.append({"associations": associations, "properties": row})
        self.make_batch_request(inputs)


class CreateTicket(CreateAssociatedObject):
    """Creates tickets"""


class CreateProduct(CreateAssociatedObject):
    """Creates products"""


class CreateQuote(CreateAssociatedObject):
    """Creates quotes"""


class CreateLineItem(CreateAssociatedObject):
    """Creates line items"""


class CreateTax(CreateAssociatedObject):
    """Creates taxes"""


class CreateCall(CreateAssociatedObject):
    """Creates calls"""


class CreateCommunication(CreateAssociatedObject):
    """Creates communications"""


class CreateEmail(CreateAssociatedObject):
    """Creates emails"""


class CreateMeeting(CreateAssociatedObject):
    """Creates meetings"""


class CreateNote(CreateAssociatedObject):
    """Creates notes"""


class CreatePostalMail(CreateAssociatedObject):
    """Creates postal_mails"""


class CreateTask(CreateAssociatedObject):
    """Creates tasks"""


class UpdateObject(HubSpotClient, ABC):
    """Parent class to CRM objects - updates objects"""

    @property
    @abstractmethod
    def object_type(self) -> str:
        pass

    @batched()
    def process_requests(self, data_reader):
        inputs = []
        for row in data_reader:
            if not row[f"{self.object_type}_id"]:
                raise UserException(f"Cannot process {self.object_type} with empty records "
                                    f"in [{self.object_type}_id] column. {row}")

            inputs.append({
                "id": str(row.pop(f'{self.object_type}_id')),
                "properties": row
            })
        self.make_batch_request(inputs)


class UpdateDeal(UpdateObject):
    """Updates Deal using deal_id"""

    @property
    def object_type(self) -> str:
        return 'deal'


class UpdateTicket(UpdateObject):
    """Updates Ticket using ticket_id"""

    @property
    def object_type(self) -> str:
        return 'ticket'


class UpdateProduct(UpdateObject):
    """Updates Product using product_id"""

    @property
    def object_type(self) -> str:
        return 'product'


class UpdateQuote(UpdateObject):
    """Updates Quote using quote_id"""

    @property
    def object_type(self) -> str:
        return 'quote'


class UpdateLineItem(UpdateObject):
    """Updates Line item using line_item_id"""

    @property
    def object_type(self) -> str:
        return 'line_item'


class UpdateTax(UpdateObject):
    """Updates Tax using tax_id"""

    @property
    def object_type(self) -> str:
        return 'tax'


class UpdateCall(UpdateObject):
    """Updates Call using call_id"""

    @property
    def object_type(self) -> str:
        return 'call'


class UpdateCommunication(UpdateObject):
    """Updates Communication using communication_id"""

    @property
    def object_type(self) -> str:
        return 'communication'


class UpdateEmail(UpdateObject):
    """Updates Email using email_id"""

    @property
    def object_type(self) -> str:
        return 'email'


class UpdateMeeting(UpdateObject):
    """Updates Meeting using meeting_id"""

    @property
    def object_type(self) -> str:
        return 'meeting'


class UpdateNote(UpdateObject):
    """Updates Note using note_id"""

    @property
    def object_type(self) -> str:
        return 'note'


class UpdatePostalMail(UpdateObject):
    """Updates PostalMail using postal_mail_id"""

    @property
    def object_type(self) -> str:
        return 'postal_mail'


class UpdateTask(UpdateObject):
    """Updates Task using task_id"""

    @property
    def object_type(self) -> str:
        return 'task'


class RemoveObject(HubSpotClient, ABC):
    """Parent class to CRM/engagement objects - removes CRM/engagement Object using Object ID"""

    @property
    @abstractmethod
    def object_type(self) -> str:
        pass

    @batched()
    def process_requests(self, data_reader):
        inputs = [{"id": str(row[f"{self.object_type}_id"])} for row in data_reader]
        self.make_batch_request(inputs)


class RemoveCompany(RemoveObject):
    """Removes Company using company_id"""

    @property
    def object_type(self) -> str:
        return 'company'


class RemoveDeal(RemoveObject):
    """Removes Deal using deal_id"""

    @property
    def object_type(self) -> str:
        return 'deal'


class RemoveTicket(RemoveObject):
    """Removes Ticket using ticket_id"""

    @property
    def object_type(self) -> str:
        return 'ticket'


class RemoveProduct(RemoveObject):
    """Removes Product using product_id"""
    @property
    def object_type(self) -> str:
        return 'product'


class RemoveQuote(RemoveObject):
    """Removes Quote using quote_id"""

    @property
    def object_type(self) -> str:
        return 'quote'


class RemoveLineItem(RemoveObject):
    """Removes Line item using line_item_id"""
    @property
    def object_type(self) -> str:
        return 'line_item'


class RemoveTax(RemoveObject):
    """Removes Tax item using tax_id"""

    @property
    def object_type(self) -> str:
        return 'tax'


class RemoveCall(RemoveObject):
    """Removes Call using call_id"""

    @property
    def object_type(self) -> str:
        return 'call'


class RemoveCommunication(RemoveObject):
    """Removes Communication using communication_id"""

    @property
    def object_type(self) -> str:
        return 'communication'


class RemoveEmail(RemoveObject):
    """Removes Email using email_id"""

    @property
    def object_type(self) -> str:
        return 'email'


class RemoveMeeting(RemoveObject):
    """Removes Meeting using meeting_id"""

    @property
    def object_type(self) -> str:
        return 'meeting'


class RemoveNote(RemoveObject):
    """Removes Note using note_id"""

    @property
    def object_type(self) -> str:
        return 'note'


class RemovePostalMail(RemoveObject):
    """Removes PostalMail using postal_mail_id"""

    @property
    def object_type(self) -> str:
        return 'postal_mail'


class RemoveTask(RemoveObject):
    """Removes Task using task_id"""

    @property
    def object_type(self) -> str:
        return 'task'


def test_credentials(token: str) -> bool:
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
    auth_headers = {'Authorization': f'Bearer {token}'}

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


def get_factory(endpoint: str, token: str, error_writer: csv.DictWriter) -> HubSpotClient:
    """Constructs an exporter factory based on endpoint selection

    Args:
        endpoint: Hubspot API endpoint set in config.json
        token: Private App Token for Hubspot API
        error_writer: csv.DictWriter for request errors
    """

    endpoints = {
        "contact_create": CreateContact,
        "list_create": CreateContactList,
        "custom_list_create": CreateCustomList,
        "contact_add_to_list": AddContactToList,
        "contact_remove_from_list": RemoveContactFromList,
        "contact_update": UpdateContact,
        "contact_update_by_email": UpdateContactByEmail,
        "company_create": CreateCompany,
        "company_update": UpdateCompany,
        "company_remove": RemoveCompany,
        "company_add_to_list": AddCompanyToList,
        "company_remove_from_list": RemoveCompanyFromList,
        "deal_create": CreateDeal,
        "deal_update": UpdateDeal,
        "deal_remove": RemoveDeal,
        "deal_add_to_list": AddDealToList,
        "deal_remove_from_list": RemoveDealFromList,
        "ticket_create": CreateTicket,
        "ticket_update": UpdateTicket,
        "ticket_remove": RemoveTicket,
        "product_create": CreateProduct,
        "product_update": UpdateProduct,
        "product_remove": RemoveProduct,
        "quote_create": CreateQuote,
        "quote_update": UpdateQuote,
        "quote_remove": RemoveQuote,
        "line_item_create": CreateLineItem,
        "line_item_update": UpdateLineItem,
        "line_item_remove": RemoveLineItem,
        "tax_create": CreateTax,
        "tax_update": UpdateTax,
        "tax_remove": RemoveTax,
        "call_create": CreateCall,
        "call_update": UpdateCall,
        "call_remove": RemoveCall,
        "communication_create": CreateCommunication,
        "communication_update": UpdateCommunication,
        "communication_remove": RemoveCommunication,
        "email_create": CreateEmail,
        "email_update": UpdateEmail,
        "email_remove": RemoveEmail,
        "meeting_create": CreateMeeting,
        "meeting_update": UpdateMeeting,
        "meeting_remove": RemoveMeeting,
        "note_create": CreateNote,
        "note_update": UpdateNote,
        "note_remove": RemoveNote,
        "postal_mail_create": CreatePostalMail,
        "postal_mail_update": UpdatePostalMail,
        "postal_mail_remove": RemovePostalMail,
        "task_create": CreateTask,
        "task_update": UpdateTask,
        "task_remove": RemoveTask
    }

    if endpoint in endpoints:
        return endpoints[endpoint](endpoint, token, error_writer)
    raise UserException(f"Unknown endpoint option: {endpoint}.")


def run(endpoint: str, data_reader: csv.DictReader, error_writer: csv.DictWriter, token: str) -> None:
    """
    Main entrypoint to call.
    Args:
        token: Private App Token for Hubspot API
        endpoint: Hubspot API endpoint
        data_reader: csv.DictReader object with data from input csv
        error_writer: csv.DictWriter object to log 207 status_code events

    Returns:
        None
    """
    factory = get_factory(endpoint, token, error_writer)
    factory.process_requests(data_reader)
