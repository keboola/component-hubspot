import logging
from abc import ABC, abstractmethod

from requests import Session, get
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from endpoint_mapping import ENDPOINT_MAPPING

# TODO: I like the Factory design! It's quite clear and a nice usecase for it. However:
""" The exact same thing as is done in the Creator class is abstracted in keboola.http-client (which is also listed in the requirements.txt and not used)
 It provides a nice extensibility in case that the make_request method may ever change. It most likely won't. Using separate Factory classes implementing
 the Creator makes it a bit too verbose IMO. There's no default behaviour of process_requests anyway.
 
 We usually create a HubSpotClient(HTTPClient) that uses the abstracted http client we built. The client instance would receive the token + common parameters.
 Then instead of Factory classes there would be methods, like HubSpotClient.add_contacts_to_list(). This also allows cleaner documentation of requirements for input data on the method level.
 
 The convenience "factory" method process_requests and get_factory, could actually exist in the client also. It would just return the appropriate client method. And call it directly
 e.g. method = get_endpoint_method(endpoint) (same as get_factory)
      method(input_data)
      
      
One last argument for using the keboola.HTTPClient is that it will look familiar to anyone that knows our other components.
"""


class Creator(ABC):
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
                             allowed_methods=frozenset(['GET', 'POST']))))

    @abstractmethod
    def process_requests(self, data_in) -> None:
        """
        Handles the assembly of URLs to call and request bodies to send.
        Args:
            data_in: csv.DictReader with loaded csv

        Returns:
            None
        """

    def make_request(self, url, request_body, method) -> None:
        """
        Makes Post/Put/Delete calls to target url.
        Args:
            url: complete target url
            request_body: json that will be sent in POST
            method: post/put/delete defined in endpoint_mapping.py

        Returns:
            None
        """

        if method == "post":
            response = self.s.post(
                url, headers=self.base_headers,
                params=self.base_params, json=request_body)
        elif method == "put":
            response = self.s.put(
                url, headers=self.base_headers,
                params=self.base_params, json=request_body)
        elif method == "delete":
            response = self.s.delete(
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


class CreateContact(Creator):
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


class CreateList(Creator):
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


class AddContactToList(Creator):
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
                raise Exception("Column [list_id] cannot be empty")

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
            # TODO: this is probably leftover from the old code. If needed use logging.debug()
            print(request_body)

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveContactFromList(Creator):
    """Removes contacts from lists"""

    def process_requests(self, data_in):
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{list_id}', str(list_id))
            request_body = {
                "vids": vids
            }

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateContact(Creator):
    """Updates contacts"""

    def process_requests(self, data_in):
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{vid}', str(row["vid"]))

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateContactByEmail(Creator):
    """Updates contacts using email as ID"""

    def process_requests(self, data_in):
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{email}', str(row["email"]))

            self.make_request(
                url=f'{self.base_url}{endpoint_path}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class CreateCompany(Creator):
    """Creates company"""

    def process_requests(self, data_in):
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

            self.make_request(
                url=f'{self.base_url}{ENDPOINT_MAPPING[self.endpoint]["endpoint"]}',
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class UpdateCompany(Creator):
    """Updates company using company ID"""

    def process_requests(self, data_in):
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

            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{company_id}', str(row["company_id"]))
            url = f'{self.base_url}{endpoint_path}'

            self.make_request(
                url=url,
                request_body=request_body,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])


class RemoveCompany(Creator):
    """Removes company using company_id"""

    def process_requests(self, data_in):
        unique_list_ids = set()
        for row in data_in:
            if row["company_id"] == "":
                raise Exception(f"Cannot process list with empty records in [company_id] column. {row}")
            unique_list_ids.add(row["company_id"])

        for company_id in unique_list_ids:
            endpoint_path = ENDPOINT_MAPPING[self.endpoint]['endpoint'].replace('{company_id}', company_id)
            url = f'{self.base_url}{endpoint_path}'

            self.make_request(
                url=url,
                request_body=None,
                method=ENDPOINT_MAPPING[self.endpoint]["method"])

# TODO: I would name this test_credentials. And it would take no parameter, because the auth method would be set at the client intialization.
# We will also use this for sync actions (once Product hears us out and enables this in dynamic generic UI)
def auth_check_ok(hapikey) -> bool:
    """
    Uses 'https://api.hubapi.com/contacts/v1/lists/all/contacts/recent' endpoint to check the validity of hapikey.
    Returns:
        True if auth check succeeds
    Raises:
        Exceptions when auth fails.
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
            raise Exception('Authentication Error. Please check your API token.')

        else:
            err_msg = 'Unexpected error. Please contact support - [{0}] - {1}'.format(
                auth_test.status_code, auth_test.json()["message"])
            raise Exception(err_msg)
    return True


def get_factory(endpoint, hapikey) -> Creator:
    """Constructs an exporter factory based on endpoint selection"""

    endpoints = {
        "create_contact": CreateContact(endpoint, hapikey),
        "create_list": CreateList(endpoint, hapikey),
        "add_contact_to_list": AddContactToList(endpoint, hapikey),
        "remove_contact_from_list": RemoveContactFromList(endpoint, hapikey),
        "update_contact": UpdateContact(endpoint, hapikey),
        "update_contact_by_email": UpdateContactByEmail(endpoint, hapikey),
        "create_company": CreateCompany(endpoint, hapikey),
        "update_company": UpdateCompany(endpoint, hapikey),
        "remove_company": RemoveCompany(endpoint, hapikey)
    }

    if endpoint in endpoints:
        return endpoints[endpoint]
    raise f"Unknown endpoint option: {endpoint}."


# TODO: Minor thing, but I don't like the naming "process_requests" much. Too generic
# TODO: The endpoint parameter can be of type since 3.8 [Literal](https://docs.python.org/3/library/typing.html#typing.Literal) that provides also implicit validation + type hint.
#  Or I sometime use enums but that may add unnecessary complexity https://bitbucket.org/kds_consulting_team/kds-team.ex-hubspot-crm/src/28ed88a54883d3b778ee18391c8d8c1edf9d4194/src/hubspot_api/client_v3.py#lines-134
#  It is just nicer to work with as a Pyton interface, but not needed.
def process_requests(endpoint, data_in, hapikey) -> None:
    """
    Args:
        endpoint: Hubspot API endpoint set in config.json
        data_in: csv.DictReader object with data from input csv
        hapikey: API key for Hubspot API

    Returns:
        None
    """

    if auth_check_ok(hapikey):
        factory = get_factory(endpoint, hapikey)
        factory.process_requests(data_in)
