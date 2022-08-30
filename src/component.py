"""
Hubspot Writer
"""
import csv
import logging

from keboola.component.base import ComponentBase
from keboola.component import dao
from exceptions import UserException

from client import run, test_credentials
from endpoint_mapping import ENDPOINT_MAPPING

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_ENDPOINT = 'endpoint'
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_TOKEN,
    KEY_ENDPOINT
]


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.params = self.configuration.parameters
        self.api_token = self.params.get(KEY_API_TOKEN)

    def run(self):
        """
        Main execution code
        """

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)

        endpoint = self.params.get(KEY_ENDPOINT)
        logging.info(f"Selected Endpoint: [{endpoint}]")

        test_credentials(self.api_token)

        # Input tables
        in_tables = self.get_input_tables_definitions()
        table = in_tables[0]

        self.validate_user_input(table)
        logging.info(f'Processing input table: {table.name}')

        # If table definition object is used, it can be written like this open(table.full_path)
        with open(table.full_path) as csvfile:
            reader = csv.DictReader(csvfile)
            run(endpoint, reader, self.api_token)

    def validate_user_input(self, table: dao.TableDefinition):

        # 1 - Ensure an endpoint is selected and valid
        if self.params.get(KEY_ENDPOINT) not in ENDPOINT_MAPPING:
            raise UserException(f'{self.params.get(KEY_ENDPOINT)} is not a valid endpoint.')

        # 3 - Ensure all required columns are in the input files for the respective endpoint.
        # Comparing this information with the file's manifest
        required_columns = ENDPOINT_MAPPING[self.params.get(KEY_ENDPOINT)]['required_column']
        table_columns = table.columns
        missing_columns = []

        for r in required_columns:
            if r not in table_columns:
                missing_columns.append(r)
        if missing_columns:
            raise UserException(f'Missing columns in input table {table.name}')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
