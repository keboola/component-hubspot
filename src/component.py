"""
Hubspot Writer
"""
import logging
import json
import csv

from keboola.component.base import ComponentBase

from endpoint_mapping import ENDPOINT_MAPPING
from client import process_requests

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_ENDPOINT = 'endpoint'
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_TOKEN,
    KEY_ENDPOINT,
    KEY_DEBUG
]


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        if self.configuration.parameters.get(KEY_DEBUG):
            logging.getLogger().setLevel(logging.DEBUG)
            logging.info('Loading configuration...')

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.params = self.configuration.parameters
        self.api_token = self.params.get(KEY_API_TOKEN)

    def run(self):
        """
        Main execution code
        """

        endpoint = self.params.get(KEY_ENDPOINT)
        logging.info(f"Selected Endpoint: [{endpoint}]")

        # Input tables
        in_tables = self.configuration.tables_input_mapping
        self.validate_user_input(in_tables)

        # Looping all the input tables
        for table in in_tables:
            logging.info(f'Processing input table: {table["destination"]}')
            with open(f'{self.tables_in_path}/{table["destination"]}') as csvfile:
                reader = csv.DictReader(csvfile)
                process_requests(endpoint, reader, self.api_token)

    def validate_user_input(self, in_tables):

        # 1 - Ensure an endpoint is selected and valid
        if self.params.get(KEY_ENDPOINT) not in ENDPOINT_MAPPING:
            raise Exception(f'{self.params.get(KEY_ENDPOINT)} is not a valid endpoint.')

        # 2 - Ensure there are input files
        if len(in_tables) < 1:
            raise Exception('Input tables are missing.')

        # 3 - Ensure all required columns are in the input files for the respective endpoint.
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
