"""
Hubspot Writer
"""
import csv
import json
import logging

from keboola.component.base import ComponentBase

from client import process_requests
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
        # TODO: this is not needed. ComponentBase does that automatically.
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
        # TODO: Using input mapping in configuration.json is not adviced, mainly because it prevents using "before" processors.
        # Use self.get_input_tables_definitions() instead, this works with manifest files and is not dependent on cofig.json. Contains the same information

        in_tables = self.configuration.tables_input_mapping
        # TODO: I would consider allowing only one input table here and make it eventually a row based component.
        # Or does it make sense to have multiple tables on the input? E.g. to update different columns each time?
        self.validate_user_input(in_tables)

        # Looping all the input tables
        for table in in_tables:
            logging.info(f'Processing input table: {table["destination"]}')
            # TODO: the f'{self.tables_in_path}/{table["destination"]}' is not needed and also is using hardcoded path separator
            # If table definition object is used, it can be written like this open(table.full_path)
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
