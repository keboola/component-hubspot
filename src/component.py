"""
Hubspot Writer
"""
import csv
import logging
from pathlib import Path

from keboola.component import dao
from keboola.component.base import ComponentBase

import client as hubspot_client
from endpoint_mapping import ENDPOINT_MAPPING, LEGACY_ENDPOINT_MAPPING_CONVERSION
from exceptions import UserException

# configuration variables
KEY_OBJECT = 'hubspot_object'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_OBJECT
]
ERRORS_TABLE_COLUMNS = ('status', 'category', 'message', 'context')


def coalesce(*arg):
    return next((a for a in arg if a is not None), None)


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.token = None
        self.params = self.configuration.parameters

    def run(self):
        """
        Main execution code
        """

        authentication_type = self.params.get("authentication_type", "API Key")
        if authentication_type == "API Key":
            self.token = self.params["#api_token"]
        elif authentication_type == "Private App Token":
            self.token = self.params["#private_app_token"]
        else:
            raise ValueError(f"Invalid authentication type: {authentication_type}")

        # Input tables
        in_tables = self.get_input_tables_definitions()
        table = in_tables[0]

        # Input checks
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        hubspot_client.test_credentials(self.token, authentication_type)
        self.validate_user_input(table)

        logging.info(f"Processing input table: {table.name}")

        output_table_destination = self.configuration.tables_output_mapping[0].destination
        output_table_full_path = Path(self.tables_out_path, output_table_destination)

        with open(table.full_path) as input_file, open(output_table_full_path, 'w', newline='') as output_file:
            reader = csv.DictReader(input_file)
            error_writer = csv.DictWriter(output_file, fieldnames=ERRORS_TABLE_COLUMNS)
            error_writer.writeheader()
            hubspot_client.run(self.endpoint, reader, error_writer, self.token, authentication_type)

    @property
    def hubspot_object(self) -> str:
        return self.params.get(KEY_OBJECT)

    @property
    def endpoint(self) -> str:
        if self.hubspot_object in LEGACY_ENDPOINT_MAPPING_CONVERSION:
            return LEGACY_ENDPOINT_MAPPING_CONVERSION[self.hubspot_object]
        else:
            return f"{self.hubspot_object}_{self.action}"

    @property
    def action(self) -> str:
        action = coalesce(self.params.get("contact_action"),
                          self.params.get("company_action"),
                          self.params.get("list_action"),
                          self.params.get("deal_action"))

        if action is None and self.hubspot_object not in list(LEGACY_ENDPOINT_MAPPING_CONVERSION.keys()):
            raise UserException("A valid Object action must be provided.")
        return action

    def validate_user_input(self, table: dao.TableDefinition):

        # 1 - Ensure an endpoint is selected and valid
        if self.endpoint not in ENDPOINT_MAPPING:
            raise UserException(f"{self.endpoint} is not a valid endpoint.")

        # 2 - Ensure all required columns are in the input files for the respective endpoint.
        # Comparing this information with the file's manifest
        required_columns = ENDPOINT_MAPPING[self.endpoint]["required_column"]
        table_columns = table.columns
        missing_columns = [column for column in required_columns if column not in table_columns]

        if missing_columns:
            raise UserException(f"Missing columns {missing_columns} in input table {table.name}")


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
