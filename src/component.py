"""
Hubspot Writer
"""
import csv
import logging

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
HUBSPOT_OBJECTS = ("contact", "company", "list", "deal", "ticket", "product", "quote", "line_item", "tax", "call",
                   "communication", "email", "meeting", "note", "postal_mail", "task")


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
        self.token = self.params["#private_app_token"]
        authentication_type = self.params.get("authentication_type", "Private App Token")

        if authentication_type != "Private App Token":
            raise ValueError(f"Invalid authentication type: {authentication_type}")

        # Input tables
        in_tables = self.get_input_tables_definitions()
        input_table = in_tables[0]

        # Input checks
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        hubspot_client.test_credentials(self.token)
        self.validate_user_input(input_table)

        output_table = self.create_out_table_definition('errors.csv', write_always=True)

        logging.info(f"Processing input table: {input_table.name}")

        with open(input_table.full_path) as input_file, open(output_table.full_path, 'w', newline='') as output_file:
            reader = csv.DictReader(input_file)
            error_writer = csv.DictWriter(output_file, fieldnames=hubspot_client.ERRORS_TABLE_COLUMNS)
            error_writer.writeheader()
            error_writer.errors = False
            hubspot_client.run(self.endpoint, reader, error_writer, self.token)

            if error_writer.errors:
                self.write_manifest(output_table)
                raise UserException(
                    'There were errors during some requests handling - check errors.csv for more details.')

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
        action = coalesce(*(self.params.get(f"{hubspot_object}_action") for hubspot_object in HUBSPOT_OBJECTS))

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
