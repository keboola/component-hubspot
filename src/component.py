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
KEY_OBJECT = 'hubspot_object'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_OBJECT
]


def coalesce(*arg):
    return next((a for a in arg if a is not None), None)


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.token = None
        self.params = self.configuration.parameters

        self.action = self.get_action()
        self.hubspot_object = self.params.get(KEY_OBJECT)
        self.endpoint = f"{self.hubspot_object}_{self.action}"

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
        test_credentials(self.token, authentication_type)
        self.validate_user_input(table)

        logging.info(f"Processing input table: {table.name}")

        with open(table.full_path) as csvfile:
            reader = csv.DictReader(csvfile)
            run(self.endpoint, reader, self.token, authentication_type)

    def get_action(self):
        if ((action := coalesce(self.params.get("contact_action"),
                                self.params.get("company_action"),
                                self.params.get("list_action"))) is None):
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
        missing_columns = []

        for r in required_columns:
            if r not in table_columns:
                missing_columns.append(r)
        if missing_columns:
            raise UserException(f"Missing columns in input table {table.name}")


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
