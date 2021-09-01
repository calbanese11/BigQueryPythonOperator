from google.cloud import bigquery
from abc import ABC, abstractmethod
import sys
import os
import pandas as pd
from typing import List, Callable
import tempfile

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/codyalbanese/Projects/BigQueryOperator/google-application-credentials.json"


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class SqlNotSet(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message, func_name=False):
        self.expression = expression
        self.message = message
        self.func_name = func_name

        if self.func_name is not False:
            print(self.func_name + " - Needs a query.")


class OutputLocation(ABC):
    @abstractmethod
    def write_output_local(self, output_location):
        return output_location
    @abstractmethod
    def write_output_gcs(self, output_location):
        pass


class BigQueryOperator:

    """
    Operator used to easily interact with bigquery
    """

    def _env_check(self):
        """
        Check to see if the required GOOGLE_APPLICATION_CREDENTIALS has been set
        """

        try:
            os.environ[self.required_env_var] # Throws error if required environment variable is not set
            return bigquery.Client()
        except KeyError as k:
            print("You have not set the required GOOGLE_APPLICATION_CREDENTIALS environment variable.\n" +
                  "To set this use the commented out line of code below the import statement.\nOr run this code in your "
                  "shell environment: export GOOGLE_APPLICATION_CREDENTIALS=<path to json key>")
            sys.exit("GOOGLE_APPLICATION_CREDENTIALS - Not Found")

    def __init__(self):
        self,
        self.name = "BIGQUERY_OPERATOR"
        self.required_env_var = "GOOGLE_APPLICATION_CREDENTIALS"
        self.client = self._env_check()

    # Use abstract method for output path if gcs is desired

    def _return_parquet(self, data):
        pass

    def _return_avro(self, output_path: str, data):
        pass

    def _return_csv(self, data, output_path: str, sep: str = ","):
        data.to_csv(output_path, sep)
        return

    def _return_text(self, output_path: str, data):
        pass

    def _query_job(self, job_instance, create_bq_storage_client: bool = True, silent: bool = False):

        sanity_check = 0
        while job_instance.state == "RUNNING":
            sanity_check += 1

            if silent is False:
                print("Job: {}\nCreated at: {}\nStarted at: {}\nIn state: {} {}\r".format(job_instance.job_id, job_instance.created, job_instance.started, job_instance.state, sanity_check))

            data = (job_instance
                    .result()
                    .to_dataframe(
                        # Optionally, explicitly request to use the BigQuery Storage API. As of
                        # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                        # API is used by default.
                        create_bqstorage_client=create_bq_storage_client,
                    )
                    )

        if silent is False:
            print("This query will bill {} bytes.".format(job_instance.total_bytes_billed))
            print("This query will process {} bytes.".format(job_instance.total_bytes_processed))

        return data

    def bigquery_download(self,
                          sql: str = None,
                          data_return_type: str = None,
                          params: dict = None,
                          output_location: str = None,
                          gcs_output_location: bool = False,
                          create_bq_storage_client: bool = True,
                          silent: bool = False):

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_download.__name__)

        query_job = self.client.query(sql)

        data = self._query_job(query_job, create_bq_storage_client=create_bq_storage_client, silent=silent)

        self._return_csv(data, "/Users/codyalbanese/Projects/BigQueryOperator/outputs/test.csv")

        return data

    def bigquery_sql_operator(self,
                              sql: str = None,
                              silent: bool = False):

        """
        Useful to call stored procedures/routines
        """

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_sql_operator.__name__)

        query_job = self.client.query(sql)

        self._query_job(query_job, silent=silent)

        return

    def bigquery_upload_stream(self,
                                   table_id=None,
                                   rows=None):
        """
        New rows must be json formatted
        """

        rows_to_insert = [rows]
        errors = self.client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            pass
            # print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

        return

