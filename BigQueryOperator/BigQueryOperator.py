from google.cloud import bigquery
from google.cloud import storage
from abc import ABC, abstractmethod
import sys
import os
# import pandas as pd
from typing import List, Callable, Optional
from .outputlocations import *


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


class BigQueryOperator:
    """
    Operator used to easily interact with bigquery

    Requires that the GOOGLE_APPLICATION_CREDENTIALS be set as an environment variable.
    Or the user must provide a path to the json key.
    """

    def _env_check(self):
        """
        Check to see if the required GOOGLE_APPLICATION_CREDENTIALS have been set
        """
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.GOOGLE_APPLICATION_CREDENTIALS
            os.environ[self.required_env_var]  # Throws error if required environment variable is not set
            return bigquery.Client()
        except Exception as e:
            print("You have not set the required GOOGLE_APPLICATION_CREDENTIALS environment variable.\n" +
                  "To set this use the commented out line of code below the import statement.\nOr run this code in your "
                  "shell environment: export GOOGLE_APPLICATION_CREDENTIALS=<path to json key>")
            sys.exit("GOOGLE_APPLICATION_CREDENTIALS - Not Found")

    def __init__(self, google_application_credentials_path: str = None):
        self,
        self.name = "BIGQUERY_OPERATOR"
        self.required_env_var = "GOOGLE_APPLICATION_CREDENTIALS"
        self.GOOGLE_APPLICATION_CREDENTIALS = google_application_credentials_path
        self.client_gbq = self._env_check()

    @staticmethod
    def _query_job(job_instance, create_bq_storage_client: bool = True, silent: bool = False):

        while job_instance.state == "RUNNING":

            if silent is False:
                print("Job: {}\nCreated at: {}\nStarted at: {}\nIn state: {}".format(job_instance.job_id,
                                                                                     job_instance.created,
                                                                                     job_instance.started,
                                                                                     job_instance.state))
            data = None
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
            print("Query job: {}".format(job_instance.state))
            print("This query will bill {} bytes.".format(job_instance.total_bytes_billed))
            print("This query will process {} bytes.".format(job_instance.total_bytes_processed))

        return data

    def bigquery_download_local(self,
                                sql: str = None,
                                data_return_type: str = "text",
                                custom_output_params: Optional[dict] = None,
                                output_path: str = "./outputs/test.txt",
                                create_bq_storage_client: Optional[bool] = True,
                                silent: Optional[bool] = False):

        """
        Function used to download Google Big Query SQL results to a local file.

        :param sql:
        :param data_return_type:
        :param custom_output_params:
        :param output_path:
        :param create_bq_storage_client:
        :param silent:
        :return: None
        """

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_download.__name__)

        query_job = self.client_gbq.query(sql)

        data = self._query_job(job_instance=query_job, create_bq_storage_client=create_bq_storage_client, silent=silent)

        write_output_func = configure_output(location="local", return_type=data_return_type)

        write_output_func(data=data, output_location=output_path, params=custom_output_params)

        return

    def bigquery_upload_gcs(self,
                            sql: str = None,
                            data_return_type: str = "text",
                            custom_output_params: Optional[dict] = None,
                            gcs_bucket_name: Optional[str] = None,
                            gcs_destination_blob_name: Optional[str] = None,
                            create_bq_storage_client: Optional[bool] = True,
                            silent: Optional[bool] = False):
        """
        Function used to download data locally and then upload the data to GBQ. The downloaded data will be in temporary
        files and then be immediately removed. This allows you to avoid sharding when exporting data directly to gcs from
        GBQ. The maximum shard size in GBQ to GCS is 1GB. It's recommended not to use this function unless you absolutely
        need the data in a single file. Hope you have fast internet

        No need to use this function if your SQL results are less than 1GB. Use the bigquery_extract_gcs func instead.

        :param sql:
        :param data_return_type:
        :param custom_output_params:
        :param gcs_bucket_name:
        :param gcs_destination_blob_name:
        :param create_bq_storage_client:
        :param silent:
        :return: None
        """

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_download.__name__)

        query_job = self.client_gbq.query(sql)

        data = self._query_job(job_instance=query_job, create_bq_storage_client=create_bq_storage_client, silent=silent)

        write_output_func = configure_output(location="gcs", return_type=data_return_type)
        write_output_func(data=data, bucket_name=gcs_bucket_name, destination_blob_name=gcs_destination_blob_name)
        return

    def bigquery_extract_gcs(self,
                             sql: str = None,
                             data_return_type: str = "TXT",
                             custom_output_params: Optional[dict] = None,
                             gcs_bucket_name: Optional[str] = None,
                             gcs_destination_blob_name: Optional[str] = None,
                             silent: Optional[bool] = False):
        """
        Function used to extract data directly from GBQ to GCS. It's recommended to use this function if your data is
        smaller than 1GB. This function will append an EXTRACT statement right before your provided SQL query.

        :param sql:
        :param data_return_type:
        :param custom_output_params:
        :param gcs_bucket_name:
        :param gcs_destination_blob_name:
        :param create_bq_storage_client:
        :param silent:
        :return:
        """

        suffix_dict = {"CSV": ".csv",
                       "TXT": ".txt",
                       "AVRO": ".avro",
                       "PARQET": ".parquet"}

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_download.__name__)

        bq_export_to_gcs = f"""
                        EXPORT DATA OPTIONS(
                          uri='gs:/{gcs_bucket_name}/{gcs_destination_blob_name}-*.{suffix_dict[data_return_type]}',
                          format={data_return_type},
                          overwrite=true,
                          header=false,
                          field_delimiter=',') AS
                          {sql}
                        """

        query_job = self.client_gbq.query(sql)

        return

    def bigquery_sql_operator(self,
                              sql: str = None,
                              silent: Optional[bool] = False):

        """
        Useful to call stored procedures or routines locally
        """

        if sql is None:
            raise SqlNotSet("SQL", "A SQL query must be defined", self.bigquery_sql_operator.__name__)

        query_job = self.client_gbq.query(sql)

        self._query_job(query_job, silent=silent)

        return

    def bigquery_upload_stream(self,
                               table_id=None,
                               rows=None):
        """
        New rows must be json formatted
        """

        rows_to_insert = [rows]
        errors = self.client_gbq.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            pass
            # print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

        return
