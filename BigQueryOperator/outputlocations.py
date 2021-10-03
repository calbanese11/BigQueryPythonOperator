from abc import ABC, abstractmethod
from google.cloud import storage
from typing import List, Callable, Optional
import tempfile


def _upload_local_gcs(bucket_name: str = None, destination_blob_name: str = None, data=None, suffix: str = None):
    # Create a named temporary file to be used in upload to GCS. Temporary file will be immediately deleted after use.
    # This may take awhile for very large files. Use with caution.
    # print(data)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    mode = "w"
    if suffix in ('.parquet', '.avro'):
        mode = "wb"

    with tempfile.NamedTemporaryFile(suffix=suffix, mode=mode) as ntf:
        ntf.write(data)
        blob.upload_from_filename(ntf.name)

    print(
        "{} uploaded to gcs.".format(destination_blob_name
                                     )
    )
    return


class OutputLocation(ABC):

    @abstractmethod
    def write_output_local(self, data, output_location: str, params: Optional[dict] = None, *args, **kwargs):
        pass

    @abstractmethod
    def write_output_local_gcs(self, params: Optional[dict] = None, bucket_name: str = None,
                               destination_blob_name: str = None,
                               data=None, *args, **kwargs):
        pass


class ReturnParquet(OutputLocation):
    """
    Contains methods needed to output parquet
    """

    def write_output_local(self, data, output_location: str, params: Optional[dict] = None, *args, **kwargs):
        if params is not None:
            data.to_parquet(output_location, **params)
        else:
            data.to_parquet(output_location)

    def write_output_local_gcs(self, params: Optional[dict] = None, bucket_name: str = None,
                               destination_blob_name: str = None,
                               data=None, *args, **kwargs):
        if params is not None:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_parquet(**params),
                              suffix=".parquet")
        else:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_parquet(),
                              suffix=".parquet")


class ReturnCsv(OutputLocation):
    """
    Contains methods needed to output csv
    """

    def write_output_local(self, data, output_location: str, params: Optional[dict] = None, *args, **kwargs):

        if params is not None:
            data.to_csv(output_location, **params)
        else:
            data.to_csv(output_location)

    def write_output_local_gcs(self, params: Optional[dict] = None, bucket_name: str = None,
                               destination_blob_name: str = None,
                               data=None, *args, **kwargs):

        if params is not None:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_csv(**params),
                              suffix=".csv")
        else:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_csv(),
                              suffix=".csv")


class ReturnText(OutputLocation):
    """
    Contains methods used to output text
    """

    def write_output_local(self, data, output_location: str, params: Optional[dict] = None, *args, **kwargs):

        if params is not None:
            data.to_string(output_location, **params)
        else:
            data.to_string(output_location)

    def write_output_local_gcs(self, params: Optional[dict] = None, bucket_name: str = None,
                               destination_blob_name: str = None,
                               data=None, *args, **kwargs):

        if params is not None:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_string(**params),
                              suffix=".txt")
        else:
            _upload_local_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name,
                              data=data.to_string(),
                              suffix=".txt")


# class ReturnAvro(OutputLocation):
#     def write_output_local(self, data, output_location: str, params: Optional[dict] = None, *args, **kwargs):
#         pass
#
#     def write_output_local_gcs(self, params: Optional[dict] = None, bucket_name: str = None,
#                                destination_blob_name: str = None,
#                                data=None, *args, **kwargs):
#         pass


def configure_output(location: str = "local", return_type: str = "csv"):
    """
    Returns the method needed dependent on the output location.

    :param location:
    :param return_type:
    """

    return_format_options = {"csv": ReturnCsv,
                             "parquet": ReturnParquet,
                             "txt": ReturnText}

    instantiated_return_class = return_format_options[return_type]()

    return_location_options = {"local": instantiated_return_class.write_output_local,
                               "gcs": instantiated_return_class.write_output_local_gcs}

    return return_location_options[location]
