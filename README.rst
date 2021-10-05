BigQuerySqlOperator
=======

**BigQuerySqlOperator**: is a Python library built for easy interaction with Google Big Query (GBQ). Requires only
basic python and sql knowledge to operate. This package is intended for data analysts, scientists, and developers.

.. note::

   This project is under active development.

**Example**: Download data from GBQ to your root directory::

 BigQueryOperator = BigQueryOperator(path_to_json_key)

 sql = "SELECT *
        FROM `my_project.my_dataset.my_data`
        LIMIT 1000"

 BigQueryOperator.bigquery_download_local(sql,
                                          silent=False,
                                          output_path="my_data.csv",
                                          data_return_type="csv",
                                          custom_output_params={"sep":";"})
