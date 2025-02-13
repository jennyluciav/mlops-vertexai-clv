import sys
from typing import NamedTuple

from google.cloud import aiplatform as vertex
from google_cloud_pipeline_components.v1 import bigquery as bq_components
from google_cloud_pipeline_components.v1.automl.training_job import \
    AutoMLTabularTrainingJobRunOp
from google_cloud_pipeline_components.v1.dataset import TabularDatasetCreateOp
from google_cloud_pipeline_components.v1.endpoint import (EndpointCreateOp,
                                                          ModelDeployOp)
from google_cloud_pipeline_components.v1.model import ModelUploadOp
from kfp import compiler, dsl
from kfp.dsl import Artifact, Input, Metrics, Output, component

from config import (PIPELINE_ROOT, PIPELINE_NAME, BQ_INPUT_DATA, 
                    MODEL_DISPLAY_NAME, ENDPOINT_NAME,
                    SERVICE_ACCOUNT, NETWORK, KEY_ID,
                    PROJECT_ID, REGION, TARGET_COLUMN,
                    BQ_DATASET_NAME)

@component(base_image="python:3.9", packages_to_install=["google-cloud-bigquery"])
def import_data_to_bigquery(
    project: str,
    bq_location: str,
    bq_dataset: str,
    gcs_data_uri: str,
    raw_dataset: Output[Artifact],
    table_name_prefix: str = "abalone",
):
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client(project=project, location=bq_location)

    def load_dataset(gcs_uri, table_id):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("Sex", "STRING"),
                bigquery.SchemaField("Length", "NUMERIC"),
                bigquery.SchemaField("Diameter", "NUMERIC"),
                bigquery.SchemaField("Height", "NUMERIC"),
                bigquery.SchemaField("Whole_weight", "NUMERIC"),
                bigquery.SchemaField("Shucked_weight", "NUMERIC"),
                bigquery.SchemaField("Viscera_weight", "NUMERIC"),
                bigquery.SchemaField("Shell_weight", "NUMERIC"),
                bigquery.SchemaField("Rings", "NUMERIC"),
            ],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )
        print(f"Loading {gcs_uri} into {table_id}")
        load_job = client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

    def create_dataset_if_not_exist(bq_dataset_id, bq_location):
        print(
            "Checking for existence of bq dataset. If it doesn't exist, it creates one"
        )
        dataset = bigquery.Dataset(bq_dataset_id)
        dataset.location = bq_location
        dataset = client.create_dataset(dataset, exists_ok=True, timeout=300)
        print(f"Created dataset {dataset.full_dataset_id} @ {dataset.location}")

    bq_dataset_id = f"{project}.{bq_dataset}"
    create_dataset_if_not_exist(bq_dataset_id, bq_location)

    raw_table_name = f"{table_name_prefix}_raw"
    table_id = f"{project}.{bq_dataset}.{raw_table_name}"
    print("Deleting any tables that might have the same name on the dataset")
    client.delete_table(table_id, not_found_ok=True)
    print("Loading data to table...")
    load_dataset(gcs_data_uri, table_id)

    raw_dataset_uri = f"bq://{table_id}"
    raw_dataset.uri = raw_dataset_uri

@component(
    base_image="python:3.9",
    packages_to_install=["google-cloud-bigquery"],
)  # pandas, pyarrow and fsspec required to export bq data to csv
def split_datasets(
    raw_dataset: Input[Artifact],
    bq_location: str,
) -> NamedTuple(
    "bqml_split",
    [
        ("dataset_uri", str),
        ("dataset_bq_uri", str),
        ("test_dataset_uri", str),
    ],
):

    from collections import namedtuple

    from google.cloud import bigquery

    raw_dataset_uri = raw_dataset.uri
    table_name = raw_dataset_uri.split("bq://")[-1]
    print(table_name)
    raw_dataset_uri = table_name.split(".")
    print(raw_dataset_uri)
    project = raw_dataset_uri[0]
    bq_dataset = raw_dataset_uri[1]
    bq_raw_table = raw_dataset_uri[2]

    client = bigquery.Client(project=project, location=bq_location)

    def split_dataset(table_name_dataset):
        training_dataset_table_name = f"{project}.{bq_dataset}.{table_name_dataset}"
        split_query = f"""
        CREATE OR REPLACE TABLE
            `{training_dataset_table_name}`
           AS
        SELECT
          Sex,
          Length,
          Diameter,
          Height,
          Whole_weight,
          Shucked_weight,
          Viscera_weight,
          Shell_weight,
          Rings,
            CASE(ABS(MOD(FARM_FINGERPRINT(TO_JSON_STRING(f)), 10)))
              WHEN 9 THEN 'TEST'
              WHEN 8 THEN 'VALIDATE'
              ELSE 'TRAIN' END AS split_col
        FROM
          `{project}.{bq_dataset}.abalone_raw` f
        """
        dataset_uri = f"{project}.{bq_dataset}.{bq_raw_table}"
        print("Splitting the dataset")
        query_job = client.query(split_query)  # Make an API request.
        query_job.result()
        print(dataset_uri)
        print(split_query.replace("\n", " "))
        return training_dataset_table_name

    def create_test_view(training_dataset_table_name, test_view_name="dataset_test"):
        view_uri = f"{project}.{bq_dataset}.{test_view_name}"
        query = f"""
             CREATE OR REPLACE VIEW `{view_uri}` AS SELECT
          Sex,
          Length,
          Diameter,
          Height,
          Whole_weight,
          Shucked_weight,
          Viscera_weight,
          Shell_weight,
          Rings 
          FROM `{training_dataset_table_name}`  f
          WHERE 
          f.split_col = 'TEST'
          """
        print(f"Creating view for --> {test_view_name}")
        print(query.replace("\n", " "))
        query_job = client.query(query)  # Make an API request.
        query_job.result()
        return view_uri

    table_name_dataset = "dataset"

    dataset_uri = split_dataset(table_name_dataset)
    test_dataset_uri = create_test_view(dataset_uri)
    dataset_bq_uri = "bq://" + dataset_uri

    print(f"dataset: {dataset_uri}")

    result_tuple = namedtuple(
        "bqml_split",
        ["dataset_uri", "dataset_bq_uri", "test_dataset_uri"],
    )
    return result_tuple(
        dataset_uri=str(dataset_uri),
        dataset_bq_uri=str(dataset_bq_uri),
        test_dataset_uri=str(test_dataset_uri),
    )

