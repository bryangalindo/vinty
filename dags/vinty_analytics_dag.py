import datetime
import logging
import os

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from include.scripts import convert_raw_to_base_data, delete_duplicate_base_data

log = logging.getLogger(__name__)

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME', Variable.get("AWS_S3_BUCKET_NAME"))
AWS_S3_BASE_DATA_BUCKET_NAME = os.getenv('AWS_S3_BASE_DATA_BUCKET_NAME', Variable.get("AWS_S3_BASE_DATA_BUCKET_NAME"))
AWS_S3_WAREHOUSE_BUCKET = os.getenv('AWS_S3_WAREHOUSE_BUCKET', Variable.get("AWS_S3_WAREHOUSE_BUCKET_NAME"))
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', Variable.get("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', Variable.get("AWS_SECRET_ACCESS_KEY"))
AWS_REGION = os.getenv('AWS_REGION', Variable.get("AWS_REGION"))
ENV = os.getenv('ENV', Variable.get("ENV"))

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
AIRFLOW_EXECUTION_DATE = "{{ ds }}"
AIRFLOW_PREVIOUS_EXECUTION_DATE = "{{ prev_ds }}"

os.environ["AWS_REGION"] = AWS_REGION
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID

ICEBERG_VSP_STORE_DB = "vsp"
ICEBERG_REBAG_STORE_DB = "rebag"
ICEBERG_CATALOG = "vinty"

SPARK_ENV_VARS = {
    "AWS_REGION": AWS_REGION,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "ENV": ENV,
}
SPARK_CONFIGS = {
    "spark.app.name": "CreateIcebergTable",
    f"spark.sql.catalog.{ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": f"s3://{AWS_S3_WAREHOUSE_BUCKET}/",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{ICEBERG_CATALOG}.aws.region": AWS_REGION,
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.region": AWS_REGION,
}

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.723,"
    "org.apache.iceberg:iceberg-aws-bundle:1.7.0,"
)


def delete_vsp_duplicate_base_data_task():
    return PythonOperator(
        task_id="delete_duplicate_base_data",
        python_callable=delete_duplicate_base_data.main,
        op_args=(ICEBERG_VSP_STORE_DB, AWS_S3_BASE_DATA_BUCKET_NAME, "{{ ds }}"),
    )


def delete_rebag_duplicate_base_data_task():
    return PythonOperator(
        task_id="delete_duplicate_base_data",
        python_callable=delete_duplicate_base_data.main,
        op_args=(ICEBERG_REBAG_STORE_DB, AWS_S3_BASE_DATA_BUCKET_NAME, "{{ ds }}"),
    )


def convert_vsp_raw_data_to_base_data_task():
    return PythonOperator(
        task_id="convert_raw_data_to_base_data",
        python_callable=convert_raw_to_base_data.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            AWS_S3_BUCKET_NAME,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def convert_rebag_raw_data_to_base_data_task():
    return PythonOperator(
        task_id="convert_raw_data_to_base_data",
        python_callable=convert_raw_to_base_data.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            AWS_S3_BUCKET_NAME,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def create_vsp_products_table_task():
    return SparkSubmitOperator(
        task_id="create_products_table",
        conn_id="my_spark_conn",
        application="./include/scripts/create_products_table.py",
        application_args=[ICEBERG_VSP_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
    )


def create_rebag_products_table_task():
    return SparkSubmitOperator(
        task_id="create_products_table",
        conn_id="my_spark_conn",
        application="./include/scripts/create_products_table.py",
        application_args=[ICEBERG_REBAG_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
    )


def delete_vsp_duplicate_product_rows_task():
    return SparkSubmitOperator(
        task_id="delete_duplicate_product_rows",
        conn_id="my_spark_conn",
        application="./include/scripts/delete_duplicate_product_rows.py",
        application_args=[ICEBERG_VSP_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
    )


def delete_rebag_duplicate_product_rows_task():
    return SparkSubmitOperator(
        task_id="delete_duplicate_product_rows",
        conn_id="my_spark_conn",
        application="./include/scripts/delete_duplicate_product_rows.py",
        application_args=[ICEBERG_REBAG_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
    )


def add_new_vsp_products_task():
    return SparkSubmitOperator(
        task_id="add_new_products",
        conn_id="my_spark_conn",
        application="./include/scripts/add_new_products.py",
        application_args=[ICEBERG_VSP_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
        env_vars=SPARK_ENV_VARS,
    )


def add_new_rebag_products_task():
    return SparkSubmitOperator(
        task_id="add_new_products",
        conn_id="my_spark_conn",
        application="./include/scripts/add_new_products.py",
        application_args=[ICEBERG_REBAG_STORE_DB, "{{ ds }}"],
        packages=SPARK_PACKAGES,
        conf=SPARK_CONFIGS,
        env_vars=SPARK_ENV_VARS,
    )


def build_dbt_bash_command(store: str) -> str:
    command = (
        f"cd {AIRFLOW_HOME} "
        f"&& source dbt_env/bin/activate "
        f"&& cd transform "
        f"&& dbt deps "
        f"&& dbt build -s stg_{store}__products "
        f"&& dbt build -s inc_{store}__sold_products "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )
    if ENV == "prod":
        command += " --target prod"
    return command


def create_vsp_models_task():
    command = build_dbt_bash_command("vsp")
    return BashOperator(
        task_id="create_vsp_models",
        bash_command=command,
    )


def create_rebag_models_task():
    command = build_dbt_bash_command("rebag")
    return BashOperator(
        task_id="create_rebag_models",
        bash_command=command,
    )


@dag(
    start_date=datetime.datetime(2024, 11, 13),
    schedule="@daily",
    max_active_runs=1,
    catchup=True,
    default_args={"owner": "Bryan Galindo", "retries": 3},
    tags=["ingestion"],
)
def vinty_analytics_pipeline():
    with TaskGroup("vsp_ingestion_tasks") as vsp_ingestion_tasks:
        delete_duplicate_base_data = delete_vsp_duplicate_base_data_task()
        convert_raw_data_to_base_data = convert_vsp_raw_data_to_base_data_task()
        create_products_table = create_vsp_products_table_task()
        delete_duplicate_product_rows = delete_vsp_duplicate_product_rows_task()
        add_new_products = add_new_vsp_products_task()

        (
            delete_duplicate_base_data
            >> convert_raw_data_to_base_data
            >> create_products_table
            >> delete_duplicate_product_rows
            >> add_new_products
        )

    with TaskGroup("rebag_ingestion_tasks") as rebag_ingestion_tasks:
        delete_duplicate_base_data = delete_rebag_duplicate_base_data_task()
        convert_raw_data_to_base_data = convert_rebag_raw_data_to_base_data_task()
        create_products_table = create_rebag_products_table_task()
        delete_duplicate_product_rows = delete_rebag_duplicate_product_rows_task()
        add_new_products = add_new_rebag_products_task()

        (
            delete_duplicate_base_data
            >> convert_raw_data_to_base_data
            >> create_products_table
            >> delete_duplicate_product_rows
            >> add_new_products
        )

    with TaskGroup("transform_tasks") as transform_tasks:
        create_vsp_models = create_vsp_models_task()
        create_rebag_models = create_rebag_models_task()

        create_vsp_models >> create_rebag_models

    vsp_ingestion_tasks >> rebag_ingestion_tasks >> transform_tasks


vinty_analytics_pipeline()
