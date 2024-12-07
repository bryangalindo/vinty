import datetime
import logging
import os

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.scripts import (
    convert_raw_to_base_data,
    delete_duplicate_base_data,
    add_new_products,
    delete_duplicate_product_rows,
    create_products_table,
)

log = logging.getLogger(__name__)

AWS_S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME", Variable.get("AWS_S3_BUCKET_NAME"))
AWS_S3_BASE_DATA_BUCKET_NAME = os.getenv(
    "AWS_S3_BASE_DATA_BUCKET_NAME", Variable.get("AWS_S3_BASE_DATA_BUCKET_NAME")
)
AWS_S3_WAREHOUSE_BUCKET = os.getenv(
    "AWS_S3_WAREHOUSE_BUCKET", Variable.get("AWS_S3_WAREHOUSE_BUCKET_NAME")
)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", Variable.get("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY", Variable.get("AWS_SECRET_ACCESS_KEY")
)
AWS_REGION = os.getenv("AWS_REGION", Variable.get("AWS_REGION"))
ENV = os.getenv("ENV", Variable.get("ENV"))

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
AIRFLOW_EXECUTION_DATE = "{{ ds }}"
AIRFLOW_PREVIOUS_EXECUTION_DATE = "{{ prev_ds }}"

os.environ["AWS_REGION"] = AWS_REGION
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID

ICEBERG_VSP_STORE_DB = "vsp"
ICEBERG_REBAG_STORE_DB = "rebag"
ICEBERG_CATALOG = "vinty"


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
    return PythonOperator(
        task_id="create_vsp_products_table",
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def create_rebag_products_table_task():
    return PythonOperator(
        task_id="create_rebag_products_table",
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_vsp_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_vsp_duplicate_product_rows",
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_rebag_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_rebag_duplicate_product_rows",
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_vsp_products_task():
    return PythonOperator(
        task_id="add_new_vsp_products",
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_rebag_products_task():
    return PythonOperator(
        task_id="add_new_rebag_products",
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def build_dbt_bash_command(store: str) -> str:
    target = ENV.lower()
    return (
        f"cd {AIRFLOW_HOME} "
        f"&& source dbt_env/bin/activate "
        f"&& cd transform "
        f"&& dbt deps "
        f"&& dbt build -s stg_{store}__products --target {target}"
        f"&& dbt build -s inc_{store}__sold_products --target {target} "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )


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
