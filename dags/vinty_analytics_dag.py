import datetime
import logging
import os

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.scripts import (
    add_new_products,
    convert_raw_to_base_data,
    create_products_table,
    delete_duplicate_base_data,
    delete_duplicate_product_rows,
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

ICEBERG_TREASURES_STORE_DB = "treasures"
ICEBERG_VSP_STORE_DB = "vsp"
ICEBERG_REBAG_STORE_DB = "rebag"
ICEBERG_CATALOG = "vinty"
ICEBERG_DCT_STORE_DB = "dct"

DBT_TARGET = ENV.lower()


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
        task_id="convert_raw_to_base",
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
        task_id="convert_raw_to_base",
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
        task_id="create_products_table",
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def create_rebag_products_table_task():
    return PythonOperator(
        task_id="create_products_table",
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_vsp_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_duplicate_product_rows",
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_rebag_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_duplicate_product_rows",
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_vsp_products_task():
    return PythonOperator(
        task_id="add_new_products",
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_VSP_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_rebag_products_task():
    return PythonOperator(
        task_id="add_new_products",
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_REBAG_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_dct_duplicate_base_data_task():
    return PythonOperator(
        task_id="delete_duplicate_base_data",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=delete_duplicate_base_data.main,
        op_args=(
            ICEBERG_DCT_STORE_DB,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def convert_dct_raw_data_to_base_data_task():
    return PythonOperator(
        task_id="convert_raw_to_base",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=convert_raw_to_base_data.main,
        op_args=(
            ICEBERG_DCT_STORE_DB,
            AWS_S3_BUCKET_NAME,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def create_dct_products_table_task():
    return PythonOperator(
        task_id="create_products_table",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_DCT_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_dct_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_duplicate_product_rows",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_DCT_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_dct_products_task():
    return PythonOperator(
        task_id="add_new_products",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_DCT_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_treasures_duplicate_base_data_task():
    return PythonOperator(
        task_id="delete_duplicate_base_data",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=delete_duplicate_base_data.main,
        op_args=(
            ICEBERG_TREASURES_STORE_DB,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def convert_treasures_raw_data_to_base_data_task():
    return PythonOperator(
        task_id="convert_raw_to_base",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=convert_raw_to_base_data.main,
        op_args=(
            ICEBERG_TREASURES_STORE_DB,
            AWS_S3_BUCKET_NAME,
            AWS_S3_BASE_DATA_BUCKET_NAME,
            "{{ ds }}",
        ),
    )


def create_treasures_products_table_task():
    return PythonOperator(
        task_id="create_products_table",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=create_products_table.main,
        op_args=(
            ICEBERG_TREASURES_STORE_DB,
            "{{ ds }}",
        ),
    )


def delete_treasures_duplicate_product_rows_task():
    return PythonOperator(
        task_id="delete_duplicate_product_rows",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=delete_duplicate_product_rows.main,
        op_args=(
            ICEBERG_TREASURES_STORE_DB,
            "{{ ds }}",
        ),
    )


def add_new_treasures_products_task():
    return PythonOperator(
        task_id="add_new_products",
        start_date=datetime.datetime(2024, 12, 8),
        python_callable=add_new_products.main,
        op_args=(
            ICEBERG_TREASURES_STORE_DB,
            "{{ ds }}",
        ),
    )


def build_dbt_bash_command_prefix() -> str:
    return (
        f"cd {AIRFLOW_HOME} "
        f"&& source dbt_env/bin/activate "
        f"&& cd transform "
        f"&& dbt deps "
    )


def create_vsp_stg_models_task():
    command = build_dbt_bash_command_prefix()
    command += f"&& dbt build -s stg_vsp__products --target {DBT_TARGET} "
    return BashOperator(
        task_id="create_stg_models",
        bash_command=command,
    )


def create_vsp_inc_models_task():
    command = build_dbt_bash_command_prefix()
    command += (
        f"&& dbt build -s inc_vsp__sold_products --target {DBT_TARGET} "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )
    return BashOperator(
        task_id="create_inc_models",
        bash_command=command,
    )


def create_rebag_stg_models_task():
    command = build_dbt_bash_command_prefix()
    command += f"&& dbt build -s stg_rebag__products --target {DBT_TARGET}"
    return BashOperator(
        task_id="create_stg_models",
        bash_command=command,
    )


def create_rebag_inc_models_task():
    command = build_dbt_bash_command_prefix()
    command += (
        f"&& dbt build -s inc_rebag__sold_products --target {DBT_TARGET} "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )
    return BashOperator(
        task_id="create_inc_models",
        bash_command=command,
    )


def create_dct_stg_models_task():
    command = build_dbt_bash_command_prefix()
    command += f"&& dbt build -s stg_dct__products --target {DBT_TARGET}"
    return BashOperator(
        task_id="create_stg_models",
        bash_command=command,
    )


def create_dct_inc_models_task():
    command = build_dbt_bash_command_prefix()
    command += (
        f"&& dbt build -s inc_dct__sold_products --target {DBT_TARGET} "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )
    return BashOperator(
        task_id="create_inc_models",
        bash_command=command,
    )


def create_treasures_stg_models_task():
    command = build_dbt_bash_command_prefix()
    command += f"&& dbt build -s stg_dct__products --target {DBT_TARGET}"
    return BashOperator(
        task_id="create_stg_models",
        bash_command=command,
    )


def create_treasures_inc_models_task():
    command = build_dbt_bash_command_prefix()
    command += (
        f"&& dbt build -s inc_dct__sold_products --target {DBT_TARGET} "
        f'--vars "{{"TODAY": "{AIRFLOW_EXECUTION_DATE}", '
        f'"YESTERDAY": "{AIRFLOW_PREVIOUS_EXECUTION_DATE}"}}"'
    )
    return BashOperator(
        task_id="create_inc_models",
        bash_command=command,
    )


def create_sold_products_model():
    command = build_dbt_bash_command_prefix()
    command += f"&& dbt build -s sold_products --target {DBT_TARGET}"
    return BashOperator(
        task_id="create_sold_products_model",
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
    start = EmptyOperator(task_id="start")

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

    with TaskGroup("dct_ingestion_tasks") as dct_ingestion_tasks:
        delete_duplicate_base_data = delete_dct_duplicate_base_data_task()
        convert_raw_data_to_base_data = convert_dct_raw_data_to_base_data_task()
        create_products_table = create_dct_products_table_task()
        delete_duplicate_product_rows = delete_dct_duplicate_product_rows_task()
        add_new_products = add_new_dct_products_task()

        (
            delete_duplicate_base_data
            >> convert_raw_data_to_base_data
            >> create_products_table
            >> delete_duplicate_product_rows
            >> add_new_products
        )

    with TaskGroup("treasures_ingestion_tasks") as treasures_ingestion_tasks:
        delete_duplicate_base_data = delete_treasures_duplicate_base_data_task()
        convert_raw_data_to_base_data = convert_treasures_raw_data_to_base_data_task()
        create_products_table = create_treasures_products_table_task()
        delete_duplicate_product_rows = delete_treasures_duplicate_product_rows_task()
        add_new_products = add_new_treasures_products_task()

        (
            delete_duplicate_base_data
            >> convert_raw_data_to_base_data
            >> create_products_table
            >> delete_duplicate_product_rows
            >> add_new_products
        )

    join_ingestion_tasks = EmptyOperator(task_id="join_ingestion_tasks")
    start_transformations = EmptyOperator(task_id="start_transformations")

    with TaskGroup("vsp_transform_tasks") as vsp_transform_tasks:
        create_vsp_stg_models = create_vsp_stg_models_task()
        create_vsp_inc_models = create_vsp_inc_models_task()

        create_vsp_stg_models >> create_vsp_inc_models

    with TaskGroup("rebag_transform_tasks") as rebag_transform_tasks:
        create_rebag_stg_models = create_rebag_stg_models_task()
        create_rebag_inc_models = create_rebag_inc_models_task()

        create_rebag_stg_models >> create_rebag_inc_models

    with TaskGroup("dct_transform_tasks") as dct_transform_tasks:
        create_dct_stg_models = create_dct_stg_models_task()
        create_dct_inc_models = create_dct_inc_models_task()

        create_dct_stg_models >> create_dct_inc_models

    with TaskGroup("treasures_transform_tasks") as treasures_transform_tasks:
        create_treasures_stg_models = create_treasures_stg_models_task()
        create_treasures_inc_models = create_treasures_inc_models_task()

        create_treasures_stg_models >> create_treasures_inc_models

    end_transformations = EmptyOperator(task_id="end_transformations")
    end = EmptyOperator(task_id="end")

    (
        start
        >> [
            vsp_ingestion_tasks,
            rebag_ingestion_tasks,
            dct_ingestion_tasks,
            treasures_ingestion_tasks,
        ]
        >> join_ingestion_tasks
        >> start_transformations
        >> [
            vsp_transform_tasks,
            rebag_transform_tasks,
            dct_transform_tasks,
            treasures_transform_tasks,
        ]
        >> create_sold_products_model
        >> end_transformations
        >> end
    )


vinty_analytics_pipeline()
