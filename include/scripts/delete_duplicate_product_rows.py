import datetime
import logging
import sys

from airflow.models import Variable
from pyspark import SparkConf
from pyspark.sql import SparkSession

AWS_S3_WAREHOUSE_BUCKET = Variable.get("AWS_S3_WAREHOUSE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = Variable.get("AWS_REGION")
ENV = Variable.get("ENV")

ICEBERG_CATALOG = "vinty"
ICEBERG_TABLE = "products" if ENV == "prod" else "dev_products"

log = logging.getLogger(__name__)


def build_spark_session():
    conf = (
        SparkConf()
        .setAppName("DeleteDuplicateProductRows")
        .set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            f"s3://{AWS_S3_WAREHOUSE_BUCKET}/",
        )
        .set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()


def main(store: str, iso_timestamp: str):
    spark = build_spark_session()

    absolute_table_path = f"{ICEBERG_CATALOG}.{store}.{ICEBERG_TABLE}"

    run_date = datetime.datetime.fromisoformat(iso_timestamp)
    run_date = run_date.replace(tzinfo=datetime.timezone.utc)
    ingestion_date = run_date.strftime("%Y-%m-%d")

    old_count = spark.sql(f"SELECT COUNT(*) FROM {absolute_table_path}").collect()[0][0]
    spark.sql(
        f"""
            DELETE FROM {absolute_table_path}
            WHERE ingestion_date = '{ingestion_date}'
        """
    )
    new_count = spark.sql(f"SELECT COUNT(*) FROM {absolute_table_path}").collect()[0][0]

    log.info(
        f"Successfully deleted records with {ingestion_date} "
        f"from table={absolute_table_path}, {old_count=}, {new_count=}"
    )


if __name__ == "__main__":
    store = sys.argv[1]
    iso_timestamp = sys.argv[2]
    main(store, iso_timestamp)
