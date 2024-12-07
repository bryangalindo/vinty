import datetime
import logging

from airflow.models import Variable
from pyspark import SparkConf
from pyspark.sql import SparkSession

AWS_S3_WAREHOUSE_BUCKET = Variable.get("AWS_S3_WAREHOUSE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")
ENV = Variable.get("ENV")

ICEBERG_CATALOG = "vinty"
ICEBERG_TABLE = "products" if ENV == "prod" else "dev_products"

log = logging.getLogger(__name__)

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.723,"
    "org.apache.iceberg:iceberg-aws-bundle:1.7.0,"
)


def build_spark_session():
    conf = (
        SparkConf()
        .setAppName("AddNewProductRows")
        .set("spark.jars.packages", SPARK_PACKAGES)
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
        .set(f"spark.sql.catalog.{ICEBERG_CATALOG}.aws.region", AWS_REGION)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.region", AWS_REGION)
    )
    return SparkSession.builder.master("local[*]").config(conf=conf).getOrCreate()


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
