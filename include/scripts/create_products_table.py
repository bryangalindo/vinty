import datetime
import logging

import boto3
from airflow.models import Variable
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

AWS_S3_BASE_DATA_BUCKET = Variable.get("AWS_S3_BASE_DATA_BUCKET_NAME")
AWS_S3_WAREHOUSE_BUCKET = Variable.get("AWS_S3_WAREHOUSE_BUCKET_NAME")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")
ENV = Variable.get("ENV")

ICEBERG_TABLE = "products" if ENV == "prod" else "dev_products"
ICEBERG_CATALOG = "vinty"


log = logging.getLogger(__name__)

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

glue_client = boto3.client(
    "glue",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.723,"
    "org.apache.iceberg:iceberg-aws-bundle:1.7.0,"
)


def get_table(client, db, table):
    try:
        return client.get_table(DatabaseName=db, Name=table)
    except Exception as e:
        return False


def build_spark_session():
    conf = (
        SparkConf()
        .setAppName("CreateIcebergTable")
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
    absolute_table_path = f"{ICEBERG_CATALOG}.{store}.{ICEBERG_TABLE}"

    if table := get_table(glue_client, store, ICEBERG_TABLE):
        log.info(f"Exiting task since table exists {absolute_table_path=}, {table=}...")
        return

    spark = build_spark_session()

    run_date = datetime.datetime.fromisoformat(iso_timestamp)
    run_date = run_date.replace(tzinfo=datetime.timezone.utc)
    ingestion_date = run_date.strftime("%Y-%m-%d")

    prefix = f"{store}" f"/{run_date.year}" f"/{run_date.month}" f"/{run_date.day}"

    response = s3_client.list_objects_v2(Bucket=AWS_S3_BASE_DATA_BUCKET, Prefix=prefix)

    key = response["Contents"][0]["Key"]
    parquet_file = f"s3a://{AWS_S3_BASE_DATA_BUCKET}/{key}"
    log.info(f"Starting to read {parquet_file}...")

    df = spark.read.parquet(parquet_file)
    df = df.withColumn("ingestion_date", lit(ingestion_date))
    df = df.limit(0)

    df.write.format("iceberg").option(
        "path", f"s3://{AWS_S3_WAREHOUSE_BUCKET}/{store}"
    ).mode("overwrite").saveAsTable(absolute_table_path)

    schema = df.schema.simpleString()
    log.info(
        f"Successfully created new iceberg table {absolute_table_path} with {schema=}"
    )
