import datetime
import logging
import os
import traceback

import boto3

AWS_S3_RAW_DATA_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

log = logging.getLogger(__name__)


client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def create_empty_trigger_file_s3(s3_client, bucket_name, s3_key):
    try:
        upload_path = f"s3://{bucket_name}/{s3_key}"
        log.info(f"Starting to upload an empty trigger file to {upload_path}...")
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body="")
        log.info(f"Successfully uploaded an empty trigger file to {upload_path}")
        return upload_path
    except Exception as e:
        log.error(f"An unexpected error occurred: {e=}, tb={traceback.format_exc()}")


def main(start_date="2024-11-13", end_date="2024-12-10"):
    bucket = AWS_S3_RAW_DATA_BUCKET_NAME

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    for date in date_range(start_date, end_date):
        for store in ["vsp", "rebag", "dct", "treasures"]:
            key = (
                f"{store}/"
                f"{date.year}/"
                f"{date.month}/"
                f"{date.day}/"
                f"trigger.txt"
            )
            log.info(f"Starting to backfill trigger file for {bucket=}, {key=}")
            create_empty_trigger_file_s3(client, bucket, key)


if __name__ == "__main__":
    main()
