import datetime
import logging

import boto3
from airflow.models import Variable

log = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = Variable.get("AWS_REGION")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME,
)


def delete_parquet_files(output_bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=output_bucket, Prefix=prefix)

    if "Contents" not in response:
        log.info(f"Exiting task, no files to delete in {output_bucket=}, {prefix=}")
    else:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]

        delete_response = s3_client.delete_objects(
            Bucket=output_bucket, Delete={"Objects": objects_to_delete, "Quiet": True}
        )

        log.info(f"Successfully deleted object {delete_response=}")


def main(store: str, output_bucket: str, iso_timestamp: str):
    log.info(
        f"Starting deletion process with following params: "
        f"{output_bucket=}, {store=}, {iso_timestamp=}"
    )

    input_dt = datetime.datetime.fromisoformat(iso_timestamp)
    input_dt_utc = input_dt.replace(tzinfo=datetime.timezone.utc)
    prefix = f"{store}/{input_dt_utc.year}/{input_dt_utc.month}/{input_dt_utc.day}"

    delete_parquet_files(output_bucket, prefix)
