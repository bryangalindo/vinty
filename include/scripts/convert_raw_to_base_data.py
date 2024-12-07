import datetime
import io
import json
import logging

import boto3
import pandas as pd
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


def json_to_parquet_s3(input_bucket, output_bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=prefix)
    dfs = []
    objects = response.get("Contents", [])
    log.info(f"Found {len(objects)} objects in {input_bucket=}, {prefix=}")
    for i, obj in enumerate(objects):
        key = obj["Key"]
        log.info(f"Starting to process {i=}, {key=}...")
        if key.endswith(".json"):
            json_obj = s3_client.get_object(Bucket=input_bucket, Key=key)
            json_data = json.loads(json_obj["Body"].read().decode("utf-8"))
            data_to_convert = json_data["products"]
            df = pd.json_normalize(data_to_convert)
            dfs.append(df)

    parquet_buffer = io.BytesIO()

    df = pd.concat(dfs, ignore_index=True)
    df = df.sort_values(by="vendor")
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)

    timestamp = int(datetime.datetime.now(datetime.UTC).timestamp())
    parquet_key = f"{prefix}/product_data_{timestamp}.parquet"

    s3_client.put_object(
        Bucket=output_bucket, Key=parquet_key, Body=parquet_buffer.getvalue()
    )
    log.info(f"Created new consolidated {parquet_key=} and saved to {output_bucket=}")


def main(store: str, input_bucket: str, output_bucket: str, iso_timestamp: str):
    log.info(
        f"Starting conversion process with following params: "
        f"{input_bucket=}, {output_bucket=}, {store=}, {iso_timestamp=}"
    )

    input_dt = datetime.datetime.fromisoformat(iso_timestamp)
    input_dt_utc = input_dt.replace(tzinfo=datetime.timezone.utc)
    prefix = f"{store}/{input_dt_utc.year}/{input_dt_utc.month}/{input_dt_utc.day}"

    json_to_parquet_s3(input_bucket, output_bucket, prefix)
