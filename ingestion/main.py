import datetime
import json
import math
import random
import tempfile
import traceback

import boto3
import config as cfg
import requests
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from logger import get_logger
from utils import Timer, generate_headers, retry, sleep_for_random_n_seconds, check_trigger_file

log = get_logger(__name__)

NOW_DATE_UTC = datetime.datetime.now(datetime.UTC)
AWS_S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=cfg.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=cfg.AWS_SECRET_ACCESS_KEY,
    region_name=cfg.AWS_REGION_NAME,
)
AWS_S3_ROOT_FOLDER = (
    f"{cfg.SHOPIFY_STORE_NAME}/"
    f"{NOW_DATE_UTC.year}/"
    f"{NOW_DATE_UTC.month}/"
    f"{NOW_DATE_UTC.day}"
)


@retry(max_retries=3, initial_delay=60, max_delay=240)
def fetch_total_product_count(url: str) -> int:
    headers = generate_headers()
    with Timer("Total time taken to fetch product count", unit="milliseconds"):
        r = requests.get(url, headers=headers)
    log.info(f"Successfully received response from {url=}, status_code={r.status_code}")
    data = r.json()
    return data["collection"]["products_count"]


def generate_scraping_urls(
    base_url: str, count: int, page_limit: int = 250
) -> list[str]:
    max_pages = math.ceil(count / page_limit) + 1
    urls = [base_url.format(page_limit, n) for n in range(1, max_pages)]
    urls = random.sample(urls, len(urls))
    log.info(
        f"Successfully created {len(urls)} urls based on product {count=} and {page_limit=}"
    )
    return urls


@retry(max_retries=3, initial_delay=60, max_delay=240)
def fetch_product_data_from_url(url: str) -> dict:
    headers = generate_headers()
    with Timer("Total time taken to fetch product data", unit="milliseconds"):
        r = requests.get(url, headers=headers)
    log.info(f"Successfully received response from {url=}, status_code={r.status_code}")
    return r.json()


def create_empty_trigger_file_locally() -> str:
    """Creates an empty trigger.txt file locally."""
    file_path = "trigger.txt"
    with open(file_path, "w") as f:
        pass
    log.info(f"Successfully created an empty trigger file at {file_path=}")
    return file_path


def create_empty_trigger_file_s3(s3_client, bucket_name, s3_key):
    """Creates an empty trigger.txt file in S3."""
    try:
        upload_path = f"s3://{bucket_name}/{s3_key}"
        log.info(f"Starting to upload an empty trigger file to {upload_path}...")
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body="")
        log.info(f"Successfully uploaded an empty trigger file to {upload_path}")
        return upload_path
    except (NoCredentialsError, PartialCredentialsError):
        log.error("Error: AWS credentials not found or are incomplete.")
    except ClientError as e:
        log.error(f"Error occurred while uploading to S3: {e}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e=}, tb={traceback.format_exc()}")


@retry(max_retries=5, initial_delay=1, max_delay=60)
def create_empty_trigger_file_remotely() -> str:
    """Creates an empty trigger.txt file in a remote store (e.g., S3)."""
    s3_client = AWS_S3_CLIENT
    bucket_name = cfg.AWS_S3_BUCKET_NAME
    s3_key = f"{AWS_S3_ROOT_FOLDER}/trigger.txt"

    upload_path = create_empty_trigger_file_s3(s3_client, bucket_name, s3_key)
    return upload_path


def create_trigger_file():
    """Creates an empty trigger.txt file, either locally or remotely based on environment."""
    file_path = None
    with Timer("Total time taken to create trigger file", unit="milliseconds"):
        if cfg.ENV.lower() == "dev":
            file_path = create_empty_trigger_file_locally()
        elif cfg.ENV.lower() == "prod":
            file_path = create_empty_trigger_file_remotely()
    return file_path


def save_json_to_local_file(data: dict) -> str:
    file_path = None
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w") as f:
        json.dump(data, f, indent=4)
        f.close()
        file_path = f.name
    log.info(f"Successfully saved JSON response to {file_path=}")
    return file_path


def save_json_to_s3(s3_client, bucket_name, s3_key, data):
    try:
        json_data = json.dumps(data)
        upload_path = f"s3://{bucket_name}/{s3_key}"
        log.info(
            f"Starting to upload JSON file of size={len(json_data)} to {upload_path}..."
        )
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_data)
        log.info(f"Successfully uploaded JSON data to {upload_path}")
        return upload_path
    except (NoCredentialsError, PartialCredentialsError):
        log.error("Error: AWS credentials not found or are incomplete.")
    except ClientError as e:
        log.error(f"Error occurred while uploading to S3: {e}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e=}, tb={traceback.format_exc()}")


@retry(max_retries=5, initial_delay=1, max_delay=60)
def save_json_to_remote_store(data: dict) -> str:
    s3_client = AWS_S3_CLIENT
    bucket_name = cfg.AWS_S3_BUCKET_NAME
    timestamp = int(datetime.datetime.now(datetime.UTC).timestamp())
    s3_key = f"{AWS_S3_ROOT_FOLDER}/product_data_{timestamp}.json"

    upload_path = save_json_to_s3(s3_client, bucket_name, s3_key, data)

    return upload_path


def save_json_data_to_file(data: dict):
    file_path = None
    with Timer("Total time taken to save JSON data", unit="milliseconds"):
        if cfg.ENV.lower() == "dev":
            file_path = save_json_to_local_file(data)
        elif cfg.ENV.lower() == "prod":
            file_path = save_json_to_remote_store(data)
    return file_path


def main():
    max_exception_count = 5
    exception_count = 0
    processed_urls_count = 0

    product_count_url = cfg.SHOPIFY_PRODUCT_COUNT_URL
    base_url = cfg.SHOPIFY_PRODUCTS_URL

    if check_trigger_file(AWS_S3_CLIENT, cfg.AWS_S3_BUCKET_NAME, AWS_S3_ROOT_FOLDER):
        log.info(f'Exiting script since data was probably prior downloaded...')
        return

    count = fetch_total_product_count(product_count_url)
    urls = generate_scraping_urls(base_url, count)

    for i, url in enumerate(urls, 1):
        if exception_count > max_exception_count:
            raise RuntimeError(
                f"Maximum exception count {max_exception_count} reached, exiting..."
            )
        try:
            log.info(f"Starting to process {i=} {url}...")
            data = fetch_product_data_from_url(url)
            save_json_data_to_file(data)
            processed_urls_count += 1
            if i < len(urls):
                sleep_for_random_n_seconds()
        except Exception as e:
            log.error(f"Failed to scrape {url=}, {e=}, {traceback.format_exc()}")
            exception_count += 1

    create_trigger_file()

    log.info(f"Successfully finished ingesting {processed_urls_count=}, exiting.")


if __name__ == "__main__":
    main()
