import os
from dotenv import load_dotenv

from logger import get_logger

log = get_logger(__name__)

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

ENV = os.getenv('ENV')

log.info(f'Successfully loaded .env from {dotenv_path=}, current {ENV=}')

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')

SHOPIFY_STORE_NAME = os.getenv('SHOPIFY_STORE_NAME')
SHOPIFY_PRODUCT_COUNT_URL = os.getenv('SHOPIFY_PRODUCT_COUNT_URL')
SHOPIFY_PRODUCTS_URL = os.getenv('SHOPIFY_PRODUCTS_URL')