import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv
from botocore.config import Config
import boto3

# Загружаем .env из корня проекта
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path="domrf/.env")


class S3Service:
    """
    Обертка над boto3 для загрузки изображений в S3-совместимое хранилище.
    Ожидает следующие ENV переменные:
      - AWS_S3_ENDPOINT_URL
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
      - AWS_S3_REGION_NAME
      - AWS_STORAGE_BUCKET_NAME
    """

    def __init__(self):
        endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL")
        self.bucket_name = os.getenv("AWS_STORAGE_BUCKET_NAME")
        AWS_STORAGE_BUCKET_NAME = os.environ.get('AWS_STORAGE_BUCKET_NAME')

        # S3 access credentials
        AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

        # S3 endpoint for TimeWeb Cloud
        AWS_S3_ENDPOINT_URL = os.environ.get('AWS_S3_ENDPOINT_URL', 'https://s3.timeweb.cloud')

        # S3 region
        AWS_S3_REGION_NAME = os.environ.get('AWS_S3_REGION_NAME', 'ru-1')

        self.s3_client = boto3.client(
            's3',
            endpoint_url=AWS_S3_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_S3_REGION_NAME,
            use_ssl=False,
        )
        self.bucket_name = AWS_STORAGE_BUCKET_NAME

        self.public_base = f"{endpoint_url}/{self.bucket_name}"

    def build_url(self, key: str) -> str:
        """Формирует публичный URL для доступа к файлу в S3."""
        key = key.lstrip("/")
        return f"{self.public_base}/{key}"

    def upload_bytes(self, data: bytes, key: str, content_type: Optional[str] = "image/jpeg") -> str:
        """Загружает байты в S3 по ключу и возвращает публичный URL."""
        key = key.lstrip("/")

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
            ContentType='image/jpeg'
        )
        
        # Формируем URL как s3_url = f"https://s3.timeweb.cloud/{bucket}/{key}"
        return self.build_url(key)
