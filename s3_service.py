import os
from typing import Optional

import boto3


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
        access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region_name = os.getenv("AWS_S3_REGION_NAME")
        self.bucket_name = os.getenv("AWS_STORAGE_BUCKET_NAME")

        # Проверяем обязательные переменные окружения
        if not endpoint_url:
            raise ValueError("AWS_S3_ENDPOINT_URL не установлена в переменных окружения")
        if not access_key_id:
            raise ValueError("AWS_ACCESS_KEY_ID не установлена в переменных окружения")
        if not secret_access_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY не установлена в переменных окружения")
        if not self.bucket_name:
            raise ValueError("AWS_STORAGE_BUCKET_NAME не установлена в переменных окружения")

        # Инициализация клиента S3
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name,
            use_ssl=False,
        )

        # Базовый публичный URL (если требуется строить абсолютные ссылки)
        # Для S3-совместимых стораджей обычно: {endpoint}/{bucket}/{key}
        self.public_base = f"{endpoint_url.rstrip('/')}/{self.bucket_name}"

    def build_url(self, key: str) -> str:
        key = key.lstrip("/")
        return f"{self.public_base}/{key}"

    def upload_bytes(self, data: bytes, key: str, content_type: Optional[str] = "image/jpeg") -> str:
        """Загружает байты в S3 по ключу и возвращает публичный URL."""
        key = key.lstrip("/")
        extra_args = {"ContentType": content_type}
        self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=data, **extra_args)
        return self.build_url(key)


