import io
import logging
import sys
from pathlib import Path
from typing import Iterable, List, Optional

from PIL import Image

# Пытаемся импортировать S3Service из доступных модулей проекта
try:
    from domrf.s3_service import S3Service  # type: ignore
except Exception:
    try:
        from domclick.s3_service import S3Service  # type: ignore
    except Exception as exc:
        raise RuntimeError("Не удалось импортировать S3Service из domrf/domclick") from exc

# Используем функции наложения водяного знака из тестового скрипта
try:
    from watermark_test import apply_watermark
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "Не удалось импортировать apply_watermark из watermark_test.py"
    ) from exc


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def is_image_key(key: str, exts: Optional[Iterable[str]] = None) -> bool:
    extensions = set(exts) if exts else IMAGE_EXTENSIONS
    suffix = Path(key).suffix.lower()
    return suffix in extensions


def list_all_objects(s3_client, bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    continuation_token: Optional[str] = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get("Contents", []) or []
        for obj in contents:
            key = obj.get("Key")
            if key:
                keys.append(key)
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def download_image_to_temp(s3_client, bucket: str, key: str) -> Image.Image:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    img = Image.open(io.BytesIO(body))
    return img


def process_key(
    s3: S3Service,
    key: str,
    logo_path: Path,
    dest_prefix: Optional[str],
    overwrite: bool,
    rel_width: float,
    opacity: float,
    margin: int,
    position: str,
    full: bool,
) -> Optional[str]:
    if not is_image_key(key):
        return None

    # Куда сохраняем
    if dest_prefix:
        filename = key.split("/")[-1]
        out_key = f"{dest_prefix.rstrip('/')}/{filename}".lstrip("/")
    else:
        if not overwrite:
            # По умолчанию пишем в тот же путь с постфиксом
            p = Path(key)
            out_key = str(p.with_stem(p.stem + "_wm"))
        else:
            out_key = key

    # Скачиваем, применяем водяной знак, загружаем обратно
    obj = s3.s3_client.get_object(Bucket=s3.bucket_name, Key=key)
    raw = obj["Body"].read()

    with Image.open(io.BytesIO(raw)) as base_img:
        tmp_in = Path("/tmp/input_image.jpg")
        tmp_in.parent.mkdir(parents=True, exist_ok=True)
        # Сохраняем во временный файл (apply_watermark ожидает пути)
        fmt = "PNG" if base_img.mode in ("RGBA", "LA") else "JPEG"
        base_img.convert("RGBA" if fmt == "PNG" else "RGB").save(tmp_in, format=fmt)

    tmp_out = Path("/tmp/output_image.jpg")
    apply_watermark(
        photo_path=tmp_in,
        svg_logo_path=logo_path,
        output_path=tmp_out,
        relative_width=rel_width,
        opacity=opacity,
        margin_px=margin,
        position=position,
        full_coverage=full,
    )

    with open(tmp_out, "rb") as f:
        out_bytes = f.read()

    content_type = "image/jpeg"
    s3.upload_bytes(out_bytes, key=out_key, content_type=content_type)
    return out_key


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger("watermark_s3")
    # Настройки по умолчанию: как watermark_test.py --full
    s3 = S3Service()
    prefix = ""  # без аргументов: обработать все ключи в бакете
    logo_path = Path("/home/art/PycharmProjects/anton_houses_parser/pic-logo.svg")
    rel_width = 0.2
    opacity = 0.15
    margin = 24
    position = "center"
    full = True
    dest_prefix: Optional[str] = None
    overwrite = True  # перезаписываем исходные файлы

    logger.info(
        "Запуск пакетной обработки: bucket=%s, prefix='%s', overwrite=%s, full=%s, position=%s, opacity=%.2f, logo=%s",
        s3.bucket_name,
        prefix,
        overwrite,
        full,
        position,
        opacity,
        str(logo_path),
    )

    keys = list_all_objects(s3.s3_client, s3.bucket_name, prefix)
    if not keys:
        logger.warning("Нет объектов по данному префиксу")
        return

    total = len(keys)
    images = sum(1 for k in keys if is_image_key(k))
    logger.info("Найдено ключей: %d, из них изображений: %d", total, images)

    processed = 0
    for key in keys:
        if not is_image_key(key):
            continue
        try:
            logger.info("Обработка: %s", key)
            out_key = process_key(
                s3=s3,
                key=key,
                logo_path=logo_path,
                dest_prefix=dest_prefix,
                overwrite=overwrite,
                rel_width=rel_width,
                opacity=opacity,
                margin=margin,
                position=position,
                full=full,
            )
            if out_key:
                processed += 1
                logger.info("Готово: %s -> %s", key, out_key)
        except Exception as e:  # pragma: no cover
            logger.exception("Ошибка при обработке %s: %s", key, e)

    logger.info("Завершено. Обработано изображений: %d", processed)


if __name__ == "__main__":
    main()


