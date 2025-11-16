import io
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from PIL import Image

try:
    import cairosvg
except Exception:
    cairosvg = None

# Пытаемся импортировать S3Service из доступных модулей проекта
try:
    from domrf.s3_service import S3Service  # type: ignore
except Exception:
    try:
        from domclick.s3_service import S3Service  # type: ignore
    except Exception as exc:
        raise RuntimeError("Не удалось импортировать S3Service из domrf/domclick") from exc


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def ensure_svg_to_png(svg_path: Path, scale_width_px: int) -> Image.Image:
    if cairosvg is None:
        raise RuntimeError(
            "Требуется пакет 'cairosvg'. Установите зависимости из req.txt или выполните: pip install cairosvg"
        )

    with open(svg_path, "rb") as f:
        svg_bytes = f.read()

    png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=scale_width_px)
    return Image.open(io.BytesIO(png_bytes)).convert("RGBA")


def apply_watermark(
        photo_path: Path,
        svg_logo_path: Path,
        output_path: Path,
        relative_width: float = 0.2,
        opacity: float = 0.6,
        margin_px: int = 24,
        position: str = "bottom-right",
        full_coverage: bool = False,
):
    base = Image.open(photo_path).convert("RGBA")

    if full_coverage:
        target_size = int(min(base.width, base.height) * 0.8)
        target_logo_width = max(1, target_size)
        if opacity == 0.6:
            opacity = 0.15
        position = "center"
    else:
        target_logo_width = max(1, int(base.width * relative_width))

    logo_rgba = ensure_svg_to_png(svg_logo_path, target_logo_width)

    if opacity < 0:
        opacity = 0
    if opacity > 1:
        opacity = 1
    if logo_rgba.mode != "RGBA":
        logo_rgba = logo_rgba.convert("RGBA")
    r, g, b, a = logo_rgba.split()
    a = a.point(lambda p: int(p * opacity))
    logo_rgba = Image.merge("RGBA", (r, g, b, a))

    if position == "center":
        x = (base.width - logo_rgba.width) // 2
        y = (base.height - logo_rgba.height) // 2
    else:
        x = base.width - logo_rgba.width - margin_px
        y = base.height - logo_rgba.height - margin_px

    composed = base.copy()
    composed.alpha_composite(logo_rgba, dest=(max(0, x), max(0, y)))

    rgb = composed.convert("RGB")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb.save(output_path, format="JPEG", quality=92)


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
) -> Optional[str]:
    if not is_image_key(key):
        return None

    # Перезаписываем исходный файл
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
        relative_width=0.2,
        opacity=0.6,
        margin_px=24,
        position="center",
        full_coverage=True,
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
    
    s3 = S3Service()
    prefix = ""  # обработать все ключи в бакете
    logo_path = Path("/home/art/PycharmProjects/anton_houses_parser/pic-logo.svg")

    logger.info(
        "Запуск пакетной обработки: bucket=%s, prefix='%s', logo=%s",
        s3.bucket_name,
        prefix,
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
            )
            if out_key:
                processed += 1
                logger.info("Готово: %s -> %s", key, out_key)
        except Exception as e:  # pragma: no cover
            logger.exception("Ошибка при обработке %s: %s", key, e)

    logger.info("Завершено. Обработано изображений: %d", processed)


if __name__ == "__main__":
    main()


