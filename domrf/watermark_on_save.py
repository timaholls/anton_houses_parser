import io
import logging
from pathlib import Path
from typing import Optional

from PIL import Image

try:
    import cairosvg
except Exception:
    cairosvg = None

try:
    from .s3_service import S3Service  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError("Не найден domrf.s3_service.S3Service") from exc

logger = logging.getLogger("domrf.watermark_on_save")


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


def _watermark_bytes(
        image_bytes: bytes,
        logo_path: Path,
        full: bool = True,
        opacity: float = 0.15,
        rel_width: float = 0.2,
        position: str = "center",
        margin: int = 24,
) -> bytes:
    with Image.open(io.BytesIO(image_bytes)) as img:
        tmp_in = Path("/tmp/domrf_wm_in.jpg")
        tmp_out = Path("/tmp/domrf_wm_out.jpg")
        tmp_in.parent.mkdir(parents=True, exist_ok=True)
        fmt = "PNG" if img.mode in ("RGBA", "LA") else "JPEG"
        img.convert("RGBA" if fmt == "PNG" else "RGB").save(tmp_in, format=fmt)

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
        return f.read()


def upload_with_watermark(
        s3: S3Service,
        image_bytes: bytes,
        key: str,
        logo_path: Optional[Path] = None,
        overwrite_content_type: str = "image/jpeg",
) -> str:
    """Накладывает водяной знак и загружает в S3 тем же ключом.

    По умолчанию логотип берётся из корня проекта: pic-logo.svg
    """
    if logo_path is None:
        logo_path = Path("pic-logo.svg")

    logger.info("Добавление водяного знака и загрузка в S3: key=%s", key)
    watermarked = _watermark_bytes(
        image_bytes=image_bytes,
        logo_path=logo_path,
        full=True,
        opacity=0.15,
        rel_width=0.2,
        position="center",
    )
    return s3.upload_bytes(watermarked, key=key, content_type=overwrite_content_type)
