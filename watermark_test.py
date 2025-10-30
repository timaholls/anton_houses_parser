import argparse
from io import BytesIO
from pathlib import Path

from PIL import Image

try:
    import cairosvg
except Exception as exc:  # pragma: no cover
    cairosvg = None


def ensure_svg_to_png(svg_path: Path, scale_width_px: int) -> Image.Image:
    if cairosvg is None:
        raise RuntimeError(
            "Требуется пакет 'cairosvg'. Установите зависимости из req.txt или выполните: pip install cairosvg"
        )

    with open(svg_path, "rb") as f:
        svg_bytes = f.read()

    # Рендерим SVG в PNG с нужной шириной (высота подстроится пропорционально)
    png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=scale_width_px)
    return Image.open(BytesIO(png_bytes)).convert("RGBA")


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

    # Если режим полного покрытия - увеличиваем размер и делаем более прозрачным
    if full_coverage:
        # Размер логотипа - 80% от меньшей стороны фото для полного покрытия
        target_size = int(min(base.width, base.height) * 0.8)
        target_logo_width = max(1, target_size)
        # Используем низкую прозрачность для водяного знака
        if opacity == 0.6:  # значение по умолчанию
            opacity = 0.15
        position = "center"
    else:
        # Размер водяного знака как доля ширины исходного фото
        target_logo_width = max(1, int(base.width * relative_width))
    
    logo_rgba = ensure_svg_to_png(svg_logo_path, target_logo_width)

    # Прозрачность
    if opacity < 0:
        opacity = 0
    if opacity > 1:
        opacity = 1
    if logo_rgba.mode != "RGBA":
        logo_rgba = logo_rgba.convert("RGBA")
    r, g, b, a = logo_rgba.split()
    a = a.point(lambda p: int(p * opacity))
    logo_rgba = Image.merge("RGBA", (r, g, b, a))

    # Позиционирование
    if position == "center":
        x = (base.width - logo_rgba.width) // 2
        y = (base.height - logo_rgba.height) // 2
    else:
        # default: bottom-right
        x = base.width - logo_rgba.width - margin_px
        y = base.height - logo_rgba.height - margin_px

    # Композиция
    composed = base.copy()
    composed.alpha_composite(logo_rgba, dest=(max(0, x), max(0, y)))

    # Сохранение в JPEG (без альфа)
    rgb = composed.convert("RGB")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb.save(output_path, format="JPEG", quality=92)


def main():
    default_photo = Path("/home/art/PycharmProjects/anton_houses_parser/img.png")
    default_logo = Path("/home/art/PycharmProjects/anton_houses_parser/pic-logo.svg")
    default_out = Path("/home/art/PycharmProjects/anton_houses_parser/watermarked_photo.jpg")

    parser = argparse.ArgumentParser(description="Наложение SVG-водяного знака на фото")
    parser.add_argument("--photo", type=Path, default=default_photo, help="Путь к исходному фото")
    parser.add_argument("--logo", type=Path, default=default_logo, help="Путь к SVG-логотипу")
    parser.add_argument(
        "--out", type=Path, default=default_out, help="Путь для сохранения результата (JPEG)"
    )
    parser.add_argument(
        "--rel-width",
        type=float,
        default=0.2,
        help="Ширина логотипа как доля ширины фото (напр. 0.2 = 20%)",
    )
    parser.add_argument(
        "--opacity",
        type=float,
        default=0.6,
        help="Прозрачность логотипа [0..1] (1 — непрозрачный)",
    )
    parser.add_argument(
        "--margin",
        type=int,
        default=24,
        help="Отступ от краёв (px), если позиция bottom-right",
    )
    parser.add_argument(
        "--position",
        type=str,
        choices=["bottom-right", "center"],
        default="bottom-right",
        help="Позиция логотипа",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Режим полного покрытия: большой прозрачный водяной знак на всё фото",
    )

    args = parser.parse_args()

    apply_watermark(
        photo_path=args.photo,
        svg_logo_path=args.logo,
        output_path=args.out,
        relative_width=args.rel_width,
        opacity=args.opacity,
        margin_px=args.margin,
        position=args.position,
        full_coverage=args.full,
    )

    print(f"Готово: {args.out}")


if __name__ == "__main__":
    main()


