import os
import datetime
import random
from io import BytesIO
from PIL import Image, ExifTags, PngImagePlugin
from logging import Logger
import piexif


class ImageProcessor:
    def __init__(self, logger: Logger, max_size=(1000, 1000), max_kb=100, ):
        self.max_size = max_size
        self.max_kb = max_kb
        self.logger = logger

    def print_image_metadata(self, img):
        # print("== МЕТАДАННЫЕ ИЗОБРАЖЕНИЯ ==")
        # print(f"Формат: {img.format}")
        # print(f"Размер: {img.size}")
        # print(f"Цветовой режим: {img.mode}")
        pass

    def generate_random_date(self):
        year = random.choice([2021, 2022, 2023])
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return f"{year:04d}:{month:02d}:{day:02d} {hour:02d}:{minute:02d}:{second:02d}"

    def resize_and_compress(self, input_bytes):
        try:
            img = Image.open(input_bytes)
        except Exception as e:
            self.logger.error(f"Проблема открытия изображения: {e}")
            return None

        # print("\nИсходные данные:")
        self.print_image_metadata(img)

        if img.mode in ('RGBA', 'LA'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1])
            img = background
        else:
            img = img.convert('RGB')

        img.thumbnail(self.max_size)

        quality = 95
        while quality >= 10:
            buffer = BytesIO()
            img.save(buffer, format='JPEG', quality=quality)
            size_kb = buffer.tell() / 1024
            if size_kb <= self.max_kb:
                # print(f"\n✅ Сжатие успешно ({size_kb:.2f} КБ, качество: {quality})")
                buffer.seek(0)
                return buffer
            quality -= 5

        self.logger.warning("\n❌ Не удалось сжать до нужного размера")
        return None

    def update_metadata(self, img_bytes):
        try:
            img = Image.open(img_bytes)
            img_bytes.seek(0)
        except Exception as e:
            self.logger.error(f"Проблема открытия изображения для обновления метаданных: {e}")
            return None

        random_date_str = self.generate_random_date()
        # print("\n== ОБНОВЛЕНИЕ МЕТАДАННЫХ ==")
        # print(f"Новая дата: {random_date_str}")

        output_bytes = BytesIO()
        if img.format.upper() == "JPEG":
            try:

                exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "Interop": {}, "1st": {}, "thumbnail": None}
                exif_dict["0th"][piexif.ImageIFD.Artist] = 'century21-mir-v-kvadratah'
                exif_dict["0th"][piexif.ImageIFD.DateTime] = random_date_str
                exif_bytes = piexif.dump(exif_dict)
                img.save(output_bytes, "jpeg", exif=exif_bytes)
                # print("✅ Обновлены метаданные для JPEG")
            except Exception as e:
                self.logger.warning(f"❌ Неудачное обновление метаданных для JPEG: {e}")
        elif img.format.upper() == "PNG":
            try:
                pnginfo = PngImagePlugin.PngInfo()
                pnginfo.add_text("Author", 'century21-mir-v-kvadratah')
                pnginfo.add_text("Date", random_date_str)
                img.save(output_bytes, "png", pnginfo=pnginfo)
                # print("✅ Обновлены метаданные для PNG")
            except Exception as e:
                self.logger.warning(f"❌ Неудачное обновление метаданных для PNG: {e}")
        else:
            self.logger.warning(f"❌ Обновление метаданных не реализовано для {img.format}")

        output_bytes.seek(0)
        return output_bytes

    def process(self, input_bytes):
        processed_bytes = self.resize_and_compress(input_bytes)
        # with open('temp_image.jpg', 'wb') as temp_file:
        #     temp_file.write(processed_bytes)
        if processed_bytes:
            processed_bytes = self.update_metadata(processed_bytes)
            return processed_bytes
        raise Exception("Не удалось обработать изображение")
        # return None