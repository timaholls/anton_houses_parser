# Базовый образ с VNC и XFCE
FROM consol/debian-xfce-vnc

# Установка часового пояса и локали
ENV TZ="Asia/Yekaterinburg"
ENV LANG="en_US.UTF-8"

USER 0

# Обновление системы и установка основных зависимостей
# Update system and install necessary packages
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    locales \
    ca-certificates \
    fonts-liberation \
    wget \
    net-tools \
    xdg-utils \
    python3-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    build-essential \
    python3-pip \
    python3-venv \
    librtmp-dev \
    fonts-noto \
    fonts-roboto \
    fonts-dejavu \
    cron \
    && sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
    && update-locale LANG=en_US.UTF-8 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Копирование зависимостей и проекта
COPY requirements.txt /app/requirements.txt

# Создание виртуального окружения
RUN python3 -m venv /app/venv &&  \
    /app/venv/bin/pip install --default-timeout=100 -r /app/requirements.txt --break-system-packages

# Копирование приложения
COPY . /app
WORKDIR /app

# Установка Playwright и его зависимостей (после копирования приложения)
RUN /app/venv/bin/pip install --default-timeout=300 playwright && \
    /app/venv/bin/playwright install && \
    /app/venv/bin/playwright install-deps

# Переменные окружения
ENV PYTHONPATH="/app"
ENV DISPLAY=:1
ENV PYTHONUNBUFFERED=1

# Настройка cron задания для запуска в 00:01 первого числа каждого месяца
RUN echo '1 0 1 * * cd /app && PYTHONPATH=/app /app/venv/bin/python -u /app/main.py > /app/logs/cron-main.log 2>&1' > /etc/cron.d/main-job \
    && chmod 0644 /etc/cron.d/main-job \
    && crontab /etc/cron.d/main-job

# Упрощённый стартовый скрипт
# Файл start.sh уже скопирован из проекта
RUN sed -i 's/\r$//' /app/start.sh && chmod 0755 /app/start.sh && chown root:root /app/start.sh

# Установка точки входа
CMD ["bash", "/app/start.sh"]

#docker build -t estateline:monthly .
#docker run -d --name estateline-monthly estateline:monthly
#docker logs - f estateline - monthly