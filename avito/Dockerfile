FROM consol/debian-xfce-vnc:latest

ENV TZ="Asia/Yekaterinburg"
ENV LANG=en_US.UTF-8

USER 0

# Удаляем нерабочий репозиторий Google Chrome (если есть)
RUN rm -f /etc/apt/sources.list.d/google-chrome.list || true

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv wget gnupg && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list' && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

ENV EXECUTABLE_PATH=/usr/bin/google-chrome-stable

WORKDIR /app
COPY req.txt /app/req.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /app/req.txt

COPY avito /app/avito

# Делаем скрипт исполняемым и добавляем парсер в Supervisor
RUN chmod +x /app/avito/run_parser.sh && \
    echo "[program:parser]" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "command=/app/avito/run_parser.sh" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "directory=/app/avito" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "autorestart=false" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile=/dev/stdout" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stdout_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile=/dev/stderr" >> /etc/supervisor/conf.d/supervisord.conf && \
    echo "stderr_logfile_maxbytes=0" >> /etc/supervisor/conf.d/supervisord.conf

# Устанавливаем права на скрипт
RUN chmod +x /app/avito/run_parser.sh

WORKDIR /app/avito
