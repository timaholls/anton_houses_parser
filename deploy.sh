#!/bin/bash
# Укажите IP сервера и пользователя
SERVER_IP="176.98.177.188"
USER="root"
REMOTE_DIR="/root/anton_houses_parser"

# Проверка наличия .gitignore
if [ ! -f .gitignore ]; then
    echo "ОШИБКА: Файл .gitignore не найден!"
    exit 1
fi

# Создаем временный файл для исключений на основе .gitignore
TMP_EXCLUDE_FILE=$(mktemp)

# Копируем .gitignore
cat .gitignore > "$TMP_EXCLUDE_FILE"

# Добавляем дополнительные исключения
echo ".git" >> "$TMP_EXCLUDE_FILE"
echo "deploy.sh" >> "$TMP_EXCLUDE_FILE"
echo ".git/" >> "$TMP_EXCLUDE_FILE" 
echo "*.pyc" >> "$TMP_EXCLUDE_FILE"
echo "__pycache__/" >> "$TMP_EXCLUDE_FILE"
echo "__pycache__/" >> "$TMP_EXCLUDE_FILE"
echo ".venv/" >> "$TMP_EXCLUDE_FILE"
echo ".idea" >> "$TMP_EXCLUDE_FILE"
echo "venv/" >> "$TMP_EXCLUDE_FILE"

echo "Начинаем синхронизацию с сервером $SERVER_IP..."
echo "Для успешной синхронизации необходим доступ по SSH без пароля."
echo "Если возникла ошибка аутентификации, выполните:"
echo "ssh-copy-id $USER@$SERVER_IP"

# Запрос пароля (опционально). Если просто нажать Enter, пароль спросит rsync/ssh
printf "Введите пароль для %s@%s (или Enter, чтобы ввести вручную при запросе): " "$USER" "$SERVER_IP"
read -r -s PASSWORD
echo

# Выполняем синхронизацию с rsync
if command -v sshpass >/dev/null 2>&1 && [ -n "$PASSWORD" ]; then
  sshpass -p "$PASSWORD" rsync -avz --delete --exclude-from="$TMP_EXCLUDE_FILE" ./ "$USER@$SERVER_IP:$REMOTE_DIR/"
else
  rsync -avz --delete --exclude-from="$TMP_EXCLUDE_FILE" ./ "$USER@$SERVER_IP:$REMOTE_DIR/"
fi

# Удаляем временный файл
rm "$TMP_EXCLUDE_FILE"

echo "Синхронизация выполнена успешно!" 