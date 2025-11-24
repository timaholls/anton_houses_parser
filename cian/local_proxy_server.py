import socket
import threading
import base64
import sys
import select
import time  # Добавляем для возможности задержки
from urllib.parse import urlparse

# Добавим немедленный флаш для логов в Docker
import os
import io


# Принудительная отправка логов сразу на консоль
class UnbufferedWriter(io.TextIOWrapper):
    def write(self, s):
        super().write(s)
        self.flush()


# Применяем принудительный флаш выводов
sys.stdout = UnbufferedWriter(sys.stdout.buffer, line_buffering=True)
sys.stderr = UnbufferedWriter(sys.stderr.buffer, line_buffering=True)


class LocalProxyServer:
    def __init__(self, local_host, local_port, proxy_host, proxy_port, proxy_username, proxy_password):
        # Настройки локального прокси сервера
        self.local_host = local_host
        self.local_port = local_port

        # Настройки удаленного прокси с авторизацией
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password

        # Создаем сокет для прослушивания подключений
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # На Linux также полезно установить TCP_NODELAY для уменьшения задержек
        try:
            self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except:
            pass  # Если не поддерживается, продолжаем

        # Максимальный размер буфера для передачи данных
        self.buffer_size = 8192

        # Генерируем заголовок авторизации для прокси
        if self.proxy_username and self.proxy_password:
            auth = f"{self.proxy_username}:{self.proxy_password}"
            self.proxy_auth = base64.b64encode(auth.encode()).decode()
        else:
            self.proxy_auth = None

    def start(self):
        """Запуск локального прокси сервера"""
        try:
            # Привязываем серверный сокет к указанным хосту и порту
            self.server_socket.bind((self.local_host, self.local_port))
            self.server_socket.listen(100)

            print(f"[+] Локальный прокси сервер запущен на {self.local_host}:{self.local_port}")
            print(f"[+] Перенаправление трафика через {self.proxy_host}:{self.proxy_port}")
            print(f"[+] Настройте ваш браузер на использование прокси: {self.local_host}:{self.local_port}")
            sys.stdout.flush()  # Принудительный флаш

            # Принимаем входящие соединения в бесконечном цикле
            while True:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    # Устанавливаем таймауты для клиентского сокета
                    client_socket.settimeout(300)  # 5 минут таймаут для неактивных соединений
                    print(f"[+] Получено соединение от {client_address[0]}:{client_address[1]}")
                    sys.stdout.flush()  # Принудительный флаш

                    # Создаем отдельный поток для обработки соединения
                    client_thread = threading.Thread(
                        target=self.handle_client_connection,
                        args=(client_socket, client_address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                except OSError as e:
                    # Ошибка при accept - возможно сервер закрыт
                    if self.server_socket.fileno() == -1:
                        break
                    print(f"[!] Ошибка при принятии соединения: {e}")
                    sys.stdout.flush()

        except KeyboardInterrupt:
            print("\n[!] Прокси сервер остановлен")
        except Exception as e:
            print(f"[!] Ошибка при запуске сервера: {e}")
            sys.stdout.flush()  # Принудительный флаш
        finally:
            self.server_socket.close()

    def handle_client_connection(self, client_socket, client_address):
        """Обработка соединения от клиента (браузера)"""
        try:
            # Получаем запрос от клиента
            initial_data = self.receive_data(client_socket)
            if not initial_data:
                return

            # Если это HTTP запрос
            if initial_data.startswith(b'GET') or initial_data.startswith(b'POST') or \
                    initial_data.startswith(b'PUT') or initial_data.startswith(b'DELETE') or \
                    initial_data.startswith(b'HEAD'):
                self.handle_http_request(client_socket, initial_data)
            # Если это CONNECT запрос (для HTTPS)
            elif initial_data.startswith(b'CONNECT'):
                self.handle_https_request(client_socket, initial_data)
            else:
                print(f"[!] Неизвестный тип запроса")
                client_socket.close()

        except Exception as e:
            print(f"[!] Ошибка при обработке соединения: {e}")
        finally:
            if client_socket:
                client_socket.close()

    def handle_http_request(self, client_socket, request):
        """Обработка HTTP запросов"""
        proxy_socket = None
        try:
            # Создаем соединение с удаленным прокси с таймаутом
            proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            proxy_socket.settimeout(30)  # Таймаут подключения
            
            try:
                proxy_socket.connect((self.proxy_host, self.proxy_port))
            except (socket.timeout, ConnectionRefusedError, OSError) as conn_error:
                print(f"[!] Ошибка подключения к внешнему прокси {self.proxy_host}:{self.proxy_port}: {conn_error}")
                # Отправляем ошибку клиенту
                try:
                    error_msg = b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
                    client_socket.sendall(error_msg)
                except:
                    pass
                return

            # Модифицируем запрос для прокси с авторизацией
            modified_request = self.modify_request(request)
            
            try:
                proxy_socket.sendall(modified_request)
            except (BrokenPipeError, OSError) as send_error:
                print(f"[!] Ошибка отправки HTTP запроса: {send_error}")
                return

            # Обмен данными между клиентом и прокси
            self.exchange_data(client_socket, proxy_socket)

        except Exception as e:
            print(f"[!] Ошибка при обработке HTTP запроса: {e}")
        finally:
            if proxy_socket:
                try:
                    proxy_socket.close()
                except:
                    pass

    def handle_https_request(self, client_socket, request):
        """Обработка HTTPS запросов (CONNECT метод)"""
        proxy_socket = None
        try:
            # Парсим строку подключения (пример: CONNECT example.com:443 HTTP/1.1)
            first_line = request.split(b'\r\n')[0].decode('utf-8', 'ignore')
            url = first_line.split(' ')[1]
            host, port = url.split(':')
            port = int(port)

            # Создаем соединение с удаленным прокси с таймаутом
            proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            proxy_socket.settimeout(30)  # Таймаут подключения
            
            try:
                proxy_socket.connect((self.proxy_host, self.proxy_port))
            except (socket.timeout, ConnectionRefusedError, OSError) as conn_error:
                # Если не удалось подключиться к внешнему прокси
                error_msg = b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
                try:
                    client_socket.sendall(error_msg)
                except:
                    pass
                print(f"[!] Ошибка подключения к внешнему прокси {self.proxy_host}:{self.proxy_port}: {conn_error}")
                return

            # Отправляем CONNECT запрос на прокси с авторизацией
            connect_request = f"CONNECT {host}:{port} HTTP/1.1\r\n"
            if self.proxy_auth:
                connect_request += f"Proxy-Authorization: Basic {self.proxy_auth}\r\n"
            connect_request += f"Host: {host}:{port}\r\n"
            connect_request += "Connection: keep-alive\r\n\r\n"

            try:
                proxy_socket.sendall(connect_request.encode())
            except (BrokenPipeError, OSError) as send_error:
                print(f"[!] Ошибка отправки CONNECT запроса: {send_error}")
                return

            # Ждем ответа от прокси
            response = self.receive_data(proxy_socket, end_marker=b'\r\n\r\n')

            # Проверяем, успешно ли установлено соединение
            if b"200 Connection established" in response or b"200" in response:
                # Отправляем клиенту сообщение об успешном подключении
                try:
                    client_socket.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
                except (BrokenPipeError, OSError) as send_error:
                    print(f"[!] Клиент закрыл соединение: {send_error}")
                    return

                # Обмен данными между клиентом и прокси
                self.exchange_data(client_socket, proxy_socket)
            else:
                # Отправляем клиенту ошибку, если прокси отказал
                error_msg = b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
                try:
                    client_socket.sendall(error_msg)
                except:
                    pass
                print(f"[!] Прокси отказался установить соединение: {response.decode('utf-8', 'ignore')[:200]}")

        except Exception as e:
            print(f"[!] Ошибка при обработке HTTPS запроса: {e}")
            try:
                # Проверяем, что сокет еще открыт перед отправкой
                if client_socket and not client_socket._closed:
                    client_socket.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            except:
                pass
        finally:
            # Закрываем соединение с прокси, если оно было создано
            if proxy_socket:
                try:
                    proxy_socket.close()
                except:
                    pass

    def exchange_data(self, client_socket, proxy_socket):
        """Двунаправленный обмен данными между клиентом и прокси"""
        try:
            client_socket.setblocking(0)
            proxy_socket.setblocking(0)
        except:
            # Если не удалось установить неблокирующий режим, продолжаем
            pass

        while True:
            try:
                # Определяем, какие сокеты готовы для чтения
                inputs = [client_socket, proxy_socket]
                readable, _, exceptional = select.select(inputs, [], inputs, 10)

                if exceptional:
                    break

                if not readable:
                    # Таймаут - проверяем, что соединения еще живы
                    continue

                for sock in readable:
                    try:
                        data = sock.recv(self.buffer_size)
                        if not data:
                            return

                        # Определяем, куда отправлять данные
                        if sock is client_socket:
                            try:
                                proxy_socket.sendall(data)
                            except (BrokenPipeError, OSError, ConnectionResetError):
                                return
                        else:
                            try:
                                client_socket.sendall(data)
                            except (BrokenPipeError, OSError, ConnectionResetError):
                                return

                    except (socket.error, OSError, ConnectionResetError) as e:
                        # Нормальное закрытие соединения
                        return
                    except Exception as e:
                        print(f"[!] Ошибка при обмене данными: {e}")
                        return
            except (ValueError, OSError) as e:
                # Сокеты закрыты или в неверном состоянии
                return
            except Exception as e:
                print(f"[!] Неожиданная ошибка в exchange_data: {e}")
                return

    def modify_request(self, request):
        """Модифицирует HTTP запрос для добавления авторизации прокси"""
        if not self.proxy_auth:
            return request

        request_lines = request.split(b'\r\n')
        modified_lines = []
        auth_added = False

        for line in request_lines:
            if not auth_added and not line.startswith(b'Proxy-Authorization:'):
                modified_lines.append(line)
                # Добавляем заголовок авторизации после первой строки
                if len(modified_lines) == 1:
                    auth_header = f"Proxy-Authorization: Basic {self.proxy_auth}".encode()
                    modified_lines.append(auth_header)
                    auth_added = True
            elif not line.startswith(b'Proxy-Authorization:'):
                modified_lines.append(line)

        return b'\r\n'.join(modified_lines)

    def receive_data(self, sock, end_marker=None, timeout=5):
        """Получение данных из сокета с опциональным маркером окончания"""
        sock.settimeout(timeout)
        chunks = []

        try:
            while True:
                chunk = sock.recv(self.buffer_size)
                if not chunk:
                    break

                chunks.append(chunk)

                if end_marker and chunk.endswith(end_marker):
                    break

                if end_marker is None:
                    # Если маркер не указан, получаем только одну порцию данных
                    break
        except socket.timeout:
            pass

        sock.settimeout(None)
        return b''.join(chunks)


def main():
    # Настройки по умолчанию
    local_host = '127.0.0.1'
    local_port = 8080
    proxy_host = '188.134.88.13'
    proxy_port = 40471
    proxy_username = '1151561916'
    proxy_password = 'a651191b15'

    try:
        # Выводим информацию о запуске для диагностики
        print(f"[*] Запуск локального прокси-сервера...")
        print(f"[*] Локальный хост: {local_host}, порт: {local_port}")
        print(f"[*] Настройки внешнего прокси: {proxy_host}:{proxy_port}")
        sys.stdout.flush()  # Принудительный флаш

        # Парсинг аргументов командной строки
        if len(sys.argv) >= 3:
            local_host = sys.argv[1]
            local_port = int(sys.argv[2])

        proxy_server = LocalProxyServer(
            local_host, local_port,
            proxy_host, proxy_port,
            proxy_username, proxy_password
        )

        # Пробуем подключиться к удаленному прокси перед запуском
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(10)
            print(f"[*] Проверка подключения к внешнему прокси {proxy_host}:{proxy_port}...")
            sys.stdout.flush()  # Принудительный флаш
            test_socket.connect((proxy_host, proxy_port))
            print(f"[+] Подключение к внешнему прокси успешно!")
            sys.stdout.flush()  # Принудительный флаш
            test_socket.close()
        except Exception as e:
            print(f"[!] Не удалось подключиться к внешнему прокси: {e}")
            print(f"[!] Локальный прокси будет работать, но запросы к внешнему прокси могут не проходить")
            sys.stdout.flush()  # Принудительный флаш

        # Запускаем локальный прокси-сервер
        proxy_server.start()

    except Exception as e:
        print(f"[!] Критическая ошибка: {e}")
        sys.stdout.flush()  # Принудительный флаш


if __name__ == "__main__":
    main()
