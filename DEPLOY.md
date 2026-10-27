# Установка на Linux-сервер

## 1. Распакуйте проект

Загрузите `telethon_bot_server.zip` на сервер и выполните:

```bash
mkdir -p telethon_bot_starter
unzip telethon_bot_server.zip -d telethon_bot_starter
cd telethon_bot_starter
```

Для работы бота серверу нужен доступ к Telegram и Python 3.9 или новее.
На Debian/Ubuntu при необходимости сначала установите пакет `python3-venv`.

## 2. Установите зависимости

```bash
chmod +x install.sh run.sh
./install.sh
```

Скрипт создаст `.venv`, установит Telethon 1.45.0, cryptg и qrcode, а также
создаст `config.json` из шаблона, если файла ещё нет.

## 3. Заполните настройки

Откройте `config.json` и укажите:

- `api_id` и `api_hash` из [my.telegram.org](https://my.telegram.org/apps);
- `bot_token` от `@BotFather`;
- `admin_id` — ваш числовой Telegram ID.

Прокси можно оставить выключенным. Если он нужен, укажите тип `socks5`,
`socks4` или `http`, адрес и порт.

## 4. Запустите бота

```bash
./run.sh
```

Оставьте процесс работающим в `tmux`/`screen` или настройте запуск через
systemd, если бот должен переживать выход из SSH-сессии и перезагрузку сервера.

Первый вход Telegram-аккаунта выполняется через бота. Сессии и экспорты
сохраняются в `accounts/`, настройки — в `config.json`. Не публикуйте и не
передавайте эти файлы посторонним.

## Обновление

Заменяйте файлы кода новой версией, сохраняя `config.json`, `.venv/` и
`accounts/`. После обновления зависимостей запустите `./install.sh` повторно.
