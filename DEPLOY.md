# Installation on a Linux Server

## 1. Extract the Project

Upload `telethon_bot_server.zip` to your server and run:

```bash
mkdir -p telethon_bot_starter
unzip telethon_bot_server.zip -d telethon_bot_starter
cd telethon_bot_starter
```

The bot requires network access to Telegram and Python 3.9 or newer. On Debian/Ubuntu, install the `python3-venv` package first if necessary.

## 2. Install Dependencies

```bash
chmod +x install.sh run.sh
./install.sh
```

The installation script will create a `.venv` virtual environment, install Telethon 1.45.0, `cryptg`, and `qrcode`, and create `config.json` from the example configuration if the file does not already exist.

## 3. Configure the Settings

Open `config.json` and provide the following values:

- `api_id` and `api_hash` from [my.telegram.org](https://my.telegram.org/apps).
- `bot_token` obtained from `@BotFather`.
- `admin_id` — your numeric Telegram user ID.

Proxy settings can remain disabled. If you need a proxy, specify its type (`socks5`, `socks4`, or `http`), host, and port.

## 4. Start the Bot

```bash
./run.sh
```

Keep the process running inside `tmux` or `screen`, or configure a `systemd` service if the bot needs to continue running after you disconnect from SSH or restart the server.

The initial Telegram account login is performed through the bot. Session files and exports are stored in `accounts/`, while configuration settings are stored in `config.json`. Do not publish or share these files with unauthorized parties.

## Updating

Replace the application code with the new version while preserving `config.json`, `.venv/`, and `accounts/`.

After updating the dependencies, run the installation script again:

```bash
./install.sh
```
