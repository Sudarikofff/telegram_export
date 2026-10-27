# Telegram Export

**A Telegram bot for exporting conversations, messages, contacts, and media into organized HTML reports and ZIP archives.**

Telegram Export is a Python-based tool that combines account management, configurable chat exports, media downloads, and archive management in a single Telegram bot interface.

Designed for self-hosted deployment, it gives you control over which chats to export, what data to include, and how the resulting files are organized.

## ✨ Features

### 📱 Telegram Account Management
- Connect Telegram accounts using a login code or QR code.
- Support for two-factor authentication (2FA).
- Manage multiple account sessions from one bot interface.
- Store account sessions locally on your server.

### 💬 Flexible Chat Export
- Export individual conversations or multiple chats at once.
- Filter chats by type: private conversations, groups, and channels.
- Search chats by name and select the conversations you need.
- Specify a date range for the export.
- Choose between exporting your own messages or all available messages in supported groups and channels.

### 📎 Media Downloads
- Download supported Telegram media alongside messages.
- Configure file size limits.
- Use parallel downloads for large files, with configurable worker counts.
- Fall back to Telethon's standard media downloader when the accelerated download path is unavailable.
- Handle Telegram rate limits with bounded retries and wait policies.

### 📦 Export Formats and Archives
- Generate browsable HTML reports.
- Preserve downloaded media alongside exported messages.
- Create individual chat archives and a combined ZIP archive.
- Choose Russian or English for generated HTML reports.

### ⚙️ Server-Friendly Configuration
- Run on Linux servers.
- Configure Telegram API credentials, bot access, and optional proxy settings.
- Use a Python virtual environment for dependency isolation.
- Install and start the bot using the included shell scripts.
- Monitor export progress, active tasks, account status, and available disk space through the bot interface.

## 🛠️ Technology Stack

- **Python 3.9+**
- **Telethon 1.45.0**
- **Asyncio** for asynchronous operations
- **cryptg** for accelerated cryptographic operations
- **qrcode** for QR-code generation

## 🚀 Installation

### Requirements

- Linux server or another compatible Python environment.
- Python 3.9 or newer.
- Telegram API credentials.
- A Telegram bot token.
- Network access to Telegram.
- Enough disk space for exported messages, media, and archives.

### 1. Clone the repository

```bash
git clone https://github.com/Sudarikofff/telegram_export.git
cd telegram_export
```

### 2. Install dependencies

```bash
chmod +x install.sh run.sh
./install.sh
```

The installation script creates a virtual environment, installs the required dependencies, and prepares `config.json` from the example configuration if it does not already exist.

### 3. Configure Telegram credentials

Obtain your Telegram API credentials from [my.telegram.org](https://my.telegram.org/apps) and create a bot using [@BotFather](https://t.me/BotFather).

Edit `config.json`:

```json
{
  "api_id": "YOUR_API_ID",
  "api_hash": "YOUR_API_HASH",
  "bot_token": "YOUR_BOT_TOKEN",
  "admin_id": 123456789,
  "proxy": {
    "enabled": false,
    "type": "socks5",
    "host": "127.0.0.1",
    "port": 1080,
    "username": "",
    "password": ""
  }
}
```

Replace the placeholder values with your actual credentials and your numeric Telegram user ID.

Keep `api_hash`, `bot_token`, proxy credentials, and account session files private.

### 4. Start the bot

```bash
./run.sh
```

Open your bot in Telegram and send `/start` from the configured administrator account.

Follow the interactive menus to connect an account and configure an export.

For persistent operation on a server, use `systemd`, `tmux`, or another suitable process manager.

## 📂 Project Structure

```text
telegram_export/
├── bot.py
├── main.py
├── telethon_client.py
├── telethon_parallel_downloader.py
├── FastTelethon.py
├── session_storage.py
├── config.example.json
├── install.sh
├── run.sh
├── requirements.txt
├── requirements-dev.txt
├── DEPLOY.md
└── tests/
    └── test_safety.py
```

| File | Purpose |
|---|---|
| `bot.py` | Telegram bot interface, account management, and export workflow |
| `main.py` | Export engine, message processing, media downloads, and HTML generation |
| `telethon_client.py` | Telegram client initialization |
| `telethon_parallel_downloader.py` | Parallel media download implementation |
| `FastTelethon.py` | Additional accelerated download implementation |
| `session_storage.py` | Account session paths and temporary QR-session cleanup |
| `config.example.json` | Example configuration |
| `install.sh` | Environment and dependency setup |
| `run.sh` | Bot startup |
| `DEPLOY.md` | Linux server deployment instructions |
| `tests/` | Safety and behavior tests |

## 🔐 Security

Telegram Export works with your Telegram account sessions and API credentials. Treat these files as sensitive authentication material.

- Restrict access to `config.json` and the `accounts/` directory.
- Never commit real credentials, active session files, or private exports to a public repository.
- Keep the bot's administrator ID restricted to trusted users.
- Use a secure server and protect SSH access.
- Review Telegram's usage policies and the permissions applicable to the chats and data you export.

The project is intended for exporting data from accounts you own or are authorized to access.

## ⚠️ Important Notes

- Export duration depends on chat size, media volume, network speed, server resources, and Telegram rate limits.
- Accelerated media downloads rely on Telethon internals and are explicitly tied to the pinned Telethon version. Changing that dependency may affect the parallel downloader.
- Export availability depends on the data accessible through the authenticated Telegram account.
- Keep sufficient free disk space for downloaded media and generated archives.

## 📖 Documentation

- [Deployment Guide](DEPLOY.md)
- [Example Configuration](config.example.json)
- [Telegram API Documentation](https://core.telegram.org/api)
- [Telethon Documentation](https://docs.telethon.dev/)

## 📄 License

No license has been specified in this repository yet. Check the repository for licensing information before redistributing the project or incorporating it into another application.
