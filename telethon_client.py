"""TelegramClient factory with locale metadata used by this Russian-language app."""


def create_telegram_client(session, api_id, api_hash, *, proxy=None):
    """Create a client with explicit app and operating-system language codes."""
    from telethon import TelegramClient

    return TelegramClient(
        session,
        api_id,
        api_hash,
        proxy=proxy,
        lang_code="ru",
        system_lang_code="ru-RU",
    )
