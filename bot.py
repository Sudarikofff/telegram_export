#!/usr/bin/env python3
import asyncio
import contextlib
import io
import json
import logging
import re
import shutil
import struct
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import zipfile
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from telethon import Button, TelegramClient, events
from telethon.errors import (
    AuthRestartError,
    FloodWaitError,
    MessageNotModifiedError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)

import main as exporter  # Основной модуль экспортёра (main.py)
from session_storage import (
    account_directory,
    account_session_base_path,
    cleanup_stale_qr_dirs,
)
from telethon_client import create_telegram_client

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
ACCOUNTS_DIR = ROOT / "accounts"
BOT_SESSION = ROOT / "bot_session"

CHAT_PAGE_SIZE = 8
ALLOWED_WORKERS = (1, 2, 4, 8)
WORKERS_DEFAULT = exporter.clamp_workers(exporter.DEFAULT_WORKERS)
TELEGRAM_UPLOAD_LIMIT = 1950 * 1024 * 1024  # ~1.95 ГБ безопасный лимит отправки

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("export-bot")


# =====================================================================
# ГЕНЕРАТОР QR-КОДОВ В PNG (БЕЗ ТЯЖЁЛЫХ ЗАВИСИМОСТЕЙ PILLOW)
# =====================================================================

def _ensure_qrcode_lib() -> bool:
    try:
        import qrcode
        return True
    except ImportError:
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--user", "qrcode"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            import qrcode  # noqa: F401
            return True
        except (OSError, subprocess.CalledProcessError, ImportError):
            return False


def _matrix_to_png_bytes(matrix: list[list[bool]], scale: int = 10, border: int = 4) -> bytes:
    """Рендерит булеву матрицу QR-кода в валидный черно-белый PNG (чистый Python + zlib)."""
    size = len(matrix)
    dim = (size + border * 2) * scale

    rows = []
    white_row = b"\x00" + (b"\xff" * dim)
    for _ in range(border * scale):
        rows.append(white_row)

    for r in range(size):
        line = bytearray([0])  # фильтр байта PNG = 0 (None)
        line.extend(b"\xff" * (border * scale))
        for c in range(size):
            pixel = b"\x00" if matrix[r][c] else b"\xff"
            line.extend(pixel * scale)
        line.extend(b"\xff" * (border * scale))
        row_bytes = bytes(line)
        for _ in range(scale):
            rows.append(row_bytes)

    for _ in range(border * scale):
        rows.append(white_row)

    raw_data = b"".join(rows)

    def make_chunk(chunk_type: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", dim, dim, 8, 0, 0, 0, 0)  # 8-bit grayscale
    idat = zlib.compress(raw_data, level=6)
    return (
        b"\x89PNG\r\n\x1a\n"
        + make_chunk(b"IHDR", ihdr)
        + make_chunk(b"IDAT", idat)
        + make_chunk(b"IEND", b"")
    )


def generate_qr_png(url: str) -> bytes:
    """Генерирует PNG-изображение QR-кода локально (или через API при отсутствии библиотеки)."""
    if _ensure_qrcode_lib():
        import qrcode
        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(url)
        qr.make(fit=True)
        matrix = qr.get_matrix()
        return _matrix_to_png_bytes(matrix, scale=10, border=0)

    # Безопасный фоллбэк, если pip заблокирован в окружении
    encoded = urllib.parse.quote(url, safe="")
    req_url = f"https://api.qrserver.com/v1/create-qr-code/?size=360x360&margin=16&data={encoded}"
    with urllib.request.urlopen(req_url, timeout=10) as resp:
        return resp.read()


# =====================================================================
# КОНФИГУРАЦИЯ И ИНИЦИАЛИЗАЦИЯ
# =====================================================================

def load_bot_config() -> dict:
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Не найден {CONFIG_PATH}. Создай его с полями api_id, api_hash, bot_token, admin_id.")
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    required = ("api_id", "api_hash", "bot_token", "admin_id")
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise RuntimeError("Не заполнены обязательные поля в config.json: " + ", ".join(missing))

    config["api_id"] = int(config["api_id"])
    raw_admins = config["admin_id"]
    if isinstance(raw_admins, (list, tuple)):
        config["admins"] = {int(x) for x in raw_admins}
    else:
        config["admins"] = {int(raw_admins)}
    return config


CONFIG = load_bot_config()
PROXY = exporter.resolve_proxy(CONFIG)
BOT = create_telegram_client(
    str(BOT_SESSION), CONFIG["api_id"], CONFIG["api_hash"], proxy=PROXY
)


# =====================================================================
# МОДЕЛИ СОСТОЯНИЯ
# =====================================================================

@dataclass
class AuthState:
    step: str = "phone"  # phone | code | password | qr
    phone: str = ""
    client: Optional[TelegramClient] = None
    phone_code_hash: str = ""
    temp_dir: Optional[Path] = None
    qr_task: Optional[asyncio.Task] = None
    qr_msg_id: Optional[int] = None


@dataclass
class ExportWizard:
    phone: str
    client: TelegramClient
    dialogs: list = field(default_factory=list)
    filtered_indices: list[int] = field(default_factory=list)
    search_query: str = ""
    type_filter: str = "all"  # "all" | "user" | "group" | "channel"
    selected_all: bool = False
    selected_indices: set[int] = field(default_factory=set)
    chat_page: int = 0
    start_date: datetime = field(default_factory=lambda: datetime(2000, 1, 1))
    end_date: Optional[datetime] = None
    media: bool = True
    max_size_mb: int = 0  # 0 = без ограничений
    mode: str = "mine"    # "mine" | "all"
    lang: str = "ru"      # "ru" | "en"
    make_zip: bool = True
    workers: int = WORKERS_DEFAULT
    awaiting: Optional[str] = None  # start_date | end_date | max_size | search


@dataclass
class ActiveJob:
    phone: str
    admin_id: int
    target_title: str
    task: Optional[asyncio.Task] = None
    current_idx: int = 0
    total_chats: int = 1
    current_chat: str = "Подготовка профиля и контактов..."
    stage: str = "Инициализация..."
    started_at: float = field(default_factory=time.time)
    status_msg_id: Optional[int] = None


AUTH_STATES: dict[int, AuthState] = {}
WIZARDS: dict[int, ExportWizard] = {}
ACCOUNT_CLIENTS: dict[str, TelegramClient] = {}
ACTIVE_JOBS: dict[str, ActiveJob] = {}


# =====================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И UI-ХЕЛПЕРЫ
# =====================================================================

def is_admin(event) -> bool:
    return getattr(event, "sender_id", None) in CONFIG["admins"]


def account_dir(phone: str) -> Path:
    return account_directory(ACCOUNTS_DIR, exporter.normalize_phone(phone))


def session_path(phone: str) -> str:
    return str(account_session_base_path(ACCOUNTS_DIR, exporter.normalize_phone(phone)))


def account_names() -> list[str]:
    if not ACCOUNTS_DIR.exists():
        return []
    return sorted(
        p.name for p in ACCOUNTS_DIR.iterdir()
        if p.is_dir() and not p.name.startswith("_") and ((p / "session.session").exists() or (p / "export").exists())
    )


def free_disk_gb() -> float:
    try:
        usage = shutil.disk_usage(ROOT)
        return usage.free / (1024 ** 3)
    except OSError:
        return 0.0


def render_progress_bar(current: int, total: int, width: int = 14) -> str:
    if total <= 0:
        return "▱" * width
    ratio = min(1.0, max(0.0, current / total))
    filled = round(ratio * width)
    return "▰" * filled + "▱" * (width - filled)


def format_duration(seconds: float) -> str:
    secs = int(max(0, seconds))
    mins, secs = divmod(secs, 60)
    hrs, mins = divmod(mins, 60)
    if hrs:
        return f"{hrs}ч {mins:02d}м {secs:02d}с"
    if mins:
        return f"{mins}м {secs:02d}с"
    return f"{secs}с"


def dialog_category(dialog) -> str:
    if getattr(dialog, "is_user", False):
        return "user"
    if getattr(dialog, "is_channel", False) and not getattr(dialog, "is_group", False):
        return "channel"
    return "group"


def dialog_icon(dialog) -> str:
    cat = dialog_category(dialog)
    if cat == "user":
        return "👤"
    if cat == "channel":
        return "📢"
    return "👥"


def apply_dialog_filters(w: ExportWizard):
    query = w.search_query.lower().strip()
    result = []
    for idx, dlg in enumerate(w.dialogs):
        if w.type_filter != "all" and dialog_category(dlg) != w.type_filter:
            continue
        if query and query not in exporter.dialog_title(dlg).lower():
            continue
        result.append(idx)
    w.filtered_indices = result


async def safe_disconnect_auth(user_id: int):
    state = AUTH_STATES.pop(user_id, None)
    if not state:
        return
    if state.qr_task is asyncio.current_task():
        state.qr_task = None
    elif state.qr_task and not state.qr_task.done():
        state.qr_task.cancel()
        try:
            await state.qr_task
        except asyncio.CancelledError:
            pass
    if state.qr_msg_id:
        try:
            await BOT.delete_messages(user_id, [state.qr_msg_id])
        except (RPCError, OSError, ConnectionError, TimeoutError) as exc:
            log.debug("Не удалось удалить QR-сообщение (%s)", type(exc).__name__)
    if state.client and state.client.is_connected():
        try:
            await state.client.disconnect()
        except (RPCError, OSError, ConnectionError, TimeoutError) as exc:
            log.debug("Не удалось закрыть клиент авторизации (%s)", type(exc).__name__)
    if state.temp_dir and state.temp_dir.exists():
        shutil.rmtree(state.temp_dir, ignore_errors=True)


async def finalize_authorized_client(user_id: int, state: AuthState) -> tuple[str, str]:
    """
    Завершает авторизацию (после кода, QR или 2FA):
    определяет номер телефона через get_me(), при необходимости переносит временную
    QR-сессию в постоянную папку accounts/+7... и регистрирует клиент.
    """
    me = await state.client.get_me()
    raw_phone = getattr(me, "phone", None) or state.phone
    if raw_phone:
        phone = exporter.normalize_phone(raw_phone)
    else:
        phone = f"+{getattr(me, 'id', int(time.time()))}"

    full_name = f"{getattr(me, 'first_name', '') or ''} {getattr(me, 'last_name', '') or ''}".strip() or "Без имени"
    state.client.session.save()

    # Если это был временный QR-клиент — переносим файл сессии в целевую папку номера
    if state.temp_dir and state.temp_dir.exists():
        await state.client.disconnect()
        dest_folder = account_dir(phone)
        dest_folder.mkdir(parents=True, exist_ok=True)
        dest_sess = dest_folder / "session.session"

        old_client = ACCOUNT_CLIENTS.pop(phone, None)
        if old_client and old_client.is_connected():
            await old_client.disconnect()

        src_sess = state.temp_dir / "session.session"
        if src_sess.exists():
            shutil.move(str(src_sess), str(dest_sess))
        shutil.rmtree(state.temp_dir, ignore_errors=True)

        new_client = create_telegram_client(
            session_path(phone), CONFIG["api_id"], CONFIG["api_hash"], proxy=PROXY
        )
        new_client.flood_sleep_threshold = 0
        await new_client.connect()
        ACCOUNT_CLIENTS[phone] = new_client
    else:
        ACCOUNT_CLIENTS[phone] = state.client

    if state.qr_msg_id:
        try:
            await BOT.delete_messages(user_id, [state.qr_msg_id])
        except (RPCError, OSError, ConnectionError, TimeoutError) as exc:
            log.debug("Не удалось удалить QR-сообщение (%s)", type(exc).__name__)

    AUTH_STATES.pop(user_id, None)
    return phone, full_name


async def start_qr_login_flow(event):
    """Запускает авторизацию по QR-коду с автообновлением токена."""
    user_id = event.sender_id
    await safe_disconnect_auth(user_id)

    temp_dir = ACCOUNTS_DIR / f"_qr_temp_{user_id}_{int(time.time())}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_sess = str(temp_dir / "session")

    client = create_telegram_client(
        temp_sess, CONFIG["api_id"], CONFIG["api_hash"], proxy=PROXY
    )
    client.flood_sleep_threshold = 0

    state = AuthState(step="qr", client=client, temp_dir=temp_dir)
    AUTH_STATES[user_id] = state

    await safe_edit(event, "⏳ <b>Подключаюсь к Telegram и генерирую QR-код...</b>")

    async def qr_worker():
        try:
            await client.connect()
            qr_login = await client.qr_login()

            for attempt in range(1, 4):
                png_bytes = await asyncio.to_thread(generate_qr_png, qr_login.url)
                file_obj = io.BytesIO(png_bytes)
                file_obj.name = "telegram_qr.png"

                caption = (
                    f"📷 <b>Вход по QR-коду (попытка {attempt}/3)</b>\n"
                    f"────────────────────────\n"
                    f"1. Открой <b>Telegram</b> на телефоне\n"
                    f"2. Перейди в <b>Настройки → Устройства → Подключить устройство</b>\n"
                    f"3. Наведи камеру на этот QR-код\n\n"
                    f"<i>Вход по QR не запрашивает SMS-код, но Telegram всё равно может ограничить авторизацию.</i>"
                )
                buttons = [[Button.inline("❌ Отменить QR-вход", b"accounts")]]

                if state.qr_msg_id:
                    try:
                        await BOT.delete_messages(user_id, [state.qr_msg_id])
                    except (RPCError, OSError, ConnectionError, TimeoutError) as exc:
                        log.debug("Не удалось удалить QR-сообщение (%s)", type(exc).__name__)

                sent_msg = await BOT.send_file(
                    user_id,
                    file_obj,
                    caption=caption,
                    parse_mode="html",
                    buttons=buttons,
                )
                state.qr_msg_id = getattr(sent_msg, "id", None)

                try:
                    await qr_login.wait(timeout=25)
                    # Успешно отсканирован без 2FA!
                    phone, full_name = await finalize_authorized_client(user_id, state)
                    await BOT.send_message(
                        user_id,
                        f"✅ <b>QR-вход выполнен успешно!</b>\n\n"
                        f"📱 Аккаунт: <code>{phone}</code> ({exporter.escape_html(full_name)}) подключён и готов к экспорту.",
                        parse_mode="html",
                        buttons=home_buttons(),
                    )
                    return
                except asyncio.TimeoutError:
                    if attempt < 3:
                        await qr_login.recreate()
                    else:
                        await safe_disconnect_auth(user_id)
                        await BOT.send_message(
                            user_id,
                            "⌛ <b>Время ожидания QR-кода истекло.</b>\nПопробуй снова, когда будешь готов отсканировать.",
                            parse_mode="html",
                            buttons=home_buttons(),
                        )
                        return
                except SessionPasswordNeededError:
                    # На аккаунте включен 2FA — переключаем состояние на ввод пароля
                    state.step = "password"
                    if state.qr_msg_id:
                        try:
                            await BOT.delete_messages(user_id, [state.qr_msg_id])
                        except (RPCError, OSError, ConnectionError, TimeoutError) as exc:
                            log.debug("Не удалось удалить QR-сообщение (%s)", type(exc).__name__)
                        state.qr_msg_id = None
                    await BOT.send_message(
                        user_id,
                        "🔐 <b>QR-код отсканирован! Требуется пароль 2FA</b>\n\n"
                        "Отправь облачный пароль двухфакторной аутентификации ответным сообщением.\n"
                        "<i>Сообщение с паролем будет мгновенно удалено из чата.</i>\n\n"
                        "Для отмены: /cancel",
                        parse_mode="html",
                    )
                    return
        except asyncio.CancelledError:
            raise
        except FloodWaitError as exc:
            wait = max(1, int(getattr(exc, "seconds", 1) or 1))
            log.warning("QR-вход остановлен из-за FloodWait (%s сек.)", wait)
            await safe_disconnect_auth(user_id)
            await BOT.send_message(
                user_id,
                f"⏳ Telegram ограничил вход по QR. Подожди {wait} сек. и запусти вход заново.",
                buttons=home_buttons(),
            )
        except Exception as exc:  # noqa: BLE001 - top-level QR worker reports unexpected failures
            log.error("Ошибка при QR-авторизации (%s)", type(exc).__name__)
            await safe_disconnect_auth(user_id)
            await BOT.send_message(
                user_id,
                f"❌ <b>Ошибка QR-авторизации:</b>\n<code>{type(exc).__name__}: {exc}</code>",
                parse_mode="html",
                buttons=home_buttons(),
            )

    state.qr_task = asyncio.create_task(qr_worker())


async def safe_edit(event, text: str, buttons=None, parse_mode: str = "html"):
    try:
        return await event.edit(text, buttons=buttons, parse_mode=parse_mode, link_preview=False)
    except MessageNotModifiedError:
        pass
    except RPCError:
        return await event.respond(text, buttons=buttons, parse_mode=parse_mode, link_preview=False)


async def get_account_client(phone: str) -> TelegramClient:
    phone = exporter.normalize_phone(phone)
    client = ACCOUNT_CLIENTS.get(phone)
    if client and client.is_connected() and await client.is_user_authorized():
        return client

    sess_file = Path(session_path(phone) + ".session")
    if not sess_file.exists():
        raise RuntimeError("Файл сессии не найден. Авторизуй аккаунт заново.")

    client = create_telegram_client(
        session_path(phone), CONFIG["api_id"], CONFIG["api_hash"], proxy=PROXY
    )
    client.flood_sleep_threshold = 0
    try:
        await client.connect()
        authorized = await client.is_user_authorized()
    except FloodWaitError as exc:
        if client.is_connected():
            await client.disconnect()
        raise exporter.FloodWaitLimitError(
            "проверка сессии аккаунта",
            max(1, int(getattr(exc, "seconds", 1) or 1)),
            1,
        ) from exc
    if not authorized:
        await client.disconnect()
        raise RuntimeError("Сессия истекла или разлогинена. Переподключи аккаунт.")
    ACCOUNT_CLIENTS[phone] = client
    return client


def create_zip_sync(source_dir: Path, dest_zip: Path, exclude_zips: bool = True) -> int:
    """
    Синхронная упаковка архива (вызывается через asyncio.to_thread).
    Исключает вложенные .zip чатов из корневой папки, чтобы избежать двойной архивации.
    """
    count = 0
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file():
                continue
            if exclude_zips and path.parent == source_dir and path.suffix.lower() == ".zip":
                continue
            zf.write(path, arcname=path.relative_to(source_dir))
            count += 1
    return count


# =====================================================================
# ЭКРАНЫ И КЛАВИАТУРЫ МЕНЮ
# =====================================================================

def home_buttons() -> list:
    active_cnt = len(ACTIVE_JOBS)
    status_label = f"📊 Задачи ({active_cnt} актив.)" if active_cnt else "📊 Активные задачи"
    return [
        [Button.inline("🚀 Новый экспорт", b"export"), Button.inline("📱 Аккаунты", b"accounts")],
        [Button.inline("🗂 Файлы и архивы", b"files"), Button.inline(status_label, b"status")],
    ]

async def show_home(event, edit: bool = False):
    acc_cnt = len(account_names())
    active_cnt = len(ACTIVE_JOBS)
    proxy_str = f"{PROXY['proxy_type']}://{PROXY['addr']}:{PROXY['port']}" if PROXY else "Прямое соединение"
    free_gb = free_disk_gb()

    text = (
        f"⚡ <b>Telegram Exporter v{exporter.__version__}</b>\n"
        f"────────────────────────\n"
        f"📱 <b>Аккаунтов в базе:</b> <code>{acc_cnt}</code>\n"
        f"🔄 <b>Активных выгрузок:</b> <code>{active_cnt}</code>\n"
        f"💾 <b>Свободно на диске:</b> <code>{free_gb:.1f} ГБ</code>\n"
        f"🌐 <b>Сеть:</b> <code>{exporter.escape_html(proxy_str)}</code>\n"
        f"────────────────────────\n"
        f"Выбери раздел:"
    )
    if edit:
        await safe_edit(event, text, buttons=home_buttons())
    else:
        await event.respond(text, buttons=home_buttons(), parse_mode="html")


async def show_accounts_list(event, prefix: bytes = b"acc:", title: str = "📱 <b>Управление аккаунтами</b>"):
    names = account_names()
    buttons = []
    for phone in names:
        sess_exists = (account_dir(phone) / "session.session").exists()
        badge = "🔄 В работе" if phone in ACTIVE_JOBS else ("🟢" if sess_exists else "📁 Только архив")
        buttons.append([Button.inline(f"{badge} {phone}", prefix + phone.encode())])

    buttons.append([
        Button.inline("📷 Войти по QR-коду", b"add_account_qr"),
        Button.inline("📱 По номеру и коду", b"add_account"),
    ])
    if prefix == b"acc:" and names:
        buttons.append([Button.inline("🩺 Проверить все сессии", b"check_all_acc")])

    buttons.append([Button.inline("🏠 Главное меню", b"home")])

    sub = "Выбери аккаунт из списка или подключи новый:" if names else "Нет подключённых аккаунтов. Добавь первый (рекомендуется 📷 <b>По QR-коду</b>):"
    await safe_edit(event, f"{title}\n────────────────────────\n{sub}", buttons=buttons)


async def show_chat_picker(event, page: int = 0):
    w = WIZARDS.get(event.sender_id)
    if not w:
        return await safe_edit(event, "⚠️ Сессия настройки устарела.", buttons=[[Button.inline("🏠 Главное меню", b"home")]])

    apply_dialog_filters(w)
    indices = w.filtered_indices
    total = len(indices)
    page_count = max(1, (total + CHAT_PAGE_SIZE - 1) // CHAT_PAGE_SIZE)
    page = max(0, min(page, page_count - 1))
    w.chat_page = page

    buttons = []

    filter_tabs = [
        ("Все", "all"),
        ("👤 Личн.", "user"),
        ("👥 Группы", "group"),
        ("📢 Кан.", "channel"),
    ]
    tab_row = []
    for label, code in filter_tabs:
        mark = f"• {label} •" if w.type_filter == code else label
        tab_row.append(Button.inline(mark, f"flt:{code}".encode()))
    buttons.append(tab_row)

    start = page * CHAT_PAGE_SIZE
    for pos in range(start, min(start + CHAT_PAGE_SIZE, total)):
        real_idx = indices[pos]
        dlg = w.dialogs[real_idx]
        icon = dialog_icon(dlg)
        title = exporter.dialog_title(dlg)
        checked = "✅" if (w.selected_all or real_idx in w.selected_indices) else "☑️"
        label = f"{checked} {real_idx + 1}. {icon} {title}"
        buttons.append([Button.inline(label[:60], f"pick:{real_idx}".encode())])

    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"page:{page - 1}".encode()))
    nav.append(Button.inline(f"📄 {page + 1}/{page_count}", b"noop"))
    if page + 1 < page_count:
        nav.append(Button.inline("➡️", f"page:{page + 1}".encode()))
    if len(nav) > 1:
        buttons.append(nav)

    search_row = [Button.inline("🔍 Поиск", b"chat_search")]
    if w.search_query:
        search_row.append(Button.inline("✖️ Сброс поиска", b"chat_search_clear"))
    search_row.append(Button.inline("🔄 Обновить", b"chat_refresh"))
    buttons.append(search_row)

    mass_row = [Button.inline("📦 Выбрать ВСЕ чаты", b"pick_all")]
    if w.selected_indices or w.selected_all:
        mass_row.append(Button.inline("🧹 Снять выбор", b"pick_clear"))
    buttons.append(mass_row)

    sel_cnt = len(w.dialogs) if w.selected_all else len(w.selected_indices)
    done_label = f"✅ Готово (выбрано: {sel_cnt})" if sel_cnt else "🔙 Назад к настройкам"
    buttons.append([Button.inline(done_label, b"back_settings")])

    filter_info = (
        f"\n🔍 Поиск: <code>{exporter.escape_html(w.search_query)}</code> (найдено: {total})"
        if w.search_query else f"\nПоказано чатов: <b>{total}</b> из <b>{len(w.dialogs)}</b>"
    )
    text = (
        f"💬 <b>Выбор чатов для экспорта</b>\n"
        f"📱 Аккаунт: <code>{w.phone}</code>{filter_info}\n"
        f"☑️ Отмечено: <b>{sel_cnt} шт.</b>\n\n"
        f"<i>Нажимай на чаты, чтобы отметить один или несколько, затем нажми «Готово»:</i>"
    )
    await safe_edit(event, text, buttons=buttons)


async def show_export_settings(event, edit: bool = True):
    w = WIZARDS.get(event.sender_id)
    if not w:
        return await show_home(event, edit=edit)

    w.workers = exporter.clamp_workers(w.workers)

    if w.selected_all:
        target = f"📦 Все чаты ({len(w.dialogs)} шт.)"
        is_private_only = False
    elif not w.selected_indices:
        target = "⚠️ Не выбрано (нажми «Выбрать чаты»)"
        is_private_only = False
    elif len(w.selected_indices) == 1:
        idx = next(iter(w.selected_indices))
        dlg = w.dialogs[idx]
        target = f"{dialog_icon(dlg)} {exporter.dialog_title(dlg)} ({exporter.dialog_type_label(dlg)})"
        is_private_only = exporter.is_private_dialog(dlg)
    else:
        indices_sorted = sorted(w.selected_indices)
        names_preview = ", ".join(exporter.dialog_title(w.dialogs[i]) for i in indices_sorted[:2])
        extra = f" и ещё {len(indices_sorted) - 2}" if len(indices_sorted) > 2 else ""
        target = f"📑 Выбрано {len(indices_sorted)} чатов ({names_preview}{extra})"
        is_private_only = all(exporter.is_private_dialog(w.dialogs[i]) for i in indices_sorted)

    start_str = w.start_date.strftime("%d.%m.%Y") if w.start_date else "01.01.2000"
    end_str = w.end_date.strftime("%d.%m.%Y") if w.end_date else "по наст. время"
    media_str = "✅ Скачивать" if w.media else "❌ Пропускать"
    limit_str = f"до {w.max_size_mb} МБ" if (w.media and w.max_size_mb > 0) else ("Без лимита (∞)" if w.media else "—")
    mode_str = "Личные чаты (все сообщения)" if is_private_only else (
        "👥 Все сообщения" if w.mode == "all" else "👤 Только мои сообщения и комменты"
    )
    lang_str = "🇷🇺 Русский (RU)" if w.lang == "ru" else "🇬🇧 English (EN)"
    zip_str = "✅ Початовые + общий ZIP" if w.make_zip else "📦 Только единый ZIP"

    text = (
        f"⚙️ <b>Конфигурация экспорта</b>\n"
        f"────────────────────────\n"
        f"📱 <b>Аккаунт:</b> <code>{w.phone}</code>\n"
        f"🎯 <b>Объект:</b> {exporter.escape_html(target)}\n"
        f"📅 <b>Период:</b> <code>{start_str} — {end_str}</code>\n"
        f"🖼 <b>Медиафайлы:</b> {media_str}\n"
        f"⚖️ <b>Лимит файла:</b> {limit_str}\n"
        f"💬 <b>Режим групп/каналов:</b> {mode_str}\n"
        f"🌐 <b>Язык HTML:</b> {lang_str}\n"
        f"🗜 <b>Архивация:</b> {zip_str}\n"
        f"⚡ <b>Потоки загрузки:</b> <code>{w.workers}</code> (из 1 / 2 / 4 / 8)\n"
        f"────────────────────────\n"
        f"<i>Настрой параметры ниже и нажми «Запустить экспорт»:</i>"
    )

    sel_cnt = len(w.dialogs) if w.selected_all else len(w.selected_indices)
    chat_btn_label = f"💬 Выбрать чаты (выбрано: {sel_cnt})" if sel_cnt else "💬 Выбрать чаты для выгрузки"

    worker_row = [
        Button.inline(f"⚡ {val} пот. {'✅' if w.workers == val else ''}".strip(), f"set_workers:{val}".encode())
        for val in ALLOWED_WORKERS
    ]

    buttons = [
        [Button.inline(chat_btn_label, f"page:{w.chat_page}".encode())],
        [
            Button.inline(f"📅 С: {start_str}", b"date_start"),
            Button.inline(f"📅 По: {end_str}", b"date_end"),
            Button.inline("⏱ Пресеты", b"date_presets"),
        ],
        [
            Button.inline(f"🖼 Медиа: {'Вкл' if w.media else 'Выкл'}", b"toggle_media"),
            Button.inline(f"⚖️ Лимит: {w.max_size_mb if w.max_size_mb else '∞'} МБ", b"menu_maxsize"),
        ],
        [
            Button.inline(f"💬 Режим: {'Все' if w.mode == 'all' else 'Мои'}", b"toggle_mode"),
            Button.inline(f"🌐 Язык: {w.lang.upper()}", b"toggle_lang"),
            Button.inline(f"🗜 ZIP чатов: {'Вкл' if w.make_zip else 'Выкл'}", b"toggle_zip"),
        ],
        worker_row,
        [Button.inline("🚀 ЗАПУСТИТЬ ЭКСПОРТ", b"start_export")],
        [Button.inline("🏠 Главное меню", b"home")],
    ]

    if edit:
        await safe_edit(event, text, buttons=buttons)
    else:
        await event.respond(text, buttons=buttons, parse_mode="html")


# =====================================================================
# ОБРАБОТЧИКИ КОМАНД И ТЕКСТОВОГО ВВОДА
# =====================================================================

@BOT.on(events.NewMessage(pattern=r"^/start$"))
async def on_start(event):
    if not is_admin(event):
        return await event.respond("⛔ Доступ запрещён.")
    await safe_disconnect_auth(event.sender_id)
    WIZARDS.pop(event.sender_id, None)
    await show_home(event, edit=False)


@BOT.on(events.NewMessage(pattern=r"^/cancel$"))
async def on_cancel(event):
    if not is_admin(event):
        return
    await safe_disconnect_auth(event.sender_id)

    w = WIZARDS.get(event.sender_id)
    if w and w.awaiting:
        w.awaiting = None
        await event.respond("↩️ Ввод параметра отменён.")
        return await show_export_settings(event, edit=False)

    WIZARDS.pop(event.sender_id, None)
    await event.respond("✖️ Действие отменено.", buttons=home_buttons())


@BOT.on(events.NewMessage)
async def on_text(event):
    if not is_admin(event) or not event.raw_text or event.raw_text.startswith("/"):
        return

    text = event.raw_text.strip()
    auth = AUTH_STATES.get(event.sender_id)
    if auth:
        return await handle_auth_input(event, text, auth)

    w = WIZARDS.get(event.sender_id)
    if w and w.awaiting:
        return await handle_wizard_input(event, text, w)


async def handle_auth_input(event, text: str, state: AuthState):
    if state.step == "qr":
        return await event.respond(
            "📷 Сейчас активен вход по QR-коду. Отсканируй картинку выше или нажми /cancel."
        )

    if state.step == "phone":
        try:
            phone = exporter.normalize_phone(text)
        except ValueError as exc:
            return await event.respond(f"⚠️ {exc}\nПопробуй ещё раз или нажми /cancel")

        folder = account_dir(phone)
        folder.mkdir(parents=True, exist_ok=True)
        client = create_telegram_client(
            session_path(phone), CONFIG["api_id"], CONFIG["api_hash"], proxy=PROXY
        )
        client.flood_sleep_threshold = 0

        status_msg = await event.respond(f"⏳ Подключаюсь и запрашиваю код для <code>{phone}</code>...", parse_mode="html")
        try:
            await client.connect()
            if await client.is_user_authorized():
                ACCOUNT_CLIENTS[phone] = client
                AUTH_STATES.pop(event.sender_id, None)
                return await safe_edit(
                    status_msg,
                    f"✅ Аккаунт <code>{phone}</code> уже авторизован и готов к работе!",
                    buttons=home_buttons(),
                )

            sent = None
            for attempt in range(1, 4):
                try:
                    sent = await client.send_code_request(phone)
                    break
                except AuthRestartError:
                    if attempt >= 3:
                        raise
                    await client.disconnect()
                    await client.connect()

            state.step = "code"
            state.phone = phone
            state.client = client
            state.phone_code_hash = sent.phone_code_hash
            await safe_edit(
                status_msg,
                f"📲 <b>Код отправлен в Telegram на номер {phone}</b>\n\n"
                f"Отправь код в ответном сообщении.\n"
                f"<i>💡 Совет: чтобы Telegram не заблокировал вход при пересылке, введи код через пробел (например: <code>12 345</code>).</i>\n\n"
                f"Для отмены: /cancel",
            )
        except FloodWaitError as exc:
            await client.disconnect()
            AUTH_STATES.pop(event.sender_id, None)
            await safe_edit(
                status_msg,
                f"⏳ <b>Telegram ограничил отправку кодов (FloodWait: {exc.seconds} сек.)</b>\n\n"
                f"💡 <b>Решение:</b> нажми кнопку ниже и войди <b>по QR-коду</b> — он работает мгновенно и не требует ожидания!",
                buttons=[
                    [Button.inline("📷 Войти по QR-коду", b"add_account_qr")],
                    [Button.inline("🏠 Главное меню", b"home")],
                ],
            )
        except Exception as exc:  # noqa: BLE001 - auth request needs cleanup for unexpected failures
            if client.is_connected():
                await client.disconnect()
            AUTH_STATES.pop(event.sender_id, None)
            log.error("Ошибка при запросе кода (%s)", type(exc).__name__)
            await safe_edit(status_msg, f"❌ Ошибка запроса кода: <code>{type(exc).__name__}: {exc}</code>", buttons=home_buttons())
        return

    if state.step == "code":
        clean_code = re.sub(r"\D", "", text)
        try:
            await event.delete()
        except (RPCError, OSError, TimeoutError) as exc:
            log.debug("Не удалось удалить сообщение с кодом (%s)", type(exc).__name__)

        if not clean_code:
            return await event.respond("⚠️ Код должен содержать цифры. Введи код или /cancel")

        try:
            await state.client.sign_in(phone=state.phone, code=clean_code, phone_code_hash=state.phone_code_hash)
            phone, full_name = await finalize_authorized_client(event.sender_id, state)
            await event.respond(
                f"✅ Аккаунт <code>{phone}</code> ({exporter.escape_html(full_name)}) успешно подключён!",
                buttons=home_buttons(),
                parse_mode="html",
            )
        except SessionPasswordNeededError:
            state.step = "password"
            await event.respond(
                "🔐 <b>Двухфакторная аутентификация (2FA)</b>\n\n"
                "Введи облачный пароль. Сообщение с паролем будет автоматически удалено из чата.\n"
                "Отмена: /cancel",
                parse_mode="html",
            )
        except PhoneCodeInvalidError:
            await event.respond("❌ Неверный код. Проверь и отправь заново (или /cancel).")
        except PhoneCodeExpiredError:
            await safe_disconnect_auth(event.sender_id)
            await event.respond("⌛ Срок действия кода истёк. Начни подключение заново.", buttons=home_buttons())
        except FloodWaitError as exc:
            wait = max(1, int(getattr(exc, "seconds", 1) or 1))
            await safe_disconnect_auth(event.sender_id)
            await event.respond(
                f"⏳ Telegram ограничил вход. Подожди {wait} сек. и начни авторизацию заново.",
                buttons=home_buttons(),
            )
        except Exception as exc:  # noqa: BLE001 - auth handler must close state on unexpected failures
            await safe_disconnect_auth(event.sender_id)
            log.error("Ошибка при вводе кода (%s)", type(exc).__name__)
            await event.respond(f"❌ Ошибка входа: <code>{type(exc).__name__}: {exc}</code>", buttons=home_buttons(), parse_mode="html")
        return

    if state.step == "password":
        try:
            await event.delete()
        except (RPCError, OSError, TimeoutError) as exc:
            log.debug("Не удалось удалить сообщение с паролем (%s)", type(exc).__name__)

        try:
            await asyncio.wait_for(state.client.sign_in(password=text), timeout=30)
            phone, full_name = await finalize_authorized_client(event.sender_id, state)
            await event.respond(
                f"✅ <b>2FA пройден!</b>\n📱 Аккаунт <code>{phone}</code> ({exporter.escape_html(full_name)}) успешно подключён.",
                buttons=home_buttons(),
                parse_mode="html",
            )
        except PasswordHashInvalidError:
            await event.respond("❌ Неверный пароль 2FA. Попробуй ещё раз или /cancel.")
        except FloodWaitError as exc:
            wait = max(1, int(getattr(exc, "seconds", 1) or 1))
            await safe_disconnect_auth(event.sender_id)
            await event.respond(
                f"⏳ Telegram ограничил проверку пароля. Подожди {wait} сек. и начни авторизацию заново.",
                buttons=home_buttons(),
            )
        except Exception as exc:  # noqa: BLE001 - auth handler must close state on unexpected failures
            await safe_disconnect_auth(event.sender_id)
            log.error("Ошибка 2FA (%s)", type(exc).__name__)
            await event.respond(f"❌ Ошибка проверки пароля: <code>{type(exc).__name__}: {exc}</code>", buttons=home_buttons(), parse_mode="html")


async def handle_wizard_input(event, text: str, w: ExportWizard):
    field_name = w.awaiting
    w.awaiting = None

    if field_name in ("start_date", "end_date"):
        if text.lower() in ("нет", "пропустить", "skip", "-", "0", "все"):
            parsed = None
        else:
            try:
                parsed = datetime.strptime(text, "%d.%m.%Y")
                if field_name == "end_date":
                    parsed = parsed.replace(hour=23, minute=59, second=59)
            except ValueError:
                w.awaiting = field_name
                return await event.respond("⚠️ Неверный формат. Введи дату как <code>ДД.ММ.ГГГГ</code> (или <code>-</code> для сброса). Отмена: /cancel", parse_mode="html")

        if field_name == "start_date":
            w.start_date = parsed or datetime(2000, 1, 1)
        else:
            w.end_date = parsed

        if w.start_date and w.end_date and w.end_date < w.start_date:
            w.end_date = None
            w.awaiting = "end_date"
            return await event.respond("⚠️ Дата окончания не может быть раньше даты начала! Введи дату окончания заново:")
        return await show_export_settings(event, edit=False)

    if field_name == "max_size":
        try:
            val = int(text)
            if val < 0:
                raise ValueError
            w.max_size_mb = val
        except ValueError:
            w.awaiting = "max_size"
            return await event.respond("⚠️ Введи целое число мегабайт (0 = без лимита). Отмена: /cancel")
        return await show_export_settings(event, edit=False)

    if field_name == "search":
        w.search_query = text.strip()
        apply_dialog_filters(w)
        if not w.filtered_indices:
            await event.respond(f"🔍 По запросу «<b>{exporter.escape_html(text)}</b>» чатов не найдено. Сбрасываю фильтр.", parse_mode="html")
            w.search_query = ""
            apply_dialog_filters(w)
        msg = await event.respond("⏳ Загружаю результаты...", parse_mode="html")
        return await show_chat_picker(msg, 0)


# =====================================================================
# ОБРАБОТЧИК INLINE-КНОПОК (CALLBACK QUERY)
# =====================================================================

@BOT.on(events.CallbackQuery)
async def on_callback(event):
    if not is_admin(event):
        return await event.answer("⛔ Нет доступа", alert=True)

    data = event.data.decode("utf-8", errors="replace")

    if data == "noop":
        return await event.answer()

    if data == "home":
        WIZARDS.pop(event.sender_id, None)
        await safe_disconnect_auth(event.sender_id)
        return await show_home(event, edit=True)

    # --- УПРАВЛЕНИЕ АККАУНТАМИ ---
    if data == "accounts":
        await safe_disconnect_auth(event.sender_id)
        return await show_accounts_list(event, prefix=b"acc:", title="📱 <b>Подключённые аккаунты</b>")

    if data == "add_account_qr":
        return await start_qr_login_flow(event)

    if data == "check_all_acc":
        names = account_names()
        if not names:
            return await event.answer("Нет аккаунтов для проверки.", alert=True)
        await safe_edit(event, "🩺 <b>Проверяю соединение всех аккаунтов...</b>")
        report = ["🩺 <b>Диагностика аккаунтов:</b>\n"]
        for phone in names:
            try:
                client = await get_account_client(phone)
                me = await client.get_me()
                uname = f"@{me.username}" if getattr(me, "username", None) else "без юзернейма"
                report.append(f"🟢 <code>{phone}</code> — Активен ({exporter.escape_html(uname)})")
            except Exception as exc:  # noqa: BLE001 - report each account's status independently
                report.append(f"🔴 <code>{phone}</code> — {exporter.escape_html(str(exc))}")
        return await safe_edit(
            event,
            "\n".join(report),
            buttons=[[Button.inline("🔙 К аккаунтам", b"accounts")], [Button.inline("🏠 Главное меню", b"home")]],
        )

    if data == "add_account":
        await safe_disconnect_auth(event.sender_id)
        AUTH_STATES[event.sender_id] = AuthState(step="phone")
        return await safe_edit(
            event,
            "➕ <b>Добавление аккаунта по номеру</b>\n"
            "────────────────────────\n"
            "Отправь номер телефона в международном формате (например, <code>+79001234567</code>).\n\n"
            "<i>💡 Если Telegram ограничил запрос кода, можно попробовать вход по QR-коду.</i>\n"
            "Для отмены: /cancel",
            buttons=[
                [Button.inline("📷 Войти по QR-коду", b"add_account_qr")],
                [Button.inline("🔙 Назад", b"accounts")],
            ],
        )

    if data.startswith("acc:"):
        phone = data.split(":", 1)[1]
        await safe_edit(event, f"⏳ Проверяю сессию <code>{phone}</code>...")
        try:
            client = await get_account_client(phone)
            me = await client.get_me()
            full_name = f"{getattr(me, 'first_name', '') or ''} {getattr(me, 'last_name', '') or ''}".strip() or "Без имени"
            username = f"@{me.username}" if getattr(me, "username", None) else "нет"
            status_txt = "🟢 Авторизован и активен"
        except Exception as exc:  # noqa: BLE001 - show account status even for unexpected failures
            full_name = "Недоступно"
            username = "—"
            status_txt = f"🔴 {exc}"

        out_dir = account_dir(phone) / "export"
        has_export = (out_dir / "index.html").exists()
        archives = list(account_dir(phone).glob("export_full_*.zip"))

        info = (
            f"📱 <b>Профиль аккаунта {phone}</b>\n"
            f"────────────────────────\n"
            f"👤 <b>Имя:</b> {exporter.escape_html(full_name)}\n"
            f"🔗 <b>Юзернейм:</b> {exporter.escape_html(username)}\n"
            f"📡 <b>Статус:</b> {exporter.escape_html(status_txt)}\n"
            f"📂 <b>Готовый HTML-экспорт:</b> {'✅ Есть' if has_export else '❌ Нет'}\n"
            f"🗜 <b>Полных архивов:</b> {len(archives)} шт."
        )
        buttons = [
            [Button.inline("🚀 Перейти к экспорту", f"init_exp:{phone}".encode())],
            [Button.inline("🗑 Отключить / Удалить", f"del_acc_confirm:{phone}".encode())],
            [Button.inline("🔙 К списку аккаунтов", b"accounts")],
        ]
        return await safe_edit(event, info, buttons=buttons)

    if data.startswith("del_acc_confirm:"):
        phone = data.split(":", 1)[1]
        return await safe_edit(
            event,
            f"⚠️ <b>Удаление аккаунта {phone}</b>\n\n"
            f"Что именно нужно удалить?",
            buttons=[
                [Button.inline("🔑 Только сессию (оставить архивы)", f"del_acc_sess:{phone}".encode())],
                [Button.inline("🔥 Удалить ВСЁ (сессию + все экспорты)", f"del_acc_all:{phone}".encode())],
                [Button.inline("🔙 Отмена", f"acc:{phone}".encode())],
            ],
        )

    if data.startswith(("del_acc_sess:", "del_acc_all:")):
        mode, phone = data.split(":", 1)
        if phone in ACTIVE_JOBS:
            return await event.answer("Сначала останови активный экспорт для этого аккаунта!", alert=True)
        client = ACCOUNT_CLIENTS.pop(phone, None)
        if client and client.is_connected():
            await client.disconnect()

        folder = account_dir(phone)
        if mode == "del_acc_all":
            shutil.rmtree(folder, ignore_errors=True)
            await event.answer("Аккаунт и все файлы удалены.", alert=True)
        else:
            for f in folder.glob("session*"):
                f.unlink(missing_ok=True)
            await event.answer("Сессия удалена.", alert=True)
        return await show_accounts_list(event, prefix=b"acc:")

    # --- ИНИЦИАЛИЗАЦИЯ ЭКСПОРТА ---
    if data == "export":
        return await show_accounts_list(event, prefix=b"init_exp:", title="🚀 <b>Выбери аккаунт для экспорта</b>")

    if data.startswith("init_exp:"):
        phone = data.split(":", 1)[1]
        if phone in ACTIVE_JOBS:
            return await event.answer("Для этого аккаунта уже запущен экспорт! Проверь раздел «Активные задачи».", alert=True)
        await safe_edit(event, f"⏳ Подключаюсь к <code>{phone}</code> и загружаю список диалогов...")
        try:
            client = await get_account_client(phone)
            dialogs = await exporter.get_dialogs(client)
            if not dialogs:
                return await safe_edit(event, "⚠️ В этом аккаунте нет ни одного чата.", buttons=[[Button.inline("🔙 Назад", b"export")]])
            WIZARDS[event.sender_id] = ExportWizard(
                phone=phone,
                client=client,
                dialogs=dialogs,
                filtered_indices=list(range(len(dialogs))),
            )
            return await show_export_settings(event, edit=True)
        except Exception as exc:  # noqa: BLE001 - report unexpected dialog-loading failures
            log.error("Ошибка загрузки диалогов (%s)", type(exc).__name__)
            return await safe_edit(
                event,
                f"❌ Не удалось загрузить чаты: <code>{type(exc).__name__}: {exc}</code>",
                buttons=[[Button.inline("🔙 Назад", b"export")]],
            )

    # --- НАВИГАЦИЯ ПО ЧАТАМ, ФИЛЬТРЫ И МУЛЬТИВЫБОР ---
    if data.startswith("page:"):
        page = int(data.split(":", 1)[1])
        return await show_chat_picker(event, page)

    if data.startswith("flt:"):
        flt = data.split(":", 1)[1]
        w = WIZARDS.get(event.sender_id)
        if not w:
            return await show_home(event, edit=True)
        w.type_filter = flt
        return await show_chat_picker(event, 0)

    if data == "chat_search":
        w = WIZARDS.get(event.sender_id)
        if not w:
            return await show_home(event, edit=True)
        w.awaiting = "search"
        return await safe_edit(
            event,
            "🔍 <b>Поиск чата</b>\n\nОтправь часть названия чата, имени контакта или канала.\nДля отмены: /cancel",
            buttons=[[Button.inline("🔙 Назад к списку", f"page:{w.chat_page}".encode())]],
        )

    if data == "chat_search_clear":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.search_query = ""
        return await show_chat_picker(event, 0)

    if data == "chat_refresh":
        w = WIZARDS.get(event.sender_id)
        if not w:
            return await show_home(event, edit=True)
        await event.answer("🔄 Обновляю список чатов...")
        w.dialogs = await exporter.get_dialogs(w.client)
        w.search_query = ""
        w.selected_indices.clear()
        w.selected_all = False
        return await show_chat_picker(event, 0)

    if data.startswith("pick:"):
        idx = int(data.split(":", 1)[1])
        w = WIZARDS.get(event.sender_id)
        if not w or not (0 <= idx < len(w.dialogs)):
            return await event.answer("⚠️ Список чатов устарел.", alert=True)
        if w.selected_all:
            w.selected_all = False
            w.selected_indices = set(range(len(w.dialogs)))
        if idx in w.selected_indices:
            w.selected_indices.remove(idx)
        else:
            w.selected_indices.add(idx)
        return await show_chat_picker(event, w.chat_page)

    if data == "pick_all":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.selected_all = True
            w.selected_indices.clear()
        return await show_export_settings(event, edit=True)

    if data == "pick_clear":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.selected_all = False
            w.selected_indices.clear()
        return await show_chat_picker(event, w.chat_page)

    if data == "back_settings":
        return await show_export_settings(event, edit=True)

    # --- НАСТРОЙКИ ДАТ И ПРЕСЕТЫ ---
    if data == "date_start":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.awaiting = "start_date"
            return await safe_edit(
                event,
                "📅 <b>Дата начала экспорта</b>\n\n"
                "Отправь дату в формате <code>ДД.ММ.ГГГГ</code> (например, <code>01.01.2025</code>).\n"
                "Отправь <code>-</code> или <code>0</code>, чтобы выгружать с самого начала.",
                buttons=[[Button.inline("🔙 Назад", b"back_settings")]],
            )

    if data == "date_end":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.awaiting = "end_date"
            return await safe_edit(
                event,
                "📅 <b>Дата окончания экспорта</b>\n\n"
                "Отправь дату в формате <code>ДД.ММ.ГГГГ</code>.\n"
                "Отправь <code>-</code> или <code>пропустить</code>, чтобы выгружать до текущего момента.",
                buttons=[[Button.inline("🔙 Назад", b"back_settings")]],
            )

    if data == "date_presets":
        now = datetime.now()
        return await safe_edit(
            event,
            "⏱ <b>Быстрый выбор периода выгрузки</b>\n\nВыбери готовый интервал:",
            buttons=[
                [
                    Button.inline("За 7 дней", b"preset_date:7d"),
                    Button.inline("За 30 дней", b"preset_date:30d"),
                ],
                [
                    Button.inline("За 90 дней", b"preset_date:90d"),
                    Button.inline(f"Весь {now.year} год", b"preset_date:year"),
                ],
                [Button.inline("♾ За всё время (сброс)", b"preset_date:all")],
                [Button.inline("🔙 Назад", b"back_settings")],
            ],
        )

    if data.startswith("preset_date:"):
        w = WIZARDS.get(event.sender_id)
        if w:
            code = data.split(":", 1)[1]
            now = datetime.now()
            w.end_date = None
            if code == "7d":
                w.start_date = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0)
            elif code == "30d":
                w.start_date = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0)
            elif code == "90d":
                w.start_date = (now - timedelta(days=90)).replace(hour=0, minute=0, second=0)
            elif code == "year":
                w.start_date = datetime(now.year, 1, 1)
            else:
                w.start_date = datetime(2000, 1, 1)
        return await show_export_settings(event, edit=True)

    # --- НАСТРОЙКИ ПОТОКОВ (СТРОГО 1 - 2 - 4 - 8) ---
    if data.startswith("set_workers:"):
        w = WIZARDS.get(event.sender_id)
        if w:
            val = int(data.split(":", 1)[1])
            w.workers = exporter.clamp_workers(val)
            await event.answer(f"⚡ Установлено потоков: {w.workers}")
        return await show_export_settings(event, edit=True)

    # --- НАСТРОЙКИ МЕДИА И РЕЖИМОВ ---
    if data == "menu_maxsize":
        w = WIZARDS.get(event.sender_id)
        if not w:
            return await show_home(event, edit=True)
        presets = [0, 5, 15, 50, 100, 500]
        btns = [
            [
                Button.inline("♾ Без лимита" if mb == 0 else f"{mb} МБ", f"set_maxsize:{mb}".encode())
                for mb in presets[:3]
            ],
            [
                Button.inline(f"{mb} МБ", f"set_maxsize:{mb}".encode())
                for mb in presets[3:]
            ],
            [Button.inline("✍️ Ввести своё значение", b"custom_maxsize")],
            [Button.inline("🔙 Назад", b"back_settings")],
        ]
        return await safe_edit(
            event,
            "⚖️ <b>Максимальный размер одного медиафайла</b>\n\n"
            "Файлы больше указанного размера скачиваться не будут (в HTML останется плашка о файле).\n"
            "Выбери готовый лимит или введи свой:",
            buttons=btns,
        )

    if data.startswith("set_maxsize:"):
        w = WIZARDS.get(event.sender_id)
        if w:
            w.max_size_mb = int(data.split(":", 1)[1])
        return await show_export_settings(event, edit=True)

    if data == "custom_maxsize":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.awaiting = "max_size"
            return await safe_edit(
                event,
                "✍️ Отправь максимальный размер файла в МБ (целое число, <code>0</code> — без ограничений):",
                buttons=[[Button.inline("🔙 Назад", b"back_settings")]],
            )

    if data == "toggle_media":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.media = not w.media
        return await show_export_settings(event, edit=True)

    if data == "toggle_zip":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.make_zip = not w.make_zip
        return await show_export_settings(event, edit=True)

    if data == "toggle_mode":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.mode = "all" if w.mode == "mine" else "mine"
        return await show_export_settings(event, edit=True)

    if data == "toggle_lang":
        w = WIZARDS.get(event.sender_id)
        if w:
            w.lang = "en" if w.lang == "ru" else "ru"
        return await show_export_settings(event, edit=True)

    if data == "start_export":
        return await launch_export(event)

    # --- СТАТУС И ОТМЕНА ЗАДАЧ ---
    if data == "status":
        return await show_status_screen(event)

    if data.startswith("cancel_job:"):
        phone = data.split(":", 1)[1]
        job = ACTIVE_JOBS.get(phone)
        if job and job.task and not job.task.done():
            job.task.cancel()
            await event.answer(f"🛑 Останавливаю экспорт {phone}...", alert=True)
            await asyncio.sleep(0.5)
        else:
            await event.answer("Экспорт уже завершён.", alert=True)
        return await show_status_screen(event)

    # --- ФАЙЛОВЫЙ МЕНЕДЖЕР ---
    if data == "files":
        return await show_files_screen(event)

    if data.startswith("dl_zip:"):
        phone = data.split(":", 1)[1]
        archives = sorted(account_dir(phone).glob("export_full_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not archives:
            return await event.answer("Архивы не найдены.", alert=True)
        latest = archives[0]
        size_mb = latest.stat().st_size / (1024 * 1024)
        if latest.stat().st_size > TELEGRAM_UPLOAD_LIMIT:
            return await event.answer(f"Файл слишком большой ({size_mb:.1f} МБ) для отправки через бота.", alert=True)
        await event.answer("📤 Отправляю полный архив в чат...")
        await BOT.send_file(
            event.chat_id,
            str(latest),
            caption=f"📦 <b>Полный архив {phone}</b>\nРазмер: {size_mb:.1f} МБ\nФайл: <code>{latest.name}</code>",
            parse_mode="html",
        )
        return

    if data.startswith("dl_idx:"):
        phone = data.split(":", 1)[1]
        idx_file = account_dir(phone) / "export" / "index.html"
        if not idx_file.exists():
            return await event.answer("Файл index.html не найден.", alert=True)
        await event.answer("📤 Отправляю index.html...")
        await BOT.send_file(
            event.chat_id,
            str(idx_file),
            caption=f"📄 <b>Индексная страница {phone}</b>\nСписок всех выгруженных чатов и статистика профиля.",
            parse_mode="html",
        )
        return

    if data.startswith("chat_zips:"):
        phone = data.split(":", 1)[1]
        out = account_dir(phone) / "export"
        zips = sorted(out.glob("*.zip")) if out.exists() else []
        if not zips:
            return await event.answer("Початовых архивов нет.", alert=True)
        btns = []
        for idx, zf in enumerate(zips[:25]):
            sz = zf.stat().st_size / (1024 * 1024)
            btns.append([Button.inline(f"🗜 {zf.stem} ({sz:.1f} МБ)", f"dl_czip:{phone}:{idx}".encode())])
        btns.append([Button.inline("🔙 Назад к файлам", b"files")])
        return await safe_edit(
            event,
            f"🗂 <b>Початовые архивы аккаунта {phone}</b>\nВыбери архив чата для скачивания:",
            buttons=btns,
        )

    if data.startswith("dl_czip:"):
        _, phone, idx_str = data.split(":", 2)
        out = account_dir(phone) / "export"
        zips = sorted(out.glob("*.zip")) if out.exists() else []
        idx = int(idx_str)
        if not (0 <= idx < len(zips)):
            return await event.answer("Файл не найден.", alert=True)
        target_zip = zips[idx]
        sz_mb = target_zip.stat().st_size / (1024 * 1024)
        if target_zip.stat().st_size > TELEGRAM_UPLOAD_LIMIT:
            return await event.answer(f"Архив слишком большой ({sz_mb:.1f} МБ).", alert=True)
        await event.answer("📤 Отправляю архив чата...")
        await BOT.send_file(
            event.chat_id,
            str(target_zip),
            caption=f"🗜 <b>Чат: {exporter.escape_html(target_zip.stem)}</b> ({phone})\nРазмер: {sz_mb:.1f} МБ",
            parse_mode="html",
        )
        return

    if data.startswith("clean_files:"):
        phone = data.split(":", 1)[1]
        if phone in ACTIVE_JOBS:
            return await event.answer("Нельзя удалять файлы во время активного экспорта!", alert=True)
        acc_path = account_dir(phone)
        shutil.rmtree(acc_path / "export", ignore_errors=True)
        for zf in acc_path.glob("export_full_*.zip"):
            zf.unlink(missing_ok=True)
        await event.answer(f"🧹 Экспорты и архивы {phone} очищены!", alert=True)
        return await show_files_screen(event)


# =====================================================================
# ЭКРАНЫ СТАТУСА, ФАЙЛОВ И ЗАПУСК ЗАДАЧИ ЭКСПОРТА
# =====================================================================

async def show_status_screen(event):
    if not ACTIVE_JOBS:
        return await safe_edit(
            event,
            "📊 <b>Активные задачи</b>\n────────────────────────\nСейчас нет запущенных выгрузок.",
            buttons=[[Button.inline("🔄 Обновить", b"status")], [Button.inline("🏠 Главное меню", b"home")]],
        )

    lines = ["📊 <b>Активные задачи экспорта</b>\n────────────────────────"]
    buttons = []
    for phone, job in list(ACTIVE_JOBS.items()):
        elapsed = format_duration(time.time() - job.started_at)
        bar = render_progress_bar(job.current_idx, job.total_chats)
        pct = int((job.current_idx / max(1, job.total_chats)) * 100)
        lines.append(
            f"📱 <code>{phone}</code> — <b>{exporter.escape_html(job.target_title)}</b>\n"
            f"<code>[{bar}] {pct}%</code> ({job.current_idx}/{job.total_chats})\n"
            f"💬 Чат: <b>{exporter.escape_html(job.current_chat)}</b>\n"
            f"⚙️ Этап: <i>{exporter.escape_html(job.stage)}</i>\n"
            f"⏱ Прошло: <code>{elapsed}</code>\n"
        )
        buttons.append([Button.inline(f"🛑 Остановить {phone}", f"cancel_job:{phone}".encode())])

    buttons.append([Button.inline("🔄 Обновить", b"status"), Button.inline("🏠 Главное меню", b"home")])
    await safe_edit(event, "\n".join(lines), buttons=buttons)


async def show_files_screen(event):
    free_gb = free_disk_gb()
    lines = [(
        f"🗂 <b>Экспорты и архивы на сервере</b>\n"
        f"💾 Свободно на диске: <code>{free_gb:.1f} ГБ</code>\n"
        f"────────────────────────"
    )]
    buttons = []
    found_any = False

    for phone in account_names():
        acc_path = account_dir(phone)
        out = acc_path / "export"
        full_zips = sorted(acc_path.glob("export_full_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        chat_zips = list(out.glob("*.zip")) if out.exists() else []
        has_html = (out / "index.html").exists()

        if not has_html and not full_zips:
            continue

        found_any = True
        latest_info = "нет"
        if full_zips:
            sz = full_zips[0].stat().st_size / (1024 * 1024)
            latest_info = f"{full_zips[0].name} ({sz:.1f} МБ)"

        lines.append(
            f"📱 <code>{phone}</code>\n"
            f"  ├ Индекс HTML: {'✅ Есть' if has_html else '❌ Нет'}\n"
            f"  ├ Архивов чатов: <b>{len(chat_zips)} шт.</b>\n"
            f"  └ Полный ZIP: <code>{latest_info}</code>\n"
        )
        row1 = []
        if full_zips:
            row1.append(Button.inline(f"📥 Полный ZIP ({phone})", f"dl_zip:{phone}".encode()))
        if has_html:
            row1.append(Button.inline("📄 index.html", f"dl_idx:{phone}".encode()))
        if row1:
            buttons.append(row1)

        row2 = []
        if chat_zips:
            row2.append(Button.inline(f"🗂 Архивы чатов ({len(chat_zips)})", f"chat_zips:{phone}".encode()))
        row2.append(Button.inline(f"🧹 Очистить ({phone})", f"clean_files:{phone}".encode()))
        buttons.append(row2)

    if not found_any:
        lines.append("\nНа сервере пока нет сохранённых выгрузок.")

    buttons.append([Button.inline("🏠 Главное меню", b"home")])
    await safe_edit(event, "\n".join(lines), buttons=buttons)


async def launch_export(event):
    w = WIZARDS.get(event.sender_id)
    if not w:
        return await event.answer("⚠️ Настройки устарели.", alert=True)
    if not w.selected_all and not w.selected_indices:
        return await event.answer("⚠️ Сначала выбери хотя бы один чат или нажми «Выбрать ВСЕ чаты»!", alert=True)

    phone = w.phone
    if phone in ACTIVE_JOBS:
        return await event.answer("⚠️ Для этого аккаунта уже идёт экспорт!", alert=True)

    if w.selected_all:
        selected_dialogs = list(w.dialogs)
        target_title = f"Все чаты ({len(selected_dialogs)} шт.)"
    else:
        sorted_indices = sorted(w.selected_indices)
        selected_dialogs = [w.dialogs[i] for i in sorted_indices]
        if len(selected_dialogs) == 1:
            target_title = exporter.dialog_title(selected_dialogs[0])
        else:
            target_title = f"Выбранные чаты ({len(selected_dialogs)} шт.)"

    output_dir = account_dir(phone) / "export"

    workers = exporter.clamp_workers(w.workers)

    args = SimpleNamespace(
        output=str(output_dir),
        lang=w.lang,
        date_from=w.start_date.strftime("%d.%m.%Y") if w.start_date else None,
        date_to=w.end_date.strftime("%d.%m.%Y") if w.end_date else None,
        media=w.media,
        max_size=w.max_size_mb if w.max_size_mb > 0 else None,
        mode=w.mode,
        all=w.selected_all,
        chat=None,
        selected_dialogs=selected_dialogs,
        zip=w.make_zip,
        workers=workers,
    )

    job = ActiveJob(
        phone=phone,
        admin_id=event.sender_id,
        target_title=target_title,
        total_chats=len(selected_dialogs),
    )
    ACTIVE_JOBS[phone] = job
    try:
        status_msg = await safe_edit(
            event,
            f"🚀 <b>Экспорт запущен!</b>\n"
            f"────────────────────────\n"
            f"📱 Аккаунт: <code>{phone}</code>\n"
            f"🎯 Объект: <b>{exporter.escape_html(target_title)}</b>\n"
            f"⚡ Потоков загрузки: <code>{workers}</code>\n"
            f"⏳ Этап: <i>Подготовка экспорта...</i>",
            buttons=[
                [Button.inline("🛑 Остановить экспорт", f"cancel_job:{phone}".encode())],
                [Button.inline("🏠 Главное меню", b"home")],
            ],
        )
        shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
    except BaseException:
        ACTIVE_JOBS.pop(phone, None)
        WIZARDS[event.sender_id] = w
        raise
    WIZARDS.pop(event.sender_id, None)
    job.status_msg_id = getattr(status_msg, "id", None)

    last_edit_ts = 0.0

    async def on_progress(
        current: int,
        total: int,
        chat_title: str,
        stage: str = "В процессе...",
        force: bool = False,
    ):
        nonlocal last_edit_ts
        job.current_idx = current
        job.total_chats = total
        job.current_chat = chat_title
        job.stage = stage or "Обработка..."

        now = time.time()
        # ЖЁСТКИЙ ТРОТТЛИНГ: не чаще 1 раза в 3 секунды ДАЖЕ на последнем чате (127/127)!
        if not force and now - last_edit_ts < 3.0:
            return
        last_edit_ts = now

        bar = render_progress_bar(current, total)
        pct = int((current / max(1, total)) * 100)
        elapsed = format_duration(now - job.started_at)
        text = (
            f"🔄 <b>Выполняется экспорт Telegram</b>\n"
            f"────────────────────────\n"
            f"📱 Аккаунт: <code>{phone}</code>\n"
            f"📊 Прогресс: <code>[{bar}] {pct}%</code> ({current} из {total})\n"
            f"💬 Текущий чат: <b>{exporter.escape_html(chat_title)}</b>\n"
            f"⚙️ Этап: <i>{exporter.escape_html(job.stage)}</i>\n"
            f"⚡ Потоков: <code>{workers}</code> · ⏱ Прошло: <code>{elapsed}</code>"
        )
        if job.status_msg_id:
            try:
                # Таймаут 2 сек, чтобы FloodWait на редактирование сообщения НИКОГДА не вешал экспорт!
                await asyncio.wait_for(
                    BOT.edit_message(
                        job.admin_id,
                        job.status_msg_id,
                        text,
                        parse_mode="html",
                        buttons=[
                            [Button.inline("🛑 Остановить экспорт", f"cancel_job:{phone}".encode())],
                            [Button.inline("🏠 Главное меню", b"home")],
                        ],
                    ),
                    timeout=2.0,
                )
            except (RPCError, OSError, TimeoutError) as exc:
                log.debug("Не удалось обновить статус экспорта (%s)", type(exc).__name__)

    async def worker():
        try:
            previous_flood_callback = getattr(
                w.client, "_exporter_flood_wait_callback", None
            )

            async def report_flood_wait(operation: str, wait_seconds: int):
                await on_progress(
                    job.current_idx,
                    job.total_chats,
                    job.current_chat,
                    f"Ограничение Telegram: ожидание {wait_seconds} сек. ({operation})",
                    force=True,
                )

            w.client._exporter_flood_wait_callback = report_flood_wait
            try:
                await exporter.run_export(
                    w.client, w.dialogs, args, progress_cb=on_progress
                )
            finally:
                if previous_flood_callback is None:
                    with contextlib.suppress(AttributeError):
                        del w.client._exporter_flood_wait_callback
                else:
                    w.client._exporter_flood_wait_callback = previous_flood_callback

            job.stage = "Упаковка итогового ZIP-архива..."
            if job.status_msg_id:
                try:
                    await asyncio.wait_for(
                        BOT.edit_message(
                            job.admin_id,
                            job.status_msg_id,
                            f"🗜 <b>Экспорт чатов завершён!</b>\n"
                            f"📱 Аккаунт: <code>{phone}</code>\n"
                            f"⏳ Упаковываю итоговый ZIP-архив...",
                            parse_mode="html",
                        ),
                        timeout=3.0,
                    )
                except (RPCError, OSError, TimeoutError) as exc:
                    log.debug("Не удалось обновить статус упаковки (%s)", type(exc).__name__)

            for old_zip in account_dir(phone).glob("export_full_*.zip"):
                old_zip.unlink(missing_ok=True)

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_archive = account_dir(phone) / f"export_full_{stamp}.zip"

            file_count = await asyncio.to_thread(create_zip_sync, output_dir, full_archive, True)
            size_mb = full_archive.stat().st_size / (1024 * 1024)
            elapsed = format_duration(time.time() - job.started_at)

            # Освобождаем задачу ДО отправки большого файла в Telegram, чтобы она не висела в "Активных"!
            ACTIVE_JOBS.pop(phone, None)

            summary = (
                f"✅ <b>Экспорт успешно завершён!</b>\n"
                f"────────────────────────\n"
                f"📱 <b>Аккаунт:</b> <code>{phone}</code>\n"
                f"🎯 <b>Объект:</b> {exporter.escape_html(target_title)}\n"
                f"📦 <b>ZIP-архив готов:</b> <code>{size_mb:.1f} МБ</code>\n"
                f"📂 <b>Файлов в архиве:</b> <code>{file_count}</code>\n"
                f"⏱ <b>Затрачено времени:</b> <code>{elapsed}</code>"
            )
            if full_archive.stat().st_size <= TELEGRAM_UPLOAD_LIMIT:
                summary += "\n\nНажми кнопку ниже, чтобы скачать архив."
                result_buttons = [
                    [Button.inline("📥 Скачать полный ZIP", f"dl_zip:{phone}".encode())],
                    [Button.inline("🏠 Главное меню", b"home")],
                ]
            else:
                summary += (
                    "\n\nАрхив сохранён на сервере, но превышает лимит отправки через Telegram. "
                    "HTML-экспорт и ZIP отдельных чатов доступны в разделе «Файлы и архивы»."
                )
                result_buttons = home_buttons()
            await BOT.send_message(
                job.admin_id,
                summary,
                parse_mode="html",
                buttons=result_buttons,
            )
        except asyncio.CancelledError:
            await BOT.send_message(
                job.admin_id,
                f"🛑 Экспорт для аккаунта <code>{phone}</code> был остановлен пользователем.",
                parse_mode="html",
                buttons=home_buttons(),
            )
            raise
        except TimeoutError:
            log.warning("Экспорт остановлен по таймауту Telegram")
            await BOT.send_message(
                job.admin_id,
                "⌛ <b>Экспорт остановлен: Telegram не ответил вовремя.</b>\n"
                "Попробуй запустить его позже.",
                parse_mode="html",
                buttons=home_buttons(),
            )
        except exporter.FloodWaitLimitError as exc:
            log.warning(
                "Экспорт остановлен из-за FloodWait (operation=%s, wait=%s сек., attempts=%s)",
                exc.operation,
                exc.wait_seconds,
                exc.attempts,
            )
            await BOT.send_message(
                job.admin_id,
                f"⏳ <b>Экспорт остановлен ограничением Telegram</b>\n"
                f"Операция: {exporter.escape_html(exc.operation)}\n"
                f"Подожди {exc.wait_seconds} сек. и запусти экспорт заново.\n"
                f"Частичные файлы могут остаться в папке экспорта.",
                parse_mode="html",
                buttons=home_buttons(),
            )
        except Exception as exc:  # noqa: BLE001 - worker reports unexpected export failures
            log.error("Критическая ошибка во время экспорта (%s)", type(exc).__name__)
            await BOT.send_message(
                job.admin_id,
                f"❌ <b>Ошибка при экспорте {phone}:</b>\n<code>{type(exc).__name__}: {exc}</code>",
                parse_mode="html",
                buttons=home_buttons(),
            )
        finally:
            ACTIVE_JOBS.pop(phone, None)

    job.task = asyncio.create_task(worker())


# =====================================================================
# ТОЧКА ВХОДА
# =====================================================================

async def main():
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_stale_qr_dirs(ACCOUNTS_DIR)

    exporter.prevent_sleep()
    try:
        await BOT.start(bot_token=CONFIG["bot_token"])
        me = await BOT.get_me()
        log.info("Бот запущен: @%s (ID: %s)", me.username, me.id)
        await BOT.run_until_disconnected()
    finally:
        for uid in list(AUTH_STATES.keys()):
            await safe_disconnect_auth(uid)
        for client in ACCOUNT_CLIENTS.values():
            if client.is_connected():
                await client.disconnect()
        exporter.allow_sleep()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Бот остановлен пользователем.")
