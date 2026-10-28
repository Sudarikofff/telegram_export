#!/usr/bin/env python3
import argparse
import asyncio
import html
import inspect
import json
import re
import subprocess
import sys
import weakref
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

__version__ = '1.0.0'

SESSION_DEFAULT = 'session'
CONFIG_FILE = Path('config.json')
ACCOUNTS_DIR = Path('accounts')

# Настройки производительности (строго 1, 2, 4 или 8 потоков)
VALID_WORKERS = (1, 2, 4, 8)
DEFAULT_WORKERS = 4
FAST_DOWNLOAD_THRESHOLD = 1024 * 1024  # Использование FastTelethon для файлов > 1 МБ
BATCH_IDS_SIZE = 100                   # Размер пачки ID для батч-запросов


@dataclass(frozen=True)
class FloodWaitPolicy:
    max_wait_seconds: int
    max_retries: int


READ_FLOOD_POLICY = FloodWaitPolicy(max_wait_seconds=120, max_retries=2)
MEDIA_FLOOD_POLICY = FloodWaitPolicy(max_wait_seconds=300, max_retries=1)
_FLOOD_WAIT_LOCKS = weakref.WeakKeyDictionary()


class FloodWaitLimitError(RuntimeError):
    def __init__(self, operation: str, wait_seconds: int, attempts: int):
        self.operation = operation
        self.wait_seconds = wait_seconds
        self.attempts = attempts
        super().__init__(
            f"Операция «{operation}» остановлена ограничением Telegram "
            f"({wait_seconds} сек., попыток: {attempts})."
        )


async def gather_cancel_on_error(*awaitables):
    tasks = [asyncio.ensure_future(awaitable) for awaitable in awaitables]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _download_with_limits(size, download, workers_sem, heavy_sem):
    if size >= FAST_DOWNLOAD_THRESHOLD:
        async with heavy_sem, workers_sem:
            return await download()
    async with workers_sem:
        return await download()


def clamp_workers(val: int) -> int:
    """Приводит количество потоков к ближайшему разрешённому из (1, 2, 4, 8)."""
    try:
        v = int(val)
    except (TypeError, ValueError):
        return DEFAULT_WORKERS
    if v in VALID_WORKERS:
        return v
    return min(VALID_WORKERS, key=lambda x: abs(x - v))


def normalize_phone(phone: str) -> str:
    """Return a filesystem-safe folder name based on the entered phone number."""
    phone = (phone or '').strip()
    digits = re.sub(r'\D', '', phone)
    if not digits:
        raise ValueError('Номер должен содержать цифры.')
    return '+' + digits


def ensure_telethon(allow_install: bool = True) -> bool:
    try:
        import telethon
        has_telethon = True
    except ImportError:
        has_telethon = False

    if not has_telethon:
        if not allow_install:
            print('Telethon не установлен.')
            return False
        if sys.prefix == sys.base_prefix:
            print('Telethon не установлен. Запустите ./install.sh, затем ./run.sh.')
            return False
        print('Устанавливаю Telethon и cryptg (для максимальной скорости)...')
        try:
            subprocess.run(
                [sys.executable, '-m', 'pip', 'install', 'Telethon==1.45.0', 'cryptg'],
                check=True,
            )
            import telethon  # noqa: F401
        except (subprocess.CalledProcessError, OSError, ImportError) as error:
            print(f'Ошибка установки: {error}')
            return False

    try:
        import cryptg  # noqa: F401
    except ImportError:
        if allow_install and sys.prefix != sys.base_prefix:
            try:
                subprocess.run(
                    [sys.executable, '-m', 'pip', 'install', 'cryptg'],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except OSError:
                return True
    return True


async def fast_download_file(client, message, dest_path: Path) -> str:
    """
    Быстрая загрузка файла через внешний FastTelethon (если есть)
    или встроенный ParallelTransferrer, с безопасным фоллбэком на Telethon.
    """
    from telethon import utils
    from telethon.errors import FloodWaitError
    from telethon.tl.types import Document

    media = getattr(message, 'media', None)
    document = getattr(media, 'document', None)

    try:
        import FastTelethon as ext_ft  # type: ignore
        if isinstance(document, Document) and hasattr(ext_ft, 'download_file'):
            with open(dest_path, 'wb') as out_file:  # noqa: ASYNC230 - external downloader requires a file object
                res = ext_ft.download_file(client, document, out_file)
                if inspect.isawaitable(res):
                    await res
            return str(dest_path)
    except ImportError:
        pass
    except FloodWaitError as error:
        raise FloodWaitLimitError(
            "загрузка медиа", int(getattr(error, "seconds", 1) or 1), 1
        ) from error
    except Exception:  # noqa: BLE001, S110 - optional external downloader falls back to Telethon
        pass

    size = media_size_of(message)
    if isinstance(document, Document) and size >= FAST_DOWNLOAD_THRESHOLD:
        try:
            dc_id, location = utils.get_input_location(document)
            from telethon_parallel_downloader import ParallelTransferrer

            transferrer = ParallelTransferrer(client, dc_id=dc_id)
            with open(dest_path, 'wb') as out_file:  # noqa: ASYNC230 - streamed writer needs a binary file object
                async for chunk in transferrer.download(location, size):
                    out_file.write(chunk)
            return str(dest_path)
        except FloodWaitError as error:
            if dest_path.exists():
                try:
                    dest_path.unlink()
                except OSError:
                    pass
            raise FloodWaitLimitError(
                "параллельная загрузка медиа",
                int(getattr(error, "seconds", 1) or 1),
                1,
            ) from error
        except Exception:  # noqa: BLE001 - fall back to public download_media
            if dest_path.exists():
                try:
                    dest_path.unlink()
                except OSError:
                    pass

    path = await flood_safe(
        client.download_media,
        message,
        file=str(dest_path),
        client=client,
        operation="загрузка медиа",
        policy=MEDIA_FLOOD_POLICY,
    )
    return str(path) if path else ''


# =====================================================================
# СЛОВАРИ И ЛОКАЛИЗАЦИЯ
# =====================================================================

LANGS = {
    'ru': {
        'index_title': 'Экспортированные данные',
        'first_name': 'Имя',
        'last_name': 'Фамилия',
        'phone': 'Номер телефона',
        'username': 'Имя пользователя',
        'user_id': 'Telegram ID',
        'bio': 'О себе',
        'chats': 'Список чатов',
        'contacts': 'Контакты',
        'messages': 'Сообщений',
        'media_files': 'Медиафайлов',
        'period': 'Период',
        'exported_at': 'Экспортировано',
        'messages_word': 'сообщений',
        'chats_list': 'Список чатов',
        'contacts_title': 'Контакты',
        'back': '← Все чаты',
        'to_index': '← На главную',
        'my_comment': 'Мой комментарий',
        'reply_to': 'Ответ на сообщение',
        'my_reaction': 'Реакции',
        'not_included': 'Не включено или превышает лимит размера.',
        'no_text': '[без текста]',
        'chat_private': 'Личный чат',
        'chat_group': 'Группа',
        'chat_channel': 'Канал',
        'unknown_sender': 'Неизвестный отправитель',
        'media_photo': 'Фото',
        'media_video': 'Видео',
        'media_voice': 'Голосовое сообщение',
        'media_audio': 'Аудиофайл',
        'media_sticker': 'Стикер',
        'media_gif': 'Анимация',
        'media_document': 'Файл',
        'media_file': 'Файл',
    },
    'en': {
        'index_title': 'Exported Data',
        'first_name': 'First name',
        'last_name': 'Last name',
        'phone': 'Phone number',
        'username': 'Username',
        'user_id': 'Telegram ID',
        'bio': 'Bio',
        'chats': 'Chats',
        'contacts': 'Contacts',
        'messages': 'Messages',
        'media_files': 'Media files',
        'period': 'Period',
        'exported_at': 'Exported at',
        'messages_word': 'messages',
        'chats_list': 'Chats',
        'contacts_title': 'Contacts',
        'back': '← All chats',
        'to_index': '← Home',
        'my_comment': 'My comment',
        'reply_to': 'Reply to',
        'my_reaction': 'Reactions',
        'not_included': 'Not included or exceeds size limit.',
        'no_text': '[no text]',
        'chat_private': 'Private chat',
        'chat_group': 'Group',
        'chat_channel': 'Channel',
        'unknown_sender': 'Unknown sender',
        'media_photo': 'Photo',
        'media_video': 'Video',
        'media_voice': 'Voice message',
        'media_audio': 'Audio file',
        'media_sticker': 'Sticker',
        'media_gif': 'Animation',
        'media_document': 'File',
        'media_file': 'File',
    },
}

MEDIA_KEYS = {
    'photo': 'media_photo',
    'video': 'media_video',
    'voice': 'media_voice',
    'audio': 'media_audio',
    'sticker': 'media_sticker',
    'gif': 'media_gif',
    'document': 'media_document',
    'file': 'media_file',
}


def t(key: str, lang: str = 'ru') -> str:
    table = LANGS.get(lang, LANGS['ru'])
    return table.get(key, LANGS['ru'].get(key, key))


MONTHS_RU = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
             'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
MONTHS_EN = ['January', 'February', 'March', 'April', 'May', 'June',
             'July', 'August', 'September', 'October', 'November', 'December']


def safe_filename(name: str) -> str:
    name = name or 'Без названия'
    name = re.sub('[<>:"/\\\\|?*\x00-\x1f]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip().strip('.')
    return name[:150] or 'Без названия'


def fmt_num(value) -> str:
    try:
        return f'{int(value):,}'.replace(',', ' ')
    except (TypeError, ValueError):
        return '0'


def format_size(size: int) -> str:
    try:
        size = int(size or 0)
    except (TypeError, ValueError):
        size = 0
    if size < 1024:
        return f'{size} B'
    if size < 1024 ** 2:
        return f'{size / 1024:.1f} KB'
    if size < 1024 ** 3:
        return f'{size / 1024 ** 2:.1f} MB'
    return f'{size / 1024 ** 3:.1f} GB'


def format_export_date(value, lang: str = 'ru') -> str:
    if value is None:
        return ''
    if lang == 'en':
        return f'{value.day} {MONTHS_EN[value.month - 1]} {value.year}'
    return f'{value.day} {MONTHS_RU[value.month - 1]} {value.year}'


def format_export_time(value) -> str:
    if value is None:
        return ''
    return value.strftime('%H:%M')


def format_export_datetime(value, lang: str = 'ru') -> str:
    if value is None:
        return ''
    date = value.astimezone() if getattr(value, 'tzinfo', None) else value
    return f'{format_export_date(date, lang)} {format_export_time(date)}'


def local_naive(value):
    if value is None:
        return None
    if getattr(value, 'tzinfo', None) is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


def escape_html(text) -> str:
    return html.escape('' if text is None else str(text), quote=True)


_URL_RE = re.compile(r'(https?://[^\s<>"\']+)', re.IGNORECASE)


def linkify_escaped(text: str) -> str:
    def repl(match):
        url = match.group(1)
        tail = ''
        while url and url[-1] in '.,;:!?)]':
            tail = url[-1] + tail
            url = url[:-1]
        if not url:
            return match.group(1)
        return (f'<a href="{url}" target="_blank" rel="noopener">{url}</a>'
                f'{escape_html(tail)}')

    return _URL_RE.sub(repl, text)


def render_text(raw) -> str:
    if not raw:
        return ''
    return linkify_escaped(escape_html(raw).replace('\n', '<br>'))


def excerpt(raw, limit: int = 200) -> str:
    text = (raw or '').replace('\n', ' ').strip()
    if len(text) > limit:
        text = text[:limit - 1].rstrip() + '…'
    return text


AVATAR_COLORS = ['#e17076', '#eda86c', '#a695e7', '#7bc862',
                 '#6ec9cb', '#65aadd', '#ee7aae']


def avatar_color(key: str) -> str:
    key = str(key or '')
    return AVATAR_COLORS[sum(ord(ch) for ch in key) % len(AVATAR_COLORS)]


def avatar_initials(name: str) -> str:
    words = [w for w in re.split(r'\s+', (name or '').strip()) if w]
    if not words:
        return '?'
    if len(words) == 1:
        return words[0][:2].upper()
    return (words[0][0] + words[1][0]).upper()


COUNT_FORMS = {
    'ru': {
        'messages': ('сообщение', 'сообщения', 'сообщений'),
        'media': ('медиафайл', 'медиафайла', 'медиафайлов'),
    },
    'en': {
        'messages': ('message', 'messages', 'messages'),
        'media': ('media file', 'media files', 'media files'),
    },
}


def plural_index(n, lang: str = 'ru') -> int:
    try:
        n = abs(int(n))
    except (TypeError, ValueError):
        return 2
    if lang != 'ru':
        return 0 if n == 1 else 2
    n10, n100 = n % 10, n % 100
    if n10 == 1 and n100 != 11:
        return 0
    if 2 <= n10 <= 4 and not 12 <= n100 <= 14:
        return 1
    return 2


def format_count(n, kind: str = 'messages', lang: str = 'ru') -> str:
    forms = COUNT_FORMS.get(lang, COUNT_FORMS['ru']).get(kind, ('', '', ''))
    return f'{fmt_num(n)} {forms[plural_index(n, lang)]}'


def render_avatar(name: str, key: str = '', size: int = 42) -> str:
    return (f'<div class="avatar" style="background:{avatar_color(key)};'
            f'width:{size}px;height:{size}px;font-size:{max(11, size // 3)}px">'
            f'{escape_html(avatar_initials(name))}</div>')


# =====================================================================
# МОДЕЛИ ДАННЫХ И HTML-РЕНДЕРИНГ
# =====================================================================

@dataclass
class MediaView:
    kind: str = 'file'
    name: str = ''
    size: int = 0
    path: str = ''
    duration: int = 0


@dataclass
class ReplyView:
    msg_id: int = 0
    author: str = ''
    text: str = ''


@dataclass
class MessageView:
    id: int = 0
    date: datetime = None
    author: str = ''
    username: str = ''
    text: str = ''
    is_outgoing: bool = False
    sender_key: str = ''
    media: MediaView = None
    reply: ReplyView = None
    reactions: list = field(default_factory=list)
    comments: list = field(default_factory=list)


def render_media(media: MediaView, lang: str = 'ru') -> str:
    if media is None:
        return ''
    kind = media.kind if media.kind in MEDIA_KEYS else 'file'
    label = t(MEDIA_KEYS[kind], lang)
    if media.path:
        rel = escape_html(media.path)
        if kind in ('photo', 'sticker', 'gif'):
            return (f'<div class="media"><a href="{rel}" target="_blank">'
                    f'<img loading="lazy" src="{rel}" alt="{escape_html(label)}"></a></div>')
        if kind == 'video':
            return f'<div class="media"><video controls preload="metadata" src="{rel}"></video></div>'
        if kind in ('voice', 'audio'):
            return f'<div class="media"><audio controls src="{rel}"></audio></div>'
        name = escape_html(media.name or Path(media.path).name)
        return (f'<div class="media"><div class="file">📎 '
                f'<a href="{rel}">{name}</a>'
                f'<div class="details">{escape_html(format_size(media.size))}</div>'
                f'</div></div>')
    details = t('not_included', lang)
    if media.size:
        details += f' | {format_size(media.size)}'
    return (f'<div class="media"><div class="file">{escape_html(label)}'
            f'<div class="details">{escape_html(details)}</div></div></div>')


def render_reply(reply: ReplyView, lang: str = 'ru') -> str:
    if reply is None:
        return ''
    author = escape_html(reply.author or t('unknown_sender', lang))
    text = escape_html(excerpt(reply.text)) if reply.text else escape_html(t('no_text', lang))
    href = f' href="#message{reply.msg_id}"' if reply.msg_id else ''
    return (f'<a class="reply-link"{href}><div class="reply"><div class="ra">{author}</div>'
            f'<div class="rt">{text}</div></div></a>')


def render_comment(comment: MessageView, lang: str = 'ru') -> str:
    text_html = f'<div class="ctext">{render_text(comment.text)}</div>' if comment.text else ''
    media_html = render_media(comment.media, lang) if comment.media else ''
    time_text = format_export_time(local_naive(comment.date))
    return f'''
<div class="my-comment">
  <div class="chead"><span>{escape_html(t('my_comment', lang))}</span><span>{escape_html(time_text)}</span></div>
  {text_html}
  {media_html}
  <div class="details">ID: {comment.id}</div>
</div>'''


def render_message(view: MessageView, lang: str = 'ru') -> str:
    username = f'@{view.username}' if view.username else ''
    time_text = format_export_time(local_naive(view.date))
    name_style = 'out' if view.is_outgoing else ''
    reply_html = render_reply(view.reply, lang)
    text_html = f'<div class="mtext">{render_text(view.text)}</div>' if view.text else ''
    media_html = render_media(view.media, lang) if view.media else ''
    reactions = ''
    if view.reactions:
        shown = ' '.join(f'<span class="react-pill">{escape_html(str(r))}</span>' for r in view.reactions if r)
        if shown:
            reactions = f'<div class="reactions-bar">{shown}</div>'
    comments_html = ''.join(render_comment(c, lang) for c in view.comments)
    return f'''
<div class="message" id="message{view.id}">
  {render_avatar(view.author, view.sender_key)}
  <div class="mbody">
    <div class="mhead">
      <span class="mname {name_style}">{escape_html(view.author or t('unknown_sender', lang))}</span>
      <span class="muser">{escape_html(username)}</span>
      <span class="mtime">{escape_html(time_text)}</span>
    </div>
    {reply_html}
    {text_html}
    {media_html}
    {reactions}
  </div>
</div>
{comments_html}'''


def render_messages(views, lang: str = 'ru') -> str:
    parts = []
    last_date = None
    for view in views:
        local = local_naive(view.date)
        if local is not None:
            date_text = format_export_date(local, lang)
            if date_text != last_date:
                last_date = date_text
                parts.append(f'<div class="date-sep"><span>{escape_html(date_text)}</span></div>')
        parts.append(render_message(view, lang))
    return '\n'.join(parts)


PAGE_CSS = """
* { box-sizing: border-box; }
body { margin: 0; background: #ffffff; color: #000000;
       font-family: "Segoe UI", Roboto, Arial, sans-serif; }
#body { max-width: 760px; margin: 0 auto; padding: 18px 18px 64px; }
a { color: #3390c8; text-decoration: none; }
a:hover { text-decoration: underline; }
.reply-link:hover { text-decoration: none; }
.page-header { padding: 14px 0 16px; border-bottom: 1px solid #e4e4e5;
               position: sticky; top: 0; background: #fff; z-index: 5; }
.page-title { font-size: 22px; font-weight: 700; margin: 0 0 6px; }
.page-sub { color: #707579; font-size: 13px; }
.back { font-size: 13px; display: inline-block; margin-bottom: 8px; }
.date-sep { text-align: center; margin: 18px 0 14px; }
.date-sep span { background: #eef1f3; color: #555658; border-radius: 12px;
                 padding: 4px 12px; font-size: 13px; font-weight: 600; }
.message { display: flex; gap: 10px; margin: 12px 0; }
.avatar { flex: 0 0 auto; border-radius: 50%; display: flex; align-items: center;
          justify-content: center; color: #fff; font-weight: 600; }
.mbody { flex: 1 1 auto; min-width: 0; }
.mhead { display: flex; align-items: baseline; gap: 8px; }
.mname { font-weight: 600; font-size: 15px; color: #d1462f; }
.mname.out { color: #3390c8; }
.muser { color: #8b9399; font-size: 12px; }
.mtime { margin-left: auto; color: #8b9399; font-size: 12px; white-space: nowrap; }
.mtext { margin-top: 2px; font-size: 15px; line-height: 1.45; word-wrap: break-word;
         overflow-wrap: anywhere; }
.reply { border-left: 3px solid #3390c8; background: #f4f6f7; padding: 5px 10px;
         border-radius: 6px; margin: 4px 0 6px; font-size: 13px; color: #444; }
.reply .ra { font-weight: 600; color: #3390c8; }
.media { margin-top: 8px; }
.media img { max-width: 100%; max-height: 520px; border-radius: 10px; display: block; }
.media video { max-width: 100%; border-radius: 10px; }
.file { display: inline-block; padding: 10px 14px; border: 1px solid #d5dadd;
        border-radius: 10px; background: #f6f8f9; font-size: 14px; }
.details { color: #707579; font-size: 12px; margin-top: 4px; }
.reactions-bar { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }
.react-pill { background: #eef4f8; color: #3390c8; border-radius: 12px;
              padding: 2px 8px; font-size: 12px; font-weight: 600; }
.my-comment { margin: -4px 0 14px 52px; max-width: 640px; padding: 10px 14px;
              background: #f4f8fb; border-left: 4px solid #3390c8;
              border-radius: 8px; }
.chead { display: flex; justify-content: space-between; color: #3390c8;
         font-weight: 700; font-size: 13px; margin-bottom: 6px; }
.ctext { font-size: 15px; line-height: 1.45; }
.index-title { font-size: 26px; font-weight: 700; margin: 18px 0 18px; }
.profile { display: flex; flex-wrap: wrap; gap: 26px; padding-bottom: 20px;
           border-bottom: 1px solid #e4e4e5; }
.pcol { min-width: 180px; }
.plabel { color: #8b9399; font-size: 12px; margin-top: 10px; }
.pvalue { font-size: 17px; font-weight: 600; }
.pavatar img, .pavatar .avatar { width: 84px; height: 84px; border-radius: 50%; }
.stat-row { display: flex; align-items: center; gap: 12px; padding: 13px 4px;
            border-bottom: 1px solid #eef0f1; font-size: 15px; }
.stat-ico { width: 26px; text-align: center; font-size: 18px; }
.stat-name { font-weight: 700; }
.stat-val { margin-left: auto; color: #707579; font-size: 15px; }
h2 { font-size: 19px; margin: 30px 0 12px; }
.chat-row { display: flex; align-items: center; gap: 12px; padding: 11px 6px;
            border-bottom: 1px solid #eef0f1; color: inherit; }
.chat-row:hover { background: #f6f8fa; text-decoration: none; }
.ctitle { font-weight: 600; font-size: 15px; color: #000; }
.cmeta { color: #8b9399; font-size: 12px; margin-top: 2px; }
.contacts-table { width: 100%; border-collapse: collapse; margin-top: 8px; }
.contacts-table th { text-align: left; padding: 10px 8px; border-bottom: 2px solid #e4e4e5;
                     color: #707579; font-size: 12px; text-transform: uppercase; letter-spacing: 0.4px; }
.contacts-table td { padding: 10px 8px; border-bottom: 1px solid #eef0f1;
                     font-size: 14px; vertical-align: middle; }
.tg-id { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 13px;
         color: #495057; background: #f1f3f5; padding: 3px 8px; border-radius: 6px;
         display: inline-block; }
@media (max-width: 620px) {
  .my-comment { margin-left: 12px; }
  .muser { display: none; }
}
"""

PAGE_JS = """
function openMedia(src) {
  var win = window.open('');
  if (!win) { return; }
  win.document.write('<html><head><title>Media</title></head>' +
    '<body style="margin:0;background:#111;display:flex;align-items:center;' +
    'justify-content:center;height:100vh">' +
    '<img src="' + src + '" style="max-width:100%;max-height:100vh"></body></html>');
  win.document.close();
}
"""


def build_chat_page(title, body_html, *, chat_type='', count=0, media_count=0,
                    date_range='', exported_at='', lang='ru',
                    back_href='../../index.html') -> str:
    title_esc = escape_html(title)
    sub_bits = []
    if chat_type:
        sub_bits.append(escape_html(chat_type))
    sub_bits.append(format_count(count, 'messages', lang))
    if media_count:
        sub_bits.append(format_count(media_count, 'media', lang))
    if date_range:
        sub_bits.append(escape_html(date_range))
    if exported_at:
        sub_bits.append(escape_html(f'{t("exported_at", lang)}: {exported_at}'))
    sub = ' · '.join(sub_bits)
    return f'''<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title_esc}</title>
<style>{PAGE_CSS}</style>
</head>
<body>
<div id="body">
  <div class="page-header">
    <a class="back" href="{escape_html(back_href)}">{escape_html(t('back', lang))}</a>
    <div class="page-title">{title_esc}</div>
    <div class="page-sub">{sub}</div>
  </div>
{body_html}
</div>
<script>{PAGE_JS}</script>
</body>
</html>
'''


def build_index_page(profile, stats, chats, lang='ru') -> str:
    first = escape_html(profile.get('first_name') or '')
    last = escape_html(profile.get('last_name') or '')
    phone = escape_html(profile.get('phone') or '')
    user_id = escape_html(str(profile.get('id') or ''))
    username = profile.get('username') or ''
    username_esc = escape_html(f'@{username}' if username else '')
    bio = escape_html(profile.get('bio') or '')
    avatar = profile.get('avatar') or ''
    if avatar:
        avatar_html = f'<div class="pavatar"><img src="{escape_html(avatar)}" alt="avatar"></div>'
    else:
        full_name = f"{profile.get('first_name') or ''} {profile.get('last_name') or ''}".strip()
        avatar_html = f'<div class="pavatar">{render_avatar(full_name, profile.get("phone") or full_name, 84)}</div>'

    bio_html = ''
    if bio:
        bio_html = f'<div class="plabel">{escape_html(t("bio", lang))}</div><div class="pvalue">{bio}</div>'

    rows = [
        ('💬', t('chats', lang), fmt_num(stats.get('chats', 0)), '#chats'),
        ('👤', t('contacts', lang), fmt_num(stats.get('contacts', 0)), 'contacts.html'),
        ('✉️', t('messages', lang), fmt_num(stats.get('messages', 0)), '#chats'),
    ]
    if stats.get('media'):
        rows.append(('🖼', t('media_files', lang), fmt_num(stats.get('media', 0)), ''))
    if stats.get('date_range'):
        rows.append(('📅', t('period', lang), stats.get('date_range', ''), ''))
    if stats.get('exported_at'):
        rows.append(('🕐', t('exported_at', lang), stats.get('exported_at', ''), ''))

    stat_html = []
    for ico, name, value, href in rows:
        inner = (f'<div class="stat-ico">{ico}</div>'
                 f'<div class="stat-name">{escape_html(name)}</div>'
                 f'<div class="stat-val">{escape_html(str(value))}</div>')
        if href:
            stat_html.append(f'<a class="stat-row" href="{escape_html(href)}">{inner}</a>')
        else:
            stat_html.append(f'<div class="stat-row">{inner}</div>')

    chat_rows = []
    for chat in chats:
        title_esc = escape_html(chat.get('title') or '')
        type_esc = escape_html(chat.get('chat_type') or '')
        count = int(chat.get('count') or 0)
        href = escape_html(chat.get('href') or '')
        chat_rows.append(f'''<a class="chat-row" href="{href}">
    {render_avatar(chat.get('title') or '?', chat.get('title') or '')}
    <div>
      <div class="ctitle">{title_esc}</div>
      <div class="cmeta">{type_esc} · {format_count(count, 'messages', lang)}</div>
    </div>
  </a>''')

    return f'''<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape_html(t('index_title', lang))}</title>
<style>{PAGE_CSS}</style>
</head>
<body>
<div id="body">
  <div class="index-title">{escape_html(t('index_title', lang))}</div>

  <div class="profile">
    <div class="pcol">
      <div class="plabel">{escape_html(t('first_name', lang))}</div>
      <div class="pvalue">{first or '—'}</div>
      <div class="plabel">{escape_html(t('last_name', lang))}</div>
      <div class="pvalue">{last or '—'}</div>
      {bio_html}
    </div>
    <div class="pcol">
      <div class="plabel">{escape_html(t('phone', lang))}</div>
      <div class="pvalue">{phone or '—'}</div>
      <div class="plabel">{escape_html(t('username', lang))}</div>
      <div class="pvalue">{username_esc or '—'}</div>
      <div class="plabel">{escape_html(t('user_id', lang))}</div>
      <div class="pvalue">{user_id or '—'}</div>
    </div>
    {avatar_html}
  </div>

  <div class="stats">
    {''.join(stat_html)}
  </div>

  <h2 id="chats">{escape_html(t('chats_list', lang))} ({len(chats)})</h2>
  {''.join(chat_rows)}

</div>
</body>
</html>
'''


def build_contacts_page(profile, contacts, lang='ru') -> str:
    rows = []
    for person in contacts:
        name = (f"{person.get('first_name') or ''} {person.get('last_name') or ''}".strip()
                or person.get('username') or '—')
        username = person.get('username') or ''
        tg_id = person.get('id')
        tg_id_str = str(tg_id) if tg_id else '—'
        rows.append(f'''<tr>
    <td style="width:52px">{render_avatar(name, str(tg_id or person.get('phone') or name), 42)}</td>
    <td><strong>{escape_html(name)}</strong></td>
    <td><span class="tg-id">{escape_html(tg_id_str)}</span></td>
    <td>{escape_html(person.get('phone') or '—')}</td>
    <td>{escape_html(f'@{username}' if username else '—')}</td>
  </tr>''')

    return f'''<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape_html(t('contacts_title', lang))}</title>
<style>{PAGE_CSS}</style>
</head>
<body>
<div id="body">
  <div class="page-header">
    <a class="back" href="index.html">{escape_html(t('to_index', lang))}</a>
    <div class="page-title">{escape_html(t('contacts_title', lang))} ({len(contacts)})</div>
    <div class="page-sub">{escape_html(profile.get('phone') or '')} {escape_html(f"@{profile.get('username')}" if profile.get('username') else '')}</div>
  </div>
  <table class="contacts-table">
    <thead>
      <tr>
        <th></th>
        <th>{escape_html(t('first_name', lang))}</th>
        <th>{escape_html(t('user_id', lang))}</th>
        <th>{escape_html(t('phone', lang))}</th>
        <th>{escape_html(t('username', lang))}</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
</div>
</body>
</html>
'''


# =====================================================================
# ОБРАБОТКА СООБЩЕНИЙ И МЕДИА
# =====================================================================

def extract_reactions(message) -> list[str]:
    """Извлекает список эмодзи-реакций с количеством из объекта Message."""
    reactions_obj = getattr(message, 'reactions', None)
    if not reactions_obj:
        return []
    results = getattr(reactions_obj, 'results', None) or []
    out = []
    for item in results:
        reaction = getattr(item, 'reaction', None)
        emoticon = getattr(reaction, 'emoticon', None)
        if not emoticon and getattr(reaction, 'document_id', None):
            emoticon = '⭐'
        count = getattr(item, 'count', 1) or 1
        if emoticon:
            out.append(f'{emoticon} {count}' if count > 1 else emoticon)
    return out


def collect_my_messages(messages, my_id):
    selected = {}
    parent_ids = set()
    for message in messages:
        if getattr(message, 'sender_id', None) != my_id:
            continue
        selected[getattr(message, 'id', 0)] = message
        reply_to = getattr(message, 'reply_to', None)
        parent_id = getattr(reply_to, 'reply_to_msg_id', None) if reply_to else None
        if parent_id:
            parent_ids.add(parent_id)
    return selected, parent_ids


def build_message_view(message, *, my_id=0, reply=None, media=None,
                       reactions=None, comments=None, lang='ru') -> MessageView:
    sender = getattr(message, 'sender', None)
    first = getattr(sender, 'first_name', '') or ''
    last = getattr(sender, 'last_name', '') or ''
    title = getattr(sender, 'title', '') or ''
    name = f'{first} {last}'.strip() or title
    if not name:
        name = getattr(message, 'sender_id', None)
        name = f'user{getattr(message, "sender_id", 0)}' if name else t('unknown_sender', lang)
    username = getattr(sender, 'username', None) or ''
    sender_id = getattr(message, 'sender_id', None) or 0
    extracted_reactions = reactions if reactions is not None else extract_reactions(message)
    return MessageView(
        id=getattr(message, 'id', 0),
        date=local_naive(getattr(message, 'date', None)),
        author=name,
        username=username,
        text=getattr(message, 'message', None) or getattr(message, 'text', None) or '',
        is_outgoing=bool(getattr(message, 'out', False)) or (my_id and sender_id == my_id),
        sender_key=str(sender_id or name),
        media=media,
        reply=reply,
        reactions=list(extracted_reactions),
        comments=list(comments or []),
    )


def get_media_type(message) -> str:
    media = getattr(message, 'media', None)
    if media is None:
        return ''
    if getattr(message, 'photo', None) is not None:
        return 'photo'
    if getattr(message, 'sticker', None) is not None:
        return 'sticker'
    if getattr(message, 'gif', None) is not None:
        return 'gif'
    if getattr(message, 'video', None) is not None:
        return 'video'
    if getattr(message, 'voice', None) is not None:
        return 'voice'
    if getattr(message, 'audio', None) is not None:
        return 'audio'
    if getattr(message, 'document', None) is not None:
        return 'document'
    return 'file'


def media_size_of(message) -> int:
    media = getattr(message, 'media', None)
    if media is None:
        return 0
    document = getattr(media, 'document', None)
    size = getattr(document, 'size', None)
    if size is None:
        size = getattr(media, 'size', None)
    if size is None:
        file_obj = getattr(message, 'file', None)
        size = getattr(file_obj, 'size', None)
    try:
        return int(size or 0)
    except (TypeError, ValueError):
        return 0


def media_name_of(message) -> str:
    media = getattr(message, 'media', None)
    document = getattr(media, 'document', None) if media is not None else None
    for attr in ('file_name',):
        for obj in (document, media):
            name = getattr(obj, attr, None) if obj is not None else None
            if name:
                return str(name)
    file_obj = getattr(message, 'file', None)
    if file_obj and getattr(file_obj, 'name', None):
        return str(file_obj.name)
    return ''


def guess_media_filename(message) -> str:
    msg_id = getattr(message, 'id', 0)
    orig_name = media_name_of(message)
    if orig_name:
        return f'{msg_id}_{safe_filename(orig_name)}'
    file_obj = getattr(message, 'file', None)
    ext = getattr(file_obj, 'ext', '') or ''
    if not ext:
        kind = get_media_type(message)
        ext = {
            'photo': '.jpg',
            'video': '.mp4',
            'voice': '.ogg',
            'audio': '.mp3',
            'sticker': '.webp',
            'gif': '.mp4',
        }.get(kind, '.bin')
    return f'{msg_id}{ext}'


def build_media_view(message, rel_path: str = '') -> MediaView:
    kind = get_media_type(message) or 'file'
    return MediaView(kind=kind, name=media_name_of(message),
                     size=media_size_of(message), path=rel_path)


# =====================================================================
# АВТОРИЗАЦИЯ И КОНФИГУРАЦИЯ
# =====================================================================

def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f'Не найден {CONFIG_FILE}. Добавь туда api_id и api_hash.'
        )

    try:
        config = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
    except json.JSONDecodeError as error:
        raise ValueError(f'В {CONFIG_FILE} сломан JSON: {error}') from error

    if not isinstance(config, dict):
        raise TypeError(f'В {CONFIG_FILE} должен быть JSON-объект.')

    return config


def resolve_credentials(config: dict):
    if 'api_id' not in config or 'api_hash' not in config:
        raise ValueError(f'В {CONFIG_FILE} нужны api_id и api_hash.')

    api_id = config.get('api_id')
    api_hash = config.get('api_hash')

    try:
        api_id = int(api_id)
    except (TypeError, ValueError) as error:
        raise ValueError(f'api_id в {CONFIG_FILE} должен быть числом.') from error

    if api_id <= 0:
        raise ValueError(f'api_id в {CONFIG_FILE} должен быть больше нуля.')

    if not isinstance(api_hash, str) or not api_hash.strip():
        raise ValueError(f'api_hash в {CONFIG_FILE} не должен быть пустым.')

    return api_id, api_hash.strip()


def resolve_proxy(config: dict):
    proxy_config = config.get('proxy') or {}
    if not proxy_config.get('enabled', False):
        return None
    proxy_type = str(proxy_config.get('type', 'socks5')).lower()
    if proxy_type not in ('socks5', 'socks4', 'http'):
        print('Прокси бывает socks5, socks4 или http. Отключаю его.')
        return None
    host = proxy_config.get('host')
    try:
        port = int(proxy_config.get('port'))
    except (TypeError, ValueError):
        port = 0
    if not host or not port:
        print('Для прокси нужны host и port.')
        return None
    return {
        'proxy_type': proxy_type,
        'addr': host,
        'port': port,
        'username': proxy_config.get('username') or None,
        'password': proxy_config.get('password') or None,
    }


async def login(config, session_name, phone_arg=''):
    from telethon.errors import (
        AuthRestartError,
        FloodWaitError,
        PasswordHashInvalidError,
        PhoneCodeExpiredError,
        PhoneCodeInvalidError,
        SessionPasswordNeededError,
    )

    from telethon_client import create_telegram_client

    api_id, api_hash = resolve_credentials(config)
    proxy = resolve_proxy(config)

    if proxy:
        print(f"Proxy: {proxy['proxy_type']}://{proxy['addr']}:{proxy['port']}")
    else:
        print('Прокси: выкл.')

    client = create_telegram_client(session_name, api_id, api_hash, proxy=proxy)
    client.flood_sleep_threshold = 0

    try:
        await client.connect()

        if await client.is_user_authorized():
            print(f'Сессия уже есть: {client.session.filename}')
            return client

        phone = phone_arg or config.get('phone') or ''
        if not phone:
            phone = input('Номер Telegram: ').strip()

        if not phone:
            print('Номер не указан.')
            await client.disconnect()
            return None

        code_sent = False
        for attempt in range(1, 4):
            try:
                print(f'Запрашиваю код ({attempt}/3)...')
                await client.send_code_request(phone)
                print('Код запрошен.')
                code_sent = True
                break
            except AuthRestartError:
                print('Telegram просит начать вход заново.')
                if attempt >= 3:
                    print('Не получилось запросить код')
                    await client.disconnect()
                    return None
                await client.disconnect()
                client = create_telegram_client(
                    session_name, api_id, api_hash, proxy=proxy
                )
                client.flood_sleep_threshold = 0
                await client.connect()

        if not code_sent:
            await client.disconnect()
            return None

        code = input('Код из Telegram: ').strip()
        if not code:
            print('Код не указан.')
            await client.disconnect()
            return None

        try:
            print('Проверяю...')
            await client.sign_in(phone=phone, code=code)
            print('Код подошёл.')
        except PhoneCodeInvalidError:
            print('Код неверный.')
            await client.disconnect()
            return None
        except PhoneCodeExpiredError:
            print('Код уже истёк. Запроси новый.')
            await client.disconnect()
            return None
        except SessionPasswordNeededError:
            print('Нужен 2FA.')
            password = input('Пароль 2FA: ').strip()
            if not password:
                print('Пароль не указан.')
                await client.disconnect()
                return None

            print('Проверяю пароль...')
            try:
                await asyncio.wait_for(client.sign_in(password=password), timeout=30)
            except asyncio.TimeoutError:
                print('Telegram не ответил за 30 секунд.')
                await client.disconnect()
                return None
            except PasswordHashInvalidError:
                print('Пароль 2FA неверный.')
                await client.disconnect()
                return None
            except FloodWaitError as error:
                wait = int(getattr(error, 'seconds', 0) or 0)
                print(f'Telegram ограничил вход. Подожди {wait} сек.')
                await client.disconnect()
                return None
            except Exception as error:  # noqa: BLE001 - preserve login cleanup and report unexpected failures
                print(f'Не получилось проверить пароль: {type(error).__name__}: {error}')
                await client.disconnect()
                return None
            print('Пароль подошёл.')

        client.session.save()
        print(f'Сессия сохранена: {client.session.filename}')
        print('Вход выполнен.')
        return client

    except FloodWaitError as error:
        if client.is_connected():
            await client.disconnect()
        wait = max(1, int(getattr(error, 'seconds', 1) or 1))
        print(f'Вход остановлен ограничением Telegram. Подожди {wait} сек. и повтори попытку.')
        return None
    except Exception as error:  # noqa: BLE001 - close client and report unexpected login failures
        if client.is_connected():
            await client.disconnect()
        print(f'Не получилось войти в Telegram: {type(error).__name__}: {error}')
        return None


# =====================================================================
# СКОРОСТНОЕ ПОЛУЧЕНИЕ СООБЩЕНИЙ И МЕДИА
# =====================================================================

async def flood_safe(
    factory,
    *args,
    client=None,
    operation: str = "запрос Telegram",
    policy: FloodWaitPolicy = READ_FLOOD_POLICY,
    request_timeout: Optional[float] = None,
    **kwargs,
):
    from telethon.errors import FloodWaitError

    owner = client or getattr(factory, "__self__", None)
    if owner is not None:
        lock = _FLOOD_WAIT_LOCKS.get(owner)
        if lock is None:
            lock = asyncio.Lock()
            _FLOOD_WAIT_LOCKS[owner] = lock
    else:
        lock = asyncio.Lock()

    async def invoke():
        request = factory(*args, **kwargs)
        if request_timeout is not None:
            return await asyncio.wait_for(request, timeout=request_timeout)
        return await request

    try:
        return await invoke()
    except FloodWaitError as error:
        attempts = 1
        async with lock:
            while True:
                wait = max(1, int(getattr(error, "seconds", 1) or 1))
                if wait > policy.max_wait_seconds or attempts > policy.max_retries:
                    raise FloodWaitLimitError(operation, wait, attempts) from error

                message = f"Telegram просит подождать {wait} сек. перед повтором: {operation}."
                print(f"\n{message}")
                callback = getattr(owner, "_exporter_flood_wait_callback", None)
                if callback:
                    try:
                        result = callback(operation, wait)
                        if inspect.isawaitable(result):
                            await result
                    except Exception:  # noqa: BLE001 - status updates must not affect API retry timing
                        callback = None
                await asyncio.sleep(wait)
                attempts += 1
                try:
                    return await invoke()
                except FloodWaitError as next_error:
                    error = next_error


async def iter_messages_safe(client, entity, *, start_date, end_date,
                             from_user=None, page_size=100):
    """
    Идёт от новых сообщений к старым, завершаясь по дате начала.
    FloodWait и таймауты передаются вызывающему коду, чтобы чат не считался
    полностью выгруженным после частичного чтения.
    """
    it = client.iter_messages(
        entity,
        from_user=from_user,
        wait_time=1,
    )

    while True:
        try:
            message = await flood_safe(
                it.__anext__,
                client=client,
                operation="чтение истории сообщений",
                policy=READ_FLOOD_POLICY,
                request_timeout=45.0,
            )
        except StopAsyncIteration:
            return
        except asyncio.TimeoutError as error:
            raise TimeoutError(
                "Истекло ожидание ответа Telegram при чтении истории сообщений."
            ) from error

        local = local_naive(getattr(message, 'date', None))
        if local is not None:
            # Идём от новых к старым: если сообщение новее end_date — пропускаем
            if end_date is not None and local > end_date:
                continue
            # Как только дошли до сообщений старше start_date — сразу выходим из цикла!
            if start_date is not None and local < start_date:
                return

        yield message


async def fetch_messages_map(client, entity, ids_iterable, chunk_size: int = BATCH_IDS_SIZE) -> dict:
    unique_ids = sorted({int(i) for i in ids_iterable if i})
    result = {}
    if not unique_ids:
        return result

    for idx in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[idx:idx + chunk_size]
        fetched = await flood_safe(
            client.get_messages,
            entity,
            ids=chunk,
            client=client,
            operation="загрузка сообщений по ID",
            policy=READ_FLOOD_POLICY,
        )
        if not isinstance(fetched, list):
            fetched = [fetched] if fetched else []
        for msg in fetched:
            if msg is not None and getattr(msg, 'id', None):
                result[msg.id] = msg
    return result


async def get_dialogs(client):
    dialogs = []
    print('Загружаю список чатов...')
    iterator = client.iter_dialogs()
    while True:
        try:
            dialog = await flood_safe(
                iterator.__anext__,
                client=client,
                operation="загрузка списка диалогов",
                policy=READ_FLOOD_POLICY,
            )
        except StopAsyncIteration:
            break
        dialogs.append(dialog)
    return dialogs


def dialog_title(dialog) -> str:
    entity = dialog.entity
    title = getattr(entity, 'title', None)
    if not title:
        first = getattr(entity, 'first_name', '') or ''
        last = getattr(entity, 'last_name', '') or ''
        title = f'{first} {last}'.strip()
    return title or dialog.name or 'Без названия'


def dialog_type_label(dialog, lang='ru') -> str:
    if getattr(dialog, 'is_user', False):
        return t('chat_private', lang)
    if getattr(dialog, 'is_group', False):
        return t('chat_group', lang)
    if getattr(dialog, 'is_channel', False):
        return t('chat_channel', lang)
    return t('chat_group', lang)


def is_private_dialog(dialog) -> bool:
    return bool(getattr(dialog, 'is_user', False))


def print_dialogs(dialogs):
    print()
    print('-' * 70)
    print('Список чатов')
    print('-' * 70)
    for number, dialog in enumerate(dialogs, start=1):
        print(f'{number:4}. [{dialog_type_label(dialog)}] {dialog_title(dialog)}')
    print('-' * 70)
    print()


async def fetch_profile(client, output_dir: Path) -> dict:
    from telethon import functions
    from telethon.errors import RPCError
    me = await flood_safe(
        client.get_me, client=client, operation="загрузка профиля",
        policy=READ_FLOOD_POLICY,
    )
    profile = {
        'id': getattr(me, 'id', '') or '',
        'first_name': getattr(me, 'first_name', '') or '',
        'last_name': getattr(me, 'last_name', '') or '',
        'phone': getattr(me, 'phone', '') or '',
        'username': getattr(me, 'username', '') or '',
        'bio': '',
        'avatar': '',
    }
    if profile['phone']:
        profile['phone'] = '+' + profile['phone']
    try:
        full = await flood_safe(
            lambda: client(functions.users.GetFullUserRequest(me)),
            client=client,
            operation="загрузка подробного профиля",
            policy=READ_FLOOD_POLICY,
        )
        about = getattr(full, 'about', '') or ''
        profile['bio'] = about
    except (FloodWaitLimitError, TimeoutError):
        raise
    except RPCError:
        profile['bio'] = ''
    try:
        path = await flood_safe(
            client.download_profile_photo,
            me,
            file=str(output_dir / 'profile_photo'),
            client=client,
            operation="загрузка фото профиля",
            policy=MEDIA_FLOOD_POLICY,
        )
        if path:
            profile['avatar'] = Path(path).name
    except (FloodWaitLimitError, TimeoutError):
        raise
    except (RPCError, OSError) as error:
        print(f'Не скачался аватар: {error}')
    return profile


async def fetch_contacts(client) -> list:
    from telethon import functions
    from telethon.errors import RPCError
    contacts = []
    try:
        result = await flood_safe(
            lambda: client(functions.contacts.GetContactsRequest(hash=0)),
            client=client,
            operation="загрузка контактов",
            policy=READ_FLOOD_POLICY,
        )
        for user in getattr(result, 'users', []) or []:
            first = getattr(user, 'first_name', '') or ''
            last = getattr(user, 'last_name', '') or ''
            phone = getattr(user, 'phone', '') or ''
            contacts.append({
                'id': getattr(user, 'id', '') or '',
                'first_name': first,
                'last_name': last,
                'phone': f'+{phone}' if phone else '',
                'username': getattr(user, 'username', '') or '',
            })
    except (FloodWaitLimitError, TimeoutError):
        raise
    except (RPCError, OSError) as error:
        print(f'Не получилось загрузить контакты: {error}')
    return contacts


async def download_one_media(client, message, media_dir: Path, max_bytes=0) -> str:
    size = media_size_of(message)
    if max_bytes and size > max_bytes:
        return ''
    dest_path = None
    try:
        media_dir.mkdir(parents=True, exist_ok=True)
        dest_path = media_dir / guess_media_filename(message)
        path = await fast_download_file(client, message, dest_path)
        if path and max_bytes and Path(path).exists() and Path(path).stat().st_size > max_bytes:
            try:
                Path(path).unlink()
            except OSError:
                pass
            return ''
        return path
    except (asyncio.CancelledError, FloodWaitLimitError, TimeoutError):
        if dest_path and dest_path.exists():
            try:
                dest_path.unlink()
            except OSError:
                pass
        raise
    except Exception as error:  # noqa: BLE001 - continue after an individual media-file failure
        print(f'\nФайл у сообщения {getattr(message, "id", "?")} не скачался: {error}')
        return ''


async def get_my_channel_comments(client, entity, my_id, start_date, end_date):
    from telethon import functions
    from telethon.errors import RPCError
    result = {}
    try:
        full = await flood_safe(
            lambda: client(functions.channels.GetFullChannelRequest(channel=entity)),
            client=client,
            operation="загрузка связанного чата канала",
            policy=READ_FLOOD_POLICY,
        )
        linked_id = getattr(full.full_chat, 'linked_chat_id', None)
        if not linked_id:
            return result
        linked_chat = await flood_safe(
            client.get_entity,
            linked_id,
            client=client,
            operation="загрузка связанного чата",
            policy=READ_FLOOD_POLICY,
        )
        print('Ищу твои комменты в канале...')
        pending = []
        root_ids = set()
        async for comment in iter_messages_safe(client, linked_chat,
                                                start_date=start_date,
                                                end_date=end_date,
                                                from_user=my_id):
            reply_to = getattr(comment, 'reply_to', None)
            if not reply_to:
                continue
            root_id = getattr(reply_to, 'reply_to_top_id', None) or \
                getattr(reply_to, 'reply_to_msg_id', None)
            if root_id:
                pending.append((comment, root_id))
                root_ids.add(root_id)

        roots_map = await fetch_messages_map(client, linked_chat, root_ids)

        unresolved = {}
        for comment, root_id in pending:
            root = roots_map.get(root_id)
            post_id = extract_channel_post_id(root)
            if post_id:
                result.setdefault(post_id, []).append(comment)
            else:
                unresolved.setdefault(root_id, []).append(comment)

        if unresolved:
            print('Привязываю комменты к постам...')
            async for post in iter_messages_safe(client, entity,
                                                 start_date=start_date,
                                                 end_date=end_date):
                discussion_ids = await get_discussion_ids(client, entity, post.id)
                matched = discussion_ids & set(unresolved)
                for root_id in matched:
                    result.setdefault(post.id, []).extend(unresolved.pop(root_id))
                if not unresolved:
                    break
    except (FloodWaitLimitError, TimeoutError):
        raise
    except RPCError as error:
        print(f'Не получилось загрузить комменты: {error}')
    return result


def extract_channel_post_id(root) -> Optional[int]:
    if root is None:
        return None
    reply = getattr(root, 'reply_to', None)
    if reply:
        post_id = getattr(reply, 'reply_to_msg_id', None) or \
            getattr(reply, 'reply_to_top_id', None)
        if post_id:
            return post_id
    fwd = getattr(root, 'fwd_from', None)
    if fwd:
        post_id = getattr(fwd, 'channel_post', None)
        if post_id:
            return post_id
    return None


async def get_discussion_ids(client, channel_entity, post_id) -> set:
    from telethon import functions
    from telethon.errors import RPCError
    ids = set()
    try:
        discussion = await flood_safe(
            lambda: client(functions.messages.GetDiscussionMessageRequest(
                peer=channel_entity, msg_id=post_id)),
            client=client,
            operation="загрузка обсуждения публикации",
            policy=READ_FLOOD_POLICY,
        )
        for message in getattr(discussion, 'messages', []) or []:
            ids.add(getattr(message, 'id', None))
    except (FloodWaitLimitError, TimeoutError):
        raise
    except RPCError:
        ids.clear()
    ids.discard(None)
    return ids


# =====================================================================
# ЭКСПОРТ ЧАТОВ (С ПАРАЛЛЕЛЬНОЙ ОЧЕРЕДЬЮ ЗАГРУЗОК И БАТЧИНГОМ)
# =====================================================================

@dataclass
class ExportOptions:
    start_date: datetime = None
    end_date: datetime = None
    download_media: bool = True
    max_media_bytes: int = 0
    mode: str = 'auto'
    lang: str = 'ru'
    make_zip: bool = False
    workers: int = DEFAULT_WORKERS


def unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    counter = 2
    while True:
        candidate = path.parent / f'{path.name}_{counter}'
        if not candidate.exists():
            return candidate
        counter += 1


async def export_chat(client, dialog, output_root: Path, my_id: int,
                      opts: ExportOptions,
                      sub_progress_cb: Optional[Callable[[str], object]] = None) -> dict:
    lang = opts.lang
    entity = dialog.entity
    title = dialog_title(dialog)
    chat_type = dialog_type_label(dialog, lang)
    private = is_private_dialog(dialog)
    mode = 'all' if private else (opts.mode if opts.mode in ('all', 'mine') else 'mine')
    workers = clamp_workers(opts.workers)

    async def notify_sub(stage_text: str):
        if sub_progress_cb:
            try:
                res = sub_progress_cb(stage_text)
                if inspect.isawaitable(res):
                    await res
            except Exception:  # noqa: BLE001 - progress hooks must not stop the export
                return

    print()
    print('=' * 70)
    print(f'Чат: {title} ({chat_type}, режим: {"всё" if mode == "all" else "только твои комменты"})')
    print('=' * 70)

    chat_dir = unique_dir(output_root / 'chats' / safe_filename(title))
    media_dir = chat_dir / 'media'
    chat_dir.mkdir(parents=True, exist_ok=True)

    views = []
    media_jobs = []

    def queue_media(view: MessageView, message):
        if message is not None and getattr(message, 'media', None) is not None:
            size = media_size_of(message)
            if opts.download_media and (not opts.max_media_bytes or size <= opts.max_media_bytes):
                media_jobs.append((view, message))
            else:
                view.media = build_media_view(message, rel_path='')

    if mode == 'all':
        print('Скачиваю историю сообщений...')
        await notify_sub('Сбор истории сообщений...')
        raw_messages = []
        msg_map = {}
        missing_parent_ids = set()

        async for message in iter_messages_safe(client, entity,
                                                start_date=opts.start_date,
                                                end_date=opts.end_date):
            raw_messages.append(message)
            msg_map[message.id] = message
            if len(raw_messages) % 500 == 0:
                await notify_sub(f'Собрано сообщений: {len(raw_messages)}')

        for message in raw_messages:
            reply_to = getattr(message, 'reply_to', None)
            parent_id = getattr(reply_to, 'reply_to_msg_id', None) if reply_to else None
            if parent_id and parent_id not in msg_map:
                missing_parent_ids.add(parent_id)

        if missing_parent_ids:
            print(f'Догружаю внешние сообщения для реплаев ({len(missing_parent_ids)} шт.)...')
            await notify_sub(f'Загрузка реплаев ({len(missing_parent_ids)} шт.)...')
            fetched_parents = await fetch_messages_map(client, entity, missing_parent_ids)
            msg_map.update(fetched_parents)

        for message in raw_messages:
            view = build_message_view(message, my_id=my_id, lang=lang)
            reply_to = getattr(message, 'reply_to', None)
            parent_id = getattr(reply_to, 'reply_to_msg_id', None) if reply_to else None
            if parent_id:
                parent_msg = msg_map.get(parent_id)
                if parent_msg is not None:
                    p_view = build_message_view(parent_msg, my_id=my_id, lang=lang)
                    view.reply = ReplyView(msg_id=parent_id, author=p_view.author,
                                           text=excerpt(p_view.text))
                else:
                    view.reply = ReplyView(msg_id=parent_id, author='', text='')
            queue_media(view, message)
            views.append(view)

    else:
        if getattr(entity, 'broadcast', False):
            await notify_sub('Поиск твоих комментариев в канале...')
            comment_map = await get_my_channel_comments(client, entity, my_id,
                                                        opts.start_date, opts.end_date)
            print(f'Нашёл комментов: {sum(len(v) for v in comment_map.values())}')
            posts_map = await fetch_messages_map(client, entity, comment_map.keys())

            for post_id, comments in sorted(comment_map.items()):
                post = posts_map.get(post_id)
                if post is not None:
                    post_view = build_message_view(post, my_id=my_id, lang=lang)
                    queue_media(post_view, post)
                else:
                    post_view = MessageView(id=post_id, date=None,
                                            author=chat_type, text='',
                                            sender_key=title)
                for comment in comments:
                    comment_view = build_message_view(comment, my_id=my_id, lang=lang)
                    queue_media(comment_view, comment)
                    post_view.comments.append(comment_view)
                views.append(post_view)
        else:
            print('Собираю твои сообщения и комменты...')
            await notify_sub('Поиск твоих сообщений в группе...')
            mine = []
            async for message in iter_messages_safe(client, entity,
                                                    start_date=opts.start_date,
                                                    end_date=opts.end_date,
                                                    from_user=my_id):
                mine.append(message)
            selected, parent_ids = collect_my_messages(mine, my_id)
            print(f'Твоих сообщений: {len(selected)}')

            parents = {m_id: msg for m_id, msg in selected.items() if m_id in parent_ids}
            missing_parents = parent_ids - set(parents.keys())
            if missing_parents:
                fetched_parents = await fetch_messages_map(client, entity, missing_parents)
                parents.update(fetched_parents)

            for message in sorted(selected.values(),
                                  key=lambda m: (local_naive(getattr(m, 'date', None)) or datetime.min,
                                                 getattr(m, 'id', 0))):
                view = build_message_view(message, my_id=my_id, lang=lang)
                reply_to = getattr(message, 'reply_to', None)
                parent_id = getattr(reply_to, 'reply_to_msg_id', None) if reply_to else None
                if parent_id:
                    parent = parents.get(parent_id)
                    if parent is not None:
                        parent_view = build_message_view(parent, my_id=my_id, lang=lang)
                        view.reply = ReplyView(msg_id=parent_id,
                                               author=parent_view.author,
                                               text=excerpt(parent_view.text))
                    else:
                        view.reply = ReplyView(msg_id=parent_id, author='', text='')
                queue_media(view, message)
                views.append(view)

    media_count = 0
    if media_jobs:
        total_media = len(media_jobs)
        limit_info = f', лимит: {format_size(opts.max_media_bytes)}' if opts.max_media_bytes else ''
        print(f'Скачиваю медиафайлы ({total_media} шт.) в {workers} потоков{limit_info}...')
        await notify_sub(f'Загрузка медиа: 0/{total_media} ({workers} пот.)')
        sem = asyncio.Semaphore(workers)
        heavy_sem = asyncio.Semaphore(min(2, workers))
        completed = 0

        async def _download_worker(target_view: MessageView, msg):
            nonlocal media_count, completed
            size = media_size_of(msg)
            path = await _download_with_limits(
                size,
                lambda: download_one_media(client, msg, media_dir, opts.max_media_bytes),
                sem,
                heavy_sem,
            )
            completed += 1
            if completed % 10 == 0 or completed == total_media:
                print(f'\r  Прогресс медиа: {completed}/{total_media}', end='', flush=True)
                await notify_sub(f'Загрузка медиа: {completed}/{total_media} ({workers} пот.)')
            if path:
                rel = 'media/' + Path(path).name
                target_view.media = build_media_view(msg, rel_path=rel)
                media_count += 1
            else:
                target_view.media = build_media_view(msg, rel_path='')

        await gather_cancel_on_error(
            *(_download_worker(v, m) for v, m in media_jobs)
        )
        print()

    views.sort(key=lambda v: ((v.date or datetime.min), v.id))
    body = render_messages(views, lang)
    exported_at = datetime.now().strftime('%d.%m.%Y %H:%M')
    date_range = build_date_range(opts.start_date, opts.end_date, lang)
    page = build_chat_page(title, body, chat_type=chat_type, count=len(views),
                           media_count=media_count, date_range=date_range,
                           exported_at=exported_at, lang=lang)
    html_path = chat_dir / 'messages.html'
    html_path.write_text(page, encoding='utf-8')

    if opts.make_zip:
        await notify_sub('Упаковка ZIP-архива чата...')
        await asyncio.to_thread(zip_chat, chat_dir, output_root)

    print(f'Готово: {html_path} (сообщений: {len(views)}, файлов: {media_count})')
    return {
        'title': title,
        'chat_type': chat_type,
        'count': len(views),
        'media': media_count,
        'href': f'chats/{chat_dir.name}/messages.html',
    }


def build_date_range(start_date, end_date, lang='ru') -> str:
    left = format_export_date(start_date, lang) if start_date else '—'
    if end_date is not None:
        right = format_export_date(end_date, lang)
    else:
        right = format_export_date(datetime.now(), lang)
    return f'{left} — {right}'


def zip_chat(chat_dir: Path, output_root: Path) -> Path:
    zip_path = output_root / f'{chat_dir.name}.zip'
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED,
                         compresslevel=6) as archive:
        for item in sorted(chat_dir.rglob('*')):
            if item.is_file():
                archive.write(item, item.relative_to(output_root))
    return zip_path


# =====================================================================
# CLI И ТОЧКА ВХОДА
# =====================================================================

def ask_export_dates():
    while True:
        text = input('Дата с (ДД.ММ.ГГГГ, Enter = с самого начала): ').strip()
        if not text:
            start_date = datetime(2000, 1, 1)
            break
        try:
            start_date = datetime.strptime(text, '%d.%m.%Y')
            break
        except ValueError:
            print('Введи дату как ДД.ММ.ГГГГ.')
    while True:
        text = input('Дата по (ДД.ММ.ГГГГ, Enter = до конца): ').strip()
        if not text:
            return start_date, None
        try:
            end_date = datetime.strptime(text, '%d.%m.%Y')
            end_date = end_date.replace(hour=23, minute=59, second=59)
            if end_date < start_date:
                print('Дата «по» раньше даты «с». Проверь даты.')
                continue
            return start_date, end_date
        except ValueError:
            print('Введи дату как ДД.ММ.ГГГГ.')


def ask_yes_no(prompt, default_yes=True):
    suffix = ' [Y/n]: ' if default_yes else ' [y/N]: '
    answer = (input(prompt + suffix).strip().lower() or ('y' if default_yes else 'n'))
    return answer in ('y', 'yes', 'да', 'д', '1')


def ask_max_media_mb() -> int:
    while True:
        text = input('Макс. размер медиафайла в МБ (Enter или 0 = без ограничений): ').strip()
        if not text or text == '0':
            return 0
        try:
            val = int(text)
            if val < 0:
                raise ValueError
            return val
        except ValueError:
            print('Введи целое число МБ (например, 50) или 0.')


def ask_message_mode():
    print('Для групп и каналов:')
    print('  1 — только твои сообщения и комменты')
    print('  2 — вообще все сообщения')
    while True:
        choice = input('Выбор [1]: ').strip() or '1'
        if choice == '1':
            return 'mine'
        if choice == '2':
            return 'all'
        print('Введи 1 или 2.')


def parse_date(text):
    return datetime.strptime(text, '%d.%m.%Y') if text else None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Высокоскоростная выгрузка Telegram в HTML')
    parser.add_argument('--all', action='store_true', help='выгрузить все чаты')
    parser.add_argument('--chat', help='выгрузить один чат (номер или название)')
    parser.add_argument('--from', dest='date_from', help='дата начала ДД.ММ.ГГГГ')
    parser.add_argument('--to', dest='date_to', help='дата окончания ДД.ММ.ГГГГ')
    parser.add_argument('--media', dest='media', action='store_true', default=None,
                        help='скачивать файлы из чатов')
    parser.add_argument('--no-media', dest='media', action='store_false',
                        help='не скачивать файлы из чатов')
    parser.add_argument('--max-size', dest='max_size', type=int, default=None,
                        help='максимальный размер одного медиафайла в МБ (0 = без лимита)')
    parser.add_argument('--mode', choices=['all', 'mine'], default=None,
                        help='для групп/каналов: all — все сообщения, mine — только пользователя')
    parser.add_argument('--output', default='telegram_export',
                        help='папка результата (по умолчанию telegram_export)')
    parser.add_argument('--lang', choices=['ru', 'en'], default='ru',
                        help='язык HTML-страниц')
    parser.add_argument('--phone', help='номер телефона для входа')
    parser.add_argument('--zip', action='store_true',
                        help='упаковать каждый чат в zip')
    parser.add_argument('--no-install', action='store_true',
                        help='не ставить Telethon и cryptg автоматически')
    parser.add_argument('--session', default=SESSION_DEFAULT,
                        help='имя файла сессии')
    parser.add_argument('--workers', type=int, choices=VALID_WORKERS, default=DEFAULT_WORKERS,
                        help=f'число параллельных потоков скачивания: 1, 2, 4 или 8 (по умолчанию {DEFAULT_WORKERS})')
    return parser.parse_args(argv)


def pick_dialog(dialogs, selector):
    selector = (selector or '').strip()
    if not selector:
        return None
    if selector.isdigit():
        number = int(selector)
        if 1 <= number <= len(dialogs):
            return dialogs[number - 1]
        return None
    lowered = selector.lower()
    for dialog in dialogs:
        if lowered == dialog_title(dialog).lower():
            return dialog
    for dialog in dialogs:
        if lowered in dialog_title(dialog).lower():
            return dialog
    return None


async def run_export(client, dialogs, args, progress_cb: Optional[Callable] = None) -> Path:
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    lang = getattr(args, 'lang', 'ru') or 'ru'

    profile, contacts, me = await gather_cancel_on_error(
        fetch_profile(client, output_dir),
        fetch_contacts(client),
        flood_safe(
            client.get_me,
            client=client,
            operation="загрузка профиля",
            policy=READ_FLOOD_POLICY,
        ),
    )
    my_id = getattr(me, 'id', 0)

    start_date = parse_date(args.date_from) if getattr(args, 'date_from', None) else None
    end_date = parse_date(args.date_to) if getattr(args, 'date_to', None) else None
    if end_date is not None:
        end_date = end_date.replace(hour=23, minute=59, second=59)

    download_media = args.media if getattr(args, 'media', None) is not None else True
    max_size_mb = getattr(args, 'max_size', None)
    mode = getattr(args, 'mode', None) or 'mine'

    selected = []
    # Прямая передача объектов диалогов из бота (исключает баги с номерами в названиях)
    if getattr(args, 'selected_dialogs', None):
        selected = list(args.selected_dialogs)
    elif getattr(args, 'all', False):
        selected = list(dialogs)
    elif getattr(args, 'chat', None):
        dialog = pick_dialog(dialogs, str(args.chat))
        if dialog is None:
            print(f'Чат «{args.chat}» не нашёл.')
            return output_dir
        selected = [dialog]
    else:
        while True:
            print_dialogs(dialogs)
            print('Что делаем?')
            print('  1 — выгрузить один чат')
            print('  2 — выгрузить все чаты')
            print('  3 — обновить список')
            print('  0 — выйти')
            command = input('Выбор: ').strip()
            if command == '0':
                return output_dir
            if command == '3':
                dialogs[:] = await get_dialogs(client)
                continue
            if command == '2':
                selected = list(dialogs)
                break
            if command == '1':
                number = input('Номер или название чата: ').strip()
                dialog = pick_dialog(dialogs, number)
                if dialog is None:
                    print('Такого чата нет.')
                    continue
                selected = [dialog]
                break
            print('Не понял. Выбери пункт из меню.')

        if start_date is None:
            start_date, end_date = ask_export_dates()
        if getattr(args, 'media', None) is None:
            download_media = ask_yes_no('Качать файлы?', True)
        if download_media and max_size_mb is None:
            max_size_mb = ask_max_media_mb()
        if getattr(args, 'mode', None) is None and any(not is_private_dialog(d) for d in selected):
            mode = ask_message_mode()

    if start_date is None:
        start_date = datetime(2000, 1, 1)

    max_media_bytes = int(max_size_mb or 0) * 1024 * 1024 if download_media else 0
    workers = clamp_workers(getattr(args, 'workers', DEFAULT_WORKERS))

    opts = ExportOptions(
        start_date=start_date,
        end_date=end_date,
        download_media=download_media,
        max_media_bytes=max_media_bytes,
        mode=mode,
        lang=lang,
        make_zip=bool(getattr(args, 'zip', False)),
        workers=workers,
    )

    chats_meta = []
    total_selected = len(selected)
    for number, dialog in enumerate(selected, start=1):
        title = dialog_title(dialog)
        print(f'\n[{number}/{total_selected}] {title}')

        async def _notify_stage(
            stage_str: str = '',
            current_number: int = number,
            current_title: str = title,
        ):
            if progress_cb:
                try:
                    sig = inspect.signature(progress_cb)
                    if len(sig.parameters) >= 4:
                        res = progress_cb(
                            current_number, total_selected, current_title, stage_str
                        )
                    else:
                        res = progress_cb(current_number, total_selected, current_title)
                    if inspect.isawaitable(res):
                        await res
                except Exception:  # noqa: BLE001 - progress hooks must not stop the export
                    return

        await _notify_stage('Инициализация чата...')
        try:
            info = await export_chat(client, dialog, output_dir, my_id, opts, sub_progress_cb=_notify_stage)
            chats_meta.append(info)
        except (FloodWaitLimitError, TimeoutError):
            raise
        except Exception as error:  # noqa: BLE001 - continue exporting other chats
            print(f'Не вышло выгрузить {title}: {error}')

    stats = {
        'chats': len(chats_meta),
        'contacts': len(contacts),
        'messages': sum(c.get('count', 0) for c in chats_meta),
        'media': sum(c.get('media', 0) for c in chats_meta),
        'date_range': build_date_range(opts.start_date, opts.end_date, lang),
        'exported_at': datetime.now().strftime('%d.%m.%Y %H:%M'),
    }
    (output_dir / 'index.html').write_text(
        build_index_page(profile, stats, chats_meta, lang), encoding='utf-8')
    (output_dir / 'contacts.html').write_text(
        build_contacts_page(profile, contacts, lang), encoding='utf-8')

    print()
    print('=' * 70)
    print('ГОТОВО')
    print(f'  Чатов: {stats["chats"]}')
    print(f'  Сообщений: {stats["messages"]}')
    print(f'  Файлов: {stats["media"]}')
    print(f'  Контактов: {stats["contacts"]}')
    print(f'  Откройте: {(output_dir / "index.html").resolve()}')
    print('=' * 70)
    return output_dir


def prevent_sleep():
    import ctypes
    if sys.platform != 'win32':
        return
    try:
        ctypes.windll.kernel32.SetThreadExecutionState(2147483648 | 1 | 64)
    except (AttributeError, OSError):
        return


def allow_sleep():
    import ctypes
    if sys.platform != 'win32':
        return
    try:
        ctypes.windll.kernel32.SetThreadExecutionState(2147483648)
    except (AttributeError, OSError):
        return


async def main_async(args) -> int:
    if not ensure_telethon(allow_install=not args.no_install):
        return 1

    try:
        config = load_config()
        resolve_credentials(config)
    except (FileNotFoundError, ValueError) as error:
        print(f'Проблема с настройками: {error}')
        return 1

    phone = (args.phone or config.get('phone') or '').strip()
    if not phone:
        phone = input('Номер Telegram: ').strip()
    try:
        phone_folder_name = normalize_phone(phone)
    except ValueError as error:
        print(error)
        return 1

    account_dir = ACCOUNTS_DIR / phone_folder_name
    account_dir.mkdir(parents=True, exist_ok=True)
    config['phone'] = phone

    session_path = account_dir / args.session
    if Path(args.output) == Path('telegram_export'):
        args.output = str(account_dir / 'export')
    else:
        args.output = str(account_dir / args.output)

    print('-' * 40)
    print(f'Telegram Export v{__version__}')
    print('-' * 40)
    print()

    client = await login(config, str(session_path), phone)
    if client is None:
        return 1
    try:
        dialogs = await get_dialogs(client)
        if not dialogs:
            print('Чатов не нашёл.')
            return 1
        await run_export(client, dialogs, args)
        return 0
    finally:
        await client.disconnect()


def main(argv=None) -> int:
    args = parse_args(argv)
    prevent_sleep()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print()
        print('Остановлено.')
        return 130
    except FloodWaitLimitError as error:
        print(f'Экспорт остановлен: {error}')
        return 1
    finally:
        allow_sleep()


if __name__ == '__main__':
    sys.exit(main())
