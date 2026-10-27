import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError
from telethon.tl.types import Document

import FastTelethon
import main
from main import FloodWaitLimitError, FloodWaitPolicy, normalize_phone
from session_storage import account_session_base_path, cleanup_stale_qr_dirs
from telethon_client import create_telegram_client
from telethon_parallel_downloader import ParallelTransferrer


class FakeClient:
    pass


def test_telegram_client_factory_sets_russian_app_and_system_languages(monkeypatch):
    import telethon

    received = {}

    def fake_client(*args, **kwargs):
        received["args"] = args
        received["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(telethon, "TelegramClient", fake_client)

    result = create_telegram_client("session", 123, "hash", proxy={"addr": "proxy"})

    assert result is not None
    assert received["args"] == ("session", 123, "hash")
    assert received["kwargs"] == {
        "proxy": {"addr": "proxy"},
        "lang_code": "ru",
        "system_lang_code": "ru-RU",
    }


def test_flood_wait_retries_after_the_server_delay(monkeypatch):
    async def run():
        delays = []
        notices = []
        client = FakeClient()

        async def sleep(seconds):
            delays.append(seconds)

        async def report(operation, seconds):
            notices.append((operation, seconds))

        client._exporter_flood_wait_callback = report
        calls = 0

        async def request():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise FloodWaitError(None, capture=7)
            return "done"

        monkeypatch.setattr(main.asyncio, "sleep", sleep)
        result = await main.flood_safe(
            request,
            client=client,
            operation="reading messages",
            policy=FloodWaitPolicy(max_wait_seconds=10, max_retries=1),
        )
        assert result == "done"
        assert calls == 2
        assert delays == [7]
        assert notices == [("reading messages", 7)]

    asyncio.run(run())


def test_flood_wait_limit_stops_without_retry(monkeypatch):
    async def run():
        calls = 0

        async def request():
            nonlocal calls
            calls += 1
            raise FloodWaitError(None, capture=31)

        async def sleep(_seconds):
            raise AssertionError("Long FloodWait must not be slept through")

        monkeypatch.setattr(main.asyncio, "sleep", sleep)
        with pytest.raises(FloodWaitLimitError) as caught:
            await main.flood_safe(
                request,
                client=FakeClient(),
                operation="reading messages",
                policy=FloodWaitPolicy(max_wait_seconds=30, max_retries=2),
            )
        assert caught.value.wait_seconds == 31
        assert caught.value.attempts == 1
        assert calls == 1

    asyncio.run(run())


def test_message_iterator_does_not_treat_flood_wait_as_end_of_chat(monkeypatch):
    async def run():
        calls = 0

        class FloodedIterator:
            async def __anext__(self):
                nonlocal calls
                calls += 1
                raise FloodWaitError(None, capture=121)

        class Client(FakeClient):
            def iter_messages(self, *_args, **_kwargs):
                return FloodedIterator()

        async def sleep(_seconds):
            raise AssertionError("A wait beyond the read policy must stop")

        monkeypatch.setattr(main.asyncio, "sleep", sleep)

        async def read_chat():
            async for _message in main.iter_messages_safe(
                Client(), object(), start_date=None, end_date=None
            ):
                pass

        with pytest.raises(FloodWaitLimitError) as caught:
            await read_chat()
        assert caught.value.wait_seconds == 121
        assert calls == 1

    asyncio.run(run())


def test_repeated_flood_wait_is_bounded(monkeypatch):
    async def run():
        calls = 0
        delays = []

        async def request():
            nonlocal calls
            calls += 1
            raise FloodWaitError(None, capture=1)

        async def sleep(seconds):
            delays.append(seconds)

        monkeypatch.setattr(main.asyncio, "sleep", sleep)
        with pytest.raises(FloodWaitLimitError) as caught:
            await main.flood_safe(
                request,
                client=FakeClient(),
                operation="reading messages",
                policy=FloodWaitPolicy(max_wait_seconds=1, max_retries=2),
            )
        assert calls == 3
        assert delays == [1, 1]
        assert caught.value.attempts == 3

    asyncio.run(run())


def test_retries_for_the_same_client_are_serialized(monkeypatch):
    async def run():
        client = FakeClient()
        calls = {"a": 0, "b": 0}
        active_retries = 0
        max_active_retries = 0
        real_sleep = asyncio.sleep

        async def no_delay(_seconds):
            await real_sleep(0)

        async def request(name):
            nonlocal active_retries, max_active_retries
            calls[name] += 1
            if calls[name] == 1:
                raise FloodWaitError(None, capture=1)
            active_retries += 1
            max_active_retries = max(max_active_retries, active_retries)
            await real_sleep(0)
            active_retries -= 1
            return name

        monkeypatch.setattr(main.asyncio, "sleep", no_delay)
        results = await asyncio.gather(
            *(
                main.flood_safe(
                    request,
                    name,
                    client=client,
                    operation=name,
                    policy=FloodWaitPolicy(max_wait_seconds=1, max_retries=1),
                )
                for name in calls
            )
        )
        assert results == ["a", "b"]
        assert max_active_retries == 1

    asyncio.run(run())


def test_cancel_during_flood_wait_does_not_retry(monkeypatch):
    async def run():
        client = FakeClient()
        waiting = asyncio.Event()
        calls = 0

        async def blocked_sleep(_seconds):
            waiting.set()
            await asyncio.Future()

        async def request():
            nonlocal calls
            calls += 1
            raise FloodWaitError(None, capture=1)

        monkeypatch.setattr(main.asyncio, "sleep", blocked_sleep)
        task = asyncio.create_task(
            main.flood_safe(
                request,
                client=client,
                operation="reading messages",
                policy=FloodWaitPolicy(max_wait_seconds=1, max_retries=1),
            )
        )
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls == 1

    asyncio.run(run())


def test_gather_cancels_sibling_operations_after_failure():
    async def run():
        sibling_cancelled = asyncio.Event()

        async def fail():
            raise RuntimeError("request failed")

        async def sibling():
            try:
                await asyncio.Future()
            finally:
                sibling_cancelled.set()

        with pytest.raises(RuntimeError, match="request failed"):
            await main.gather_cancel_on_error(fail(), sibling())
        assert sibling_cancelled.is_set()

    asyncio.run(run())


def test_account_session_path_uses_normalized_account_folder(tmp_path):
    normalized_phone = normalize_phone(" +7 (900) 123-45-67 ")
    path = account_session_base_path(tmp_path / "accounts", normalized_phone)
    assert path == tmp_path / "accounts" / "+79001234567" / "session"
    assert str(path) + ".session" == str(
        tmp_path / "accounts" / "+79001234567" / "session.session"
    )


def test_cleanup_removes_only_stale_valid_qr_folders(tmp_path):
    accounts = tmp_path / "accounts"
    stale = accounts / "_qr_temp_101_1000"
    fresh = accounts / "_qr_temp_102_2000"
    malformed = accounts / "_qr_temp_old"
    account_session = accounts / "+79001234567" / "session.session"
    for directory in (stale, fresh, malformed, account_session.parent):
        directory.mkdir(parents=True)
    (stale / "temporary.session").write_bytes(b"temporary")
    (fresh / "temporary.session").write_bytes(b"fresh")
    account_session.write_bytes(b"existing session marker")
    old_time = 1_000_000 - 25 * 60 * 60
    import os

    os.utime(stale, (old_time, old_time))

    removed = cleanup_stale_qr_dirs(accounts, now=1_000_000)

    assert removed == 1
    assert not stale.exists()
    assert fresh.exists()
    assert malformed.exists()
    assert account_session.read_bytes() == b"existing session marker"


def test_public_media_fallback_and_loader_error(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setitem(sys.modules, "FastTelethon", None)
        document = Document(
            id=10,
            access_hash=20,
            file_reference=b"ref",
            date=None,
            mime_type="application/octet-stream",
            size=20,
            dc_id=2,
            attributes=[],
        )
        message = SimpleNamespace(media=SimpleNamespace(document=document))

        class Client(FakeClient):
            async def download_media(self, _message, file):
                Path(file).write_bytes(b"public download")
                return file

        client = Client()
        dest = tmp_path / "small.bin"
        result = await main.fast_download_file(client, message, dest)
        assert result == str(dest)
        assert dest.read_bytes() == b"public download"

        class BrokenClient(FakeClient):
            async def download_media(self, _message, file):
                raise OSError("disk unavailable")

        with pytest.raises(OSError, match="disk unavailable"):
            await main.fast_download_file(BrokenClient(), message, tmp_path / "failed.bin")

    asyncio.run(run())


def test_fasttelethon_download_uses_document_and_binary_stream(monkeypatch, tmp_path):
    async def run():
        document = Document(
            id=12,
            access_hash=22,
            file_reference=b"ref",
            date=None,
            mime_type="application/octet-stream",
            size=20,
            dc_id=2,
            attributes=[],
        )
        message = SimpleNamespace(media=SimpleNamespace(document=document))
        calls = []

        async def download_file(client, location, out_file):
            calls.append((client, location))
            out_file.write(b"FastTelethon download")
            return out_file

        monkeypatch.setitem(
            sys.modules,
            "FastTelethon",
            SimpleNamespace(download_file=download_file),
        )
        client = FakeClient()
        dest = tmp_path / "fast.bin"

        result = await main.fast_download_file(client, message, dest)

        assert result == str(dest)
        assert dest.read_bytes() == b"FastTelethon download"
        assert calls == [(client, document)]

    asyncio.run(run())


def test_bundled_fasttelethon_path_downloads_through_project_transferrer(
    monkeypatch, tmp_path
):
    async def run():
        document = Document(
            id=13,
            access_hash=23,
            file_reference=b"ref",
            date=None,
            mime_type="application/octet-stream",
            size=5,
            dc_id=2,
            attributes=[],
        )
        message = SimpleNamespace(media=SimpleNamespace(document=document))
        calls = []

        class FakeTransferrer:
            def __init__(self, client, dc_id=None):
                calls.append((client, dc_id))

            async def download(self, location, file_size):
                calls.append((location, file_size))
                yield b"fast "
                yield b"data"

        monkeypatch.setattr(FastTelethon, "ParallelTransferrer", FakeTransferrer)
        monkeypatch.setattr(
            FastTelethon.utils,
            "get_input_location",
            lambda _document: (2, "input-location"),
        )

        client = FakeClient()
        dest = tmp_path / "bundled-fast.bin"
        result = await main.fast_download_file(client, message, dest)

        assert result == str(dest)
        assert dest.read_bytes() == b"fast data"
        assert calls == [(client, 2), ("input-location", 5)]

    asyncio.run(run())


def test_fetch_profile_reads_about_from_user_full_result(monkeypatch, tmp_path):
    async def run():
        from telethon import functions

        monkeypatch.setattr(functions.users, "GetFullUserRequest", lambda _user: object())

        class Client(FakeClient):
            async def get_me(self):
                return SimpleNamespace(
                    id=42,
                    first_name="Test",
                    last_name="User",
                    phone=None,
                    username=None,
                )

            async def __call__(self, _request):
                return SimpleNamespace(about="Profile bio")

            async def download_profile_photo(self, _user, file):
                return None

        profile = await main.fetch_profile(Client(), tmp_path)
        assert profile["bio"] == "Profile bio"

    asyncio.run(run())


def test_parallel_loader_error_falls_back_and_cleans_partial_file(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setitem(sys.modules, "FastTelethon", None)
        document = Document(
            id=10,
            access_hash=20,
            file_reference=b"ref",
            date=None,
            mime_type="application/octet-stream",
            size=2 * 1024 * 1024,
            dc_id=2,
            attributes=[],
        )
        message = SimpleNamespace(media=SimpleNamespace(document=document))

        class BrokenTransferrer:
            def __init__(self, _client, dc_id=None):
                assert dc_id == 2

            async def download(self, _location, _size):
                yield b"partial"
                raise OSError("parallel path failed")

        class Client(FakeClient):
            async def download_media(self, _message, file):
                Path(file).write_bytes(b"fallback")
                return file

        monkeypatch.setitem(
            sys.modules,
            "telethon_parallel_downloader",
            SimpleNamespace(ParallelTransferrer=BrokenTransferrer),
        )
        import telethon.utils

        monkeypatch.setattr(telethon.utils, "get_input_location", lambda _doc: (2, "location"))
        dest = tmp_path / "large.bin"
        result = await main.fast_download_file(Client(), message, dest)
        assert result == str(dest)
        assert dest.read_bytes() == b"fallback"

    asyncio.run(run())


def test_parallel_loader_flood_wait_stops_before_public_fallback(monkeypatch, tmp_path):
    async def run():
        monkeypatch.setitem(sys.modules, "FastTelethon", None)
        document = Document(
            id=11,
            access_hash=21,
            file_reference=b"ref",
            date=None,
            mime_type="application/octet-stream",
            size=2 * 1024 * 1024,
            dc_id=2,
            attributes=[],
        )
        message = SimpleNamespace(media=SimpleNamespace(document=document))
        fallback_calls = 0

        class FloodedTransferrer:
            def __init__(self, _client, dc_id=None):
                assert dc_id == 2

            async def download(self, _location, _size):
                yield b"partial"
                raise FloodWaitError(None, capture=45)

        class Client(FakeClient):
            async def download_media(self, _message, _file):
                nonlocal fallback_calls
                fallback_calls += 1

        monkeypatch.setitem(
            sys.modules,
            "telethon_parallel_downloader",
            SimpleNamespace(ParallelTransferrer=FloodedTransferrer),
        )
        import telethon.utils

        monkeypatch.setattr(telethon.utils, "get_input_location", lambda _doc: (2, "location"))
        dest = tmp_path / "flooded.bin"
        with pytest.raises(FloodWaitLimitError) as caught:
            await main.fast_download_file(Client(), message, dest)
        assert caught.value.wait_seconds == 45
        assert fallback_calls == 0
        assert not dest.exists()

    asyncio.run(run())


def test_parallel_loader_disconnects_partial_sender_pool_after_init_error():
    async def run():
        class Sender:
            disconnected = False

            async def disconnect(self):
                self.disconnected = True

        sender = Sender()
        client = SimpleNamespace(
            session=SimpleNamespace(dc_id=2, auth_key=b"auth")
        )
        transferrer = ParallelTransferrer(client, dc_id=2)
        calls = 0

        async def create_sender():
            nonlocal calls
            calls += 1
            if calls == 1:
                await asyncio.sleep(0)
                return sender
            await asyncio.sleep(0)
            raise OSError("connection failed")

        transferrer._create_sender = create_sender
        with pytest.raises(OSError, match="connection failed"):
            await transferrer._init_senders(2)
        assert sender.disconnected
        assert transferrer.senders == []

    asyncio.run(run())


def test_parallel_loader_disconnects_sender_when_auth_import_fails(monkeypatch):
    async def run():
        import telethon.network

        class Sender:
            disconnected = False

            def __init__(self, *_args, **_kwargs):
                pass

            async def connect(self, _connection):
                pass

            async def disconnect(self):
                self.disconnected = True

        sender = Sender()
        monkeypatch.setattr(telethon.network, "MTProtoSender", lambda *_a, **_kw: sender)

        async def get_dc(_dc_id):
            return SimpleNamespace(ip_address="127.0.0.1", port=443, id=3)

        async def fail_export(_request):
            raise OSError("authorization export failed")

        class Client:
            def __init__(self):
                self.session = SimpleNamespace(dc_id=2, auth_key=b"auth")
                self._log = {}
                self._proxy = None
                self._init_request = SimpleNamespace()

            def _connection(self, *_a, **_kw):
                return object()

            async def _get_dc(self, dc_id):
                return await get_dc(dc_id)

            async def __call__(self, request):
                return await fail_export(request)

        client = Client()
        transferrer = ParallelTransferrer(client, dc_id=3)

        with pytest.raises(OSError, match="authorization export failed"):
            await transferrer._create_sender()
        assert sender.disconnected

    asyncio.run(run())


def test_parallel_download_limits_total_and_heavy_concurrency():
    async def run():
        workers_sem = asyncio.Semaphore(4)
        heavy_sem = asyncio.Semaphore(2)
        active = 0
        active_heavy = 0
        max_active = 0
        max_active_heavy = 0

        async def download(size):
            nonlocal active, active_heavy, max_active, max_active_heavy
            active += 1
            max_active = max(max_active, active)
            is_heavy = size >= main.FAST_DOWNLOAD_THRESHOLD
            if is_heavy:
                active_heavy += 1
                max_active_heavy = max(max_active_heavy, active_heavy)
            await asyncio.sleep(0.01)
            active -= 1
            if is_heavy:
                active_heavy -= 1

        await asyncio.gather(
            *(
                main._download_with_limits(
                    size,
                    lambda size=size: download(size),
                    workers_sem,
                    heavy_sem,
                )
                for size in [1, 2 * main.FAST_DOWNLOAD_THRESHOLD] * 8
            )
        )

        assert max_active <= 4
        assert max_active_heavy <= 2

    asyncio.run(run())


def test_parallel_loader_cancels_other_part_requests_after_failure():
    async def run():
        cancelled = asyncio.Event()

        async def fail():
            raise OSError("part failed")

        async def pending():
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        with pytest.raises(OSError, match="part failed"):
            await ParallelTransferrer._gather_cancel_on_error([fail(), pending()])
        assert cancelled.is_set()

    asyncio.run(run())
