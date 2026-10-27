"""Compatibility adapter for the project's parallel Telethon media downloader.

This module intentionally contains the private Telethon calls used by the
parallel MTProto sender implementation. It is supported only with Telethon
1.45.0; the regular public ``TelegramClient.download_media`` fallback remains
available for other versions.
"""

import asyncio
import copy
import math
from contextlib import suppress
from typing import Optional

import telethon

SUPPORTED_TELETHON_VERSION = "1.45.0"


def _require_supported_telethon() -> None:
    if telethon.__version__ != SUPPORTED_TELETHON_VERSION:
        raise RuntimeError(
            "Параллельный загрузчик проверен только с Telethon "
            f"{SUPPORTED_TELETHON_VERSION}; установлена версия {telethon.__version__}."
        )


class ParallelTransferrer:
    """Download large files through additional MTProto sender connections."""

    def __init__(self, client, dc_id: Optional[int] = None):
        _require_supported_telethon()
        from telethon.tl.functions.upload import GetFileRequest

        self.client = client
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders = []
        self.GetFileRequest = GetFileRequest

    async def _create_sender(self):
        from telethon.network import MTProtoSender
        from telethon.tl.alltlobjects import LAYER
        from telethon.tl.functions import InvokeWithLayerRequest
        from telethon.tl.functions.auth import (
            ExportAuthorizationRequest,
            ImportAuthorizationRequest,
        )

        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        try:
            await sender.connect(
                self.client._connection(
                    dc.ip_address,
                    dc.port,
                    dc.id,
                    loggers=self.client._log,
                    proxy=self.client._proxy,
                )
            )
            if not self.auth_key:
                auth = await self.client(ExportAuthorizationRequest(self.dc_id))
                init_req = copy.copy(self.client._init_request)
                init_req.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                req = InvokeWithLayerRequest(LAYER, init_req)
                await sender.send(req)
                self.auth_key = sender.auth_key
        except BaseException:
            with suppress(BaseException):
                await sender.disconnect()
            raise
        return sender

    async def _init_senders(self, connections: int):
        tasks = [
            asyncio.create_task(self._create_sender())
            for _ in range(connections)
        ]
        try:
            self.senders = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            self.senders = [
                result for result in results if not isinstance(result, BaseException)
            ]
            await self._cleanup()
            raise

    @staticmethod
    async def _gather_cancel_on_error(awaitables):
        tasks = [asyncio.create_task(awaitable) for awaitable in awaitables]
        try:
            return await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _cleanup(self):
        await asyncio.gather(
            *[sender.disconnect() for sender in self.senders],
            return_exceptions=True,
        )
        self.senders.clear()

    @staticmethod
    def _get_connection_count(
        file_size: int,
        max_count: int = 8,
        full_size: int = 100 * 1024 * 1024,
    ) -> int:
        if file_size <= 0:
            return 2
        if file_size >= full_size:
            return max_count
        return max(2, math.ceil((file_size / full_size) * max_count))

    async def download(
        self,
        file_location,
        file_size: int,
        part_size_kb: int = 512,
        connection_count: Optional[int] = None,
    ):
        part_size = part_size_kb * 1024
        conn_count = connection_count or self._get_connection_count(file_size)
        part_count = math.ceil(file_size / part_size) if file_size > 0 else 0

        await self._init_senders(conn_count)
        try:
            if part_count == 0:
                offset = 0
                while True:
                    res = await self.senders[0].send(
                        self.GetFileRequest(file_location, offset=offset, limit=part_size)
                    )
                    if not res.bytes:
                        break
                    yield res.bytes
                    offset += len(res.bytes)
                    if len(res.bytes) < part_size:
                        break
                return

            for batch_start in range(0, part_count, conn_count):
                batch_end = min(batch_start + conn_count, part_count)
                tasks = []
                for idx, part_idx in enumerate(range(batch_start, batch_end)):
                    sender = self.senders[idx % len(self.senders)]
                    offset = part_idx * part_size
                    tasks.append(
                        sender.send(
                            self.GetFileRequest(file_location, offset=offset, limit=part_size)
                        )
                    )
                results = await self._gather_cancel_on_error(tasks)
                for res in results:
                    if res and res.bytes:
                        yield res.bytes
        finally:
            await self._cleanup()
