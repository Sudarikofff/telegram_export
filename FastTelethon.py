"""FastTelethon-compatible download API backed by the pinned Telethon adapter."""

import inspect
from typing import BinaryIO

from telethon import utils

from telethon_parallel_downloader import ParallelTransferrer


async def download_file(client, location, out: BinaryIO, progress_callback=None):
    """Download a Telethon document to a binary stream using parallel senders.

    This keeps the common ``FastTelethon.download_file(client, document, out)``
    interface while relying on the project's version-pinned and cleaned-up
    sender implementation.
    """
    file_size = getattr(location, "size", None)
    if file_size is None:
        raise ValueError("FastTelethon download requires a file with a known size.")

    dc_id, input_location = utils.get_input_location(location)
    transferrer = ParallelTransferrer(client, dc_id=dc_id)
    downloaded = 0
    async for chunk in transferrer.download(input_location, file_size):
        out.write(chunk)
        downloaded += len(chunk)
        if progress_callback:
            result = progress_callback(downloaded, file_size)
            if inspect.isawaitable(result):
                await result
    return out
