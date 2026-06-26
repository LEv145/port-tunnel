import asyncio
import json
from collections.abc import Mapping
from typing import cast

from .abc import ABCMessageCodec


class ControlMessageCodec(ABCMessageCodec):
    """Передаёт управляющие JSON-сообщения с четырёхбайтовым префиксом длины."""

    def __init__(self, *, max_message_size: int = 64 * 1024) -> None:
        if max_message_size <= 0:
            raise ValueError("max_message_size must be positive")

        self._max_message_size = max_message_size

    async def send_json(self, writer: asyncio.StreamWriter, message: Mapping[str, object]) -> None:
        """Отправить один JSON-объект с префиксом длины."""
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        if len(data) > self._max_message_size:
            raise ValueError(f"Control message is too large: {len(data)} bytes")

        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()

    async def read_json(self, reader: asyncio.StreamReader) -> dict[str, object]:
        """Прочитать один JSON-объект с префиксом длины."""
        size_raw = await reader.readexactly(4)
        size = int.from_bytes(size_raw, "big")

        if size > self._max_message_size:
            raise ValueError(f"Control message is too large: {size} bytes")

        data = await reader.readexactly(size)
        payload = json.loads(data.decode("utf-8"))

        if not isinstance(payload, dict):
            raise ValueError("Control message must be a JSON object")

        return cast(dict[str, object], payload)
