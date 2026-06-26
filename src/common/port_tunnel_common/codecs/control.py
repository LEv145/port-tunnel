import asyncio
import json
from typing import cast

from .abc import ABCMessageCodec


class ControlMessageCodec(ABCMessageCodec):
    """Кодирует JSON-объекты в TCP-поток и декодирует их обратно."""

    async def send_json(self, writer: asyncio.StreamWriter, message: dict[str, object]) -> None:
        """Отправить один JSON-объект с префиксом длины."""
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()

    async def read_json(self, reader: asyncio.StreamReader) -> dict[str, object]:
        """Прочитать один JSON-объект с префиксом длины."""
        size_raw = await reader.readexactly(4)
        size = int.from_bytes(size_raw, "big")
        data = await reader.readexactly(size)
        payload = json.loads(data.decode("utf-8"))

        if not isinstance(payload, dict):
            raise ValueError("Control message must be a JSON object")

        return cast(dict[str, object], payload)
