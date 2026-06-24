import asyncio
import json
from typing import Any

from .abc import ABCTransmitter


class TCPTransmitter(ABCTransmitter):
    async def send_json(self, writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
        data = json.dumps(message, ensure_ascii=False).encode("utf-8")
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()

    async def read_json(self, reader: asyncio.StreamReader) -> dict[str, Any]:
        size_raw = await reader.readexactly(4)
        size = int.from_bytes(size_raw, "big")
        data = await reader.readexactly(size)
        return json.loads(data.decode("utf-8"))
