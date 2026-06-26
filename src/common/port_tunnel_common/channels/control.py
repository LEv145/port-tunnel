import asyncio
import contextlib
from typing import Self

from port_tunnel_protocol import (
    ControlMessage,
    ControlMessageBase,
    parse_control_message,
    serialize_control_message,
)

from port_tunnel_common.codecs import ABCMessageCodec


class ControlChannel:
    """Канал управляющего протокола.

    Канал владеет парой `StreamReader` и `StreamWriter` и предоставляет
    безопасные операции чтения и отправки управляющих сообщений.
    """

    def __init__(
        self,
        *,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        codec: ABCMessageCodec,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._codec = codec

        self._write_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()

        self._read_in_progress = False
        self._closed = False
        self._detached = False

    @classmethod
    async def connect(cls, *, host: str, port: int, codec: ABCMessageCodec) -> Self:
        """Открыть TCP-соединение и создать управляющий канал."""
        reader, writer = await asyncio.open_connection(host, port)

        return cls(reader=reader, writer=writer, codec=codec)

    async def send(self, message: ControlMessageBase) -> None:
        """Последовательно отправить типизированное сообщение."""
        self._ensure_active()

        payload = serialize_control_message(message)

        async with self._write_lock:
            self._ensure_active()

            await self._codec.send_json(self._writer, payload)

    async def receive(self) -> ControlMessage:
        """Прочитать и проверить одно управляющее сообщение."""
        self._ensure_active()

        if self._read_in_progress:
            raise ConcurrentControlReadError("ControlChannel already has an active reader")

        self._read_in_progress = True

        try:
            payload = await self._codec.read_json(self._reader)
            return parse_control_message(payload)
        finally:
            self._read_in_progress = False

    def detach(self) -> tuple[asyncio.StreamReader,  asyncio.StreamWriter]:
        """Передать потоки вызывающему коду для raw TCP-обмена.

        После вызова канал больше нельзя использовать для `send`,
        `receive` или `close`. Ответственность за закрытие writer
        переходит к вызывающему коду.
        """
        self._ensure_active()

        if self._read_in_progress:
            raise ControlChannelStateError("Cannot detach while receive() is active")

        if self._write_lock.locked():
            raise ControlChannelStateError("Cannot detach while send() is active")

        self._detached = True

        return self._reader, self._writer

    async def close(self) -> None:
        """Идемпотентно закрыть принадлежащий каналу TCP-поток."""
        async with self._close_lock:
            if self._closed:
                return

            if self._detached:
                raise ControlChannelStateError("Detached channel no longer owns its streams")

            self._closed = True
            self._writer.close()

            with contextlib.suppress(ConnectionError, RuntimeError):
                await self._writer.wait_closed()

    @property
    def is_closing(self) -> bool:
        """Возвращает `True`, если канал закрывается или закрыт."""
        return self._closed or self._writer.is_closing()

    @property
    def peername(self) -> object:
        """Адрес удалённой стороны, если он доступен."""
        return self._writer.get_extra_info("peername")

    def _ensure_active(self) -> None:
        """Проверить, что канал владеет открытыми потоками."""
        if self._detached:
            raise ControlChannelStateError("Control channel streams have been detached")

        if self._closed or self._writer.is_closing():
            raise ControlChannelStateError("Control channel is closed")


class ControlChannelStateError(RuntimeError):
    """Операция недопустима в текущем состоянии канала."""


class ConcurrentControlReadError(RuntimeError):
    """Несколько корутин одновременно читают один control-канал."""
