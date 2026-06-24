import asyncio

from port_tunnel_common.codecs import ABCMessageCodec
from port_tunnel_protocol.codec import ControlMessage, parse_control_message, serialize_control_message
from port_tunnel_protocol.messages import ControlMessageBase


class ProtocolTransmitterMixin:
    _codec: ABCMessageCodec

    async def _read_control_message(
        self,
        reader: asyncio.StreamReader,
    ) -> ControlMessage:
        """Прочитать и проверить одно сообщение управляющего протокола."""
        payload = await self._codec.read_json(reader)
        return parse_control_message(payload)

    async def _send_control_message(
        self,
        writer: asyncio.StreamWriter,
        message: ControlMessageBase,
    ) -> None:
        """Сериализовать и отправить управляющее сообщение."""
        payload = serialize_control_message(message)
        await self._codec.send_json(
            writer,
            payload,
        )
