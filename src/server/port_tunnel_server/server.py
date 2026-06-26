"""Серверная часть сервиса обратного TCP-туннелирования."""

import asyncio
import hmac
import contextlib
import secrets
import logging
from dataclasses import dataclass, field
from functools import partial

from port_tunnel_common.channels import ControlChannel, ControlChannelStateError
from port_tunnel_common.codecs import ABCMessageCodec
from port_tunnel_common.mixins import BridgeMixin, StreamUtilsMixin
from port_tunnel_protocol import (
    DataMessage,
    ErrorMessage,
    NewConnectionMessage,
    RegisteredMessage,
    RegisterMessage,
)

from .registry import ABCTunnelRegistry, PendingTCPConnection, RegisteredTCPTunnel, TCPTunnelRegistry
from .authentication import ABCClientAuthenticator


_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TCPTunnelServerConfig:
    """Неизменяемые параметры TCP-сервера туннелирования."""

    control_host: str
    control_port: int
    public_host: str


class TCPTunnelServer(BridgeMixin, StreamUtilsMixin):
    """Управляет control-соединениями, публичными портами и TCP-мостами."""
    def __init__(
        self,
        *,
        config: TCPTunnelServerConfig,
        codec: ABCMessageCodec,
        authenticator: ABCClientAuthenticator,
    ) -> None:
        self._config = config
        self._codec = codec

        self._authenticator = authenticator
        self._registry: ABCTunnelRegistry = TCPTunnelRegistry()

    async def run(self) -> None:
        """Запустить общий control-listener и обслуживать его бесконечно."""
        control_server = await asyncio.start_server(
            self._handle_control,
            self._config.control_host,
            self._config.control_port,
        )

        _log.info("[server] control listening on %s:%s", self._config.control_host, self._config.control_port)

        async with control_server:
            await control_server.serve_forever()

    async def _handle_control(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Классифицировать новое клиентское соединение по первому сообщению."""
        channel = ControlChannel(reader=reader, writer=writer, codec=self._codec)

        try:
            message = await channel.receive()
        except (asyncio.IncompleteReadError, ConnectionError, OSError, UnicodeDecodeError, ValueError) as error:
            _log.warning("[control] invalid initial message error_type=%s", type(error).__name__)
            await channel.close()
            return

        match message:
            case RegisterMessage():
                await self._handle_register(message, channel)

            case DataMessage():
                raw_reader, raw_writer = channel.detach()
                await self._handle_data(message, raw_reader, raw_writer)

            case _:
                _log.warning("[control] unexpected initial message type=%s", type(message).__name__)
                await channel.close()

    async def _handle_register(self, message: RegisterMessage, channel: ControlChannel) -> None:
        """Зарегистрировать туннель и обслуживать его управляющий канал."""
        public_port = message.public_port
        client_id = message.client_id

        if not self._authenticator.authenticate(client_id, message.token):
            _log.info("[auth] registration rejected client_id=%s", client_id)
            await self._send_error_and_close(channel, code="unauthorized", message="invalid client credentials")
            return

        tunnel_id = secrets.token_hex(8)

        try:
            public_server = await asyncio.start_server(
                partial(self._handle_public_connection, tunnel_id),
                self._config.public_host,
                public_port,
            )
        except OSError as error:
            await self._send_error_and_close(
                channel,
                code="public_port_unavailable",
                message=f"cannot listen public port {public_port}: {error}",
            )
            return

        data_token = secrets.token_urlsafe(32)
        tunnel = RegisteredTCPTunnel(
            tunnel_id=tunnel_id,
            client_id=client_id,
            data_token=data_token,
            public_host=self._config.public_host,
            public_port=public_port,
            control_channel=channel,
            public_server=public_server,
        )

        try:
            await self._registry.add(tunnel)
        except ValueError as error:
            public_server.close()
            await public_server.wait_closed()
            await self._send_error_and_close(
                channel,
                code="public_port_already_registered",
                message=str(error),
            )
            return

        public_task: asyncio.Task[None] | None = None

        try:
            await channel.send(
                RegisteredMessage(tunnel_id=tunnel_id, data_token=data_token, public_port=public_port),
            )
            _log.info(
                "[control] tunnel registered client_id=%s tunnel_id=%s public_port=%s",
                client_id,
                tunnel_id,
                public_port,
            )

            async with public_server:
                public_task = asyncio.create_task(
                    public_server.serve_forever(),
                    name=f"public-listener-{tunnel_id}",
                )

                try:
                    await self._run_registered_control_loop(
                        tunnel_id=tunnel_id,
                        channel=channel,
                    )
                except (
                    asyncio.IncompleteReadError,
                    ConnectionError,
                    OSError,
                    UnicodeDecodeError,
                    ValueError,
                    ControlChannelStateError,
                ) as error:
                    _log.info(
                        "[control] channel closed tunnel_id=%s reason=%s",
                        tunnel_id,
                        type(error).__name__,
                    )
        finally:
            if public_task is not None:
                public_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await public_task

            await self._unregister_tunnel(tunnel_id)

    async def _handle_public_connection(
        self,
        tunnel_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Принять внешнего пользователя и запросить data-канал у клиента."""
        tunnel = await self._registry.get_by_id(tunnel_id)
        if tunnel is None:
            _log.info("[public] unknown tunnel_id=%s", tunnel_id)
            await self._close_writer(writer)
            return

        connection_id = secrets.token_hex(8)
        added = await self._registry.put_pending_public_connection(
            tunnel_id,
            connection_id,
            PendingTCPConnection(reader=reader, writer=writer),
        )
        if not added:
            await self._close_writer(writer)
            return

        _log.info("[public] new connection tunnel_id=%s connection_id=%s", tunnel_id, connection_id)

        try:
            await tunnel.control_channel.send(
                NewConnectionMessage(tunnel_id=tunnel_id, connection_id=connection_id),
            )
        except (ConnectionError, OSError, ControlChannelStateError) as error:
            _log.info("[public] cannot notify client: %s", error)
            await self._registry.pop_pending_public_connection(tunnel_id, connection_id)
            await self._close_writer(writer)

    async def _handle_data(
        self,
        message: DataMessage,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Сопоставить data-канал клиента с ожидающим внешним соединением."""
        tunnel_id = message.tunnel_id
        connection_id = message.connection_id
        tunnel = await self._registry.get_by_id(tunnel_id)

        if tunnel is None:
            _log.warning("[auth] data connection rejected: unknown tunnel")
            await self._close_writer(writer)
            return

        if not hmac.compare_digest(tunnel.data_token, message.data_token):
            _log.warning("[auth] data connection rejected tunnel_id=%s", tunnel_id)
            await self._close_writer(writer)
            return

        public_connection = await self._registry.pop_pending_public_connection(
            tunnel_id,
            connection_id,
        )
        if public_connection is None:
            _log.info("[data] unknown tunnel_id=%s connection_id=%s", tunnel_id, connection_id)
            await self._close_writer(writer)
            return

        _log.info("[data] bridge started tunnel_id=%s connection_id=%s", tunnel_id, connection_id)

        try:
            await self._bridge(public_connection.reader, public_connection.writer, reader, writer)
        finally:
            await self._close_writer(public_connection.writer)
            await self._close_writer(writer)

        _log.info("[data] bridge closed tunnel_id=%s connection_id=%s", tunnel_id, connection_id)

    async def _unregister_tunnel(self, tunnel_id: str) -> None:
        """Удалить туннель и закрыть все принадлежащие ему ресурсы."""
        tunnel = await self._registry.remove(tunnel_id)
        if tunnel is None:
            return

        tunnel.public_server.close()
        await tunnel.public_server.wait_closed()

        for connection_id, connection in list(tunnel.pending_public_connections.items()):
            _log.info("[control] close pending connection_id=%s", connection_id)
            await self._close_writer(connection.writer)

        tunnel.pending_public_connections.clear()
        await tunnel.control_channel.close()

        _log.info(
            "[control] tunnel unregistered tunnel_id=%s public_port=%s",
            tunnel.tunnel_id,
            tunnel.public_port,
        )

    async def _run_registered_control_loop(
        self,
        *,
        tunnel_id: str,
        channel: ControlChannel,
    ) -> None:
        """Последовательно читать сообщения зарегистрированного клиента."""
        while True:
            message = await channel.receive()

            _log.warning(
                "[control] unexpected message "
                "tunnel_id=%s type=%s",
                tunnel_id,
                type(message).__name__,
            )

    async def _send_error_and_close(
        self,
        channel: ControlChannel,
        *,
        code: str,
        message: str,
    ) -> None:
        """Отправить ошибку и гарантированно закрыть канал."""
        try:
            await channel.send(ErrorMessage(code=code, message=message))
        finally:
            await channel.close()
