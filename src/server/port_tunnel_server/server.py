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
    PingMessage,
    PongMessage,
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
    heartbeat_interval: float = 15.0
    heartbeat_timeout: float = 45.0

    def __post_init__(self) -> None:
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")

        if self.heartbeat_timeout <= self.heartbeat_interval:
            raise ValueError("heartbeat_timeout must be greater than heartbeat_interval")


@dataclass(slots=True)
class _HeartbeatState:
    expected_id: str | None = None
    response_event: asyncio.Event = field(default_factory=asyncio.Event)

    def begin(self, heartbeat_id: str) -> None:
        self.expected_id = heartbeat_id
        self.response_event.clear()

    def accept(self, heartbeat_id: str) -> bool:
        if heartbeat_id != self.expected_id:
            return False

        self.expected_id = None
        self.response_event.set()
        return True


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

            heartbeat = _HeartbeatState()

            async with public_server:
                public_task = asyncio.create_task(
                    public_server.serve_forever(),
                    name=f"public-listener-{tunnel_id}",
                )

                try:
                    await self._run_registered_session(tunnel_id=tunnel_id, channel=channel, heartbeat=heartbeat)
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

    async def _run_registered_session(
        self,
        *,
        tunnel_id: str,
        channel: ControlChannel,
        heartbeat: _HeartbeatState,
    ) -> None:
        """Одновременно обслуживать control-loop и серверный heartbeat."""
        tasks = {
            asyncio.create_task(
                self._run_registered_control_loop(tunnel_id=tunnel_id, channel=channel, heartbeat=heartbeat),
                name=f"control-loop-{tunnel_id}",
            ),
            asyncio.create_task(
                self._run_heartbeat_loop(tunnel_id=tunnel_id, channel=channel, heartbeat=heartbeat),
                name=f"heartbeat-{tunnel_id}",
            ),
        }

        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_registered_control_loop(
        self,
        *,
        tunnel_id: str,
        channel: ControlChannel,
        heartbeat: _HeartbeatState,
    ) -> None:
        """Последовательно читать сообщения зарегистрированного клиента."""
        while True:
            message = await channel.receive()

            match message:
                case PongMessage(heartbeat_id=heartbeat_id):
                    if heartbeat.accept(heartbeat_id):
                        _log.info("[heartbeat] pong received tunnel_id=%s heartbeat_id=%s", tunnel_id, heartbeat_id)
                    else:
                        _log.warning(
                            "[heartbeat] unexpected pong tunnel_id=%s heartbeat_id=%s",
                            tunnel_id,
                            heartbeat_id,
                        )

                case _:
                    _log.warning(
                        "[control] unexpected message tunnel_id=%s type=%s",
                        tunnel_id,
                        type(message).__name__,
                    )

    async def _run_heartbeat_loop(
        self,
        *,
        tunnel_id: str,
        channel: ControlChannel,
        heartbeat: _HeartbeatState,
    ) -> None:
        """Периодически проверять доступность клиента через ping/pong."""
        while True:
            await asyncio.sleep(self._config.heartbeat_interval)
            heartbeat_id = secrets.token_hex(8)
            heartbeat.begin(heartbeat_id)

            try:
                async with asyncio.timeout(self._config.heartbeat_timeout):
                    await channel.send(PingMessage(heartbeat_id=heartbeat_id))
                    _log.info("[heartbeat] ping sent tunnel_id=%s heartbeat_id=%s", tunnel_id, heartbeat_id)
                    await heartbeat.response_event.wait()
            except TimeoutError:
                _log.warning(
                    "[heartbeat] timeout tunnel_id=%s heartbeat_id=%s timeout=%.1fs",
                    tunnel_id,
                    heartbeat_id,
                    self._config.heartbeat_timeout,
                )
                await channel.close()
                return
            except (ConnectionError, OSError, ControlChannelStateError):
                await channel.close()
                return

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

        public_connection = await self._registry.activate_data_connection(tunnel_id, connection_id, writer)
        if public_connection is None:
            _log.info("[data] unknown tunnel_id=%s connection_id=%s", tunnel_id, connection_id)
            await self._close_writer(writer)
            return

        _log.info("[data] bridge started tunnel_id=%s connection_id=%s", tunnel_id, connection_id)

        try:
            await self._bridge(public_connection.reader, public_connection.writer, reader, writer)
        finally:
            await self._registry.remove_active_data_connection(tunnel_id, connection_id)
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
        await tunnel.control_channel.close()

        for connection_id, connection in list(tunnel.pending_public_connections.items()):
            _log.info("[control] close pending connection_id=%s", connection_id)
            await self._close_writer(connection.writer)

        for connection_id, connection in list(tunnel.active_data_connections.items()):
            _log.info("[control] close active connection_id=%s", connection_id)
            await asyncio.gather(
                self._close_writer(connection.public_writer),
                self._close_writer(connection.data_writer),
            )

        tunnel.pending_public_connections.clear()
        tunnel.active_data_connections.clear()

        _log.info(
            "[control] tunnel unregistered tunnel_id=%s public_port=%s",
            tunnel.tunnel_id,
            tunnel.public_port,
        )

    async def _send_error_and_close(self, channel: ControlChannel, *, code: str, message: str) -> None:
        """Отправить ошибку и гарантированно закрыть канал."""
        try:
            await channel.send(ErrorMessage(code=code, message=message))
        finally:
            await channel.close()
