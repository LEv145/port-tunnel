"""Серверная часть сервиса обратного TCP-туннелирования."""

import asyncio
import hmac
import contextlib
import secrets
import logging
from dataclasses import dataclass
from functools import partial

from port_tunnel_protocol import (
    DataMessage,
    ErrorMessage,
    NewConnectionMessage,
    RegisteredMessage,
    RegisterMessage,
)
from port_tunnel_common.transmitters import ABCTransmitter
from port_tunnel_common.mixins import ProtocolTransmitterMixin, BridgeMixin

from .registry import PendingTCPConnection, RegisteredTCPTunnel, TCPTunnelRegistry, ABCTunnelRegistry
from .authentication import ABCClientAuthenticator


_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TCPTunnelServerConfig:
    """Неизменяемые параметры одного TCP-туннеля."""

    control_host: str
    control_port: int
    public_host: str


class TCPTunnelServer(ProtocolTransmitterMixin, BridgeMixin):
    """Управляет control-соединениями, публичными портами и TCP-мостами."""

    def __init__(
        self,
        *,
        config: TCPTunnelServerConfig,
        transmitter: ABCTransmitter,
        authenticator: ABCClientAuthenticator,
    ) -> None:
        self._config = config
        self._transmitter = transmitter
        self._authenticator = authenticator
        self._registry: ABCTunnelRegistry = TCPTunnelRegistry()

    async def run(self) -> None:
        """Запустить общий control-listener и обслуживать его бесконечно."""
        control_server = await asyncio.start_server(
            self._handle_control,
            self._config.control_host,
            self._config.control_port,
        )

        _log.info(f"[server] control listening on {self._config.control_host}:{self._config.control_port}")

        async with control_server:
            await control_server.serve_forever()

    async def _handle_control(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Классифицировать новое клиентское соединение по первому сообщению."""
        try:
            message = await self._read_control_message(reader)
        except Exception as error:
            _log.warning("[control] invalid initial message error_type=%s", type(error).__name__)
            await self._close_writer(writer)
            return

        match message:
            case RegisterMessage():
                await self._handle_register(message, reader, writer)

            case DataMessage():
                await self._handle_data(message, reader, writer)

            case _:
                _log.warning("[control] unexpected initial message type=%s", type(message).__name__)
                await self._close_writer(writer)


    async def _handle_register(
        self,
        message: RegisterMessage,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Зарегистрировать туннель и запустить его публичный TCP-listener.

        Управляющий `writer` сохраняется на всё время жизни туннеля. Через него
        сервер сообщает клиенту о новых внешних подключениях. Завершение этого
        соединения инициирует очистку публичного listener и состояния туннеля.
        """
        public_port = message.public_port
        client_id = message.client_id
        token = message.token

        if not self._authenticator.authenticate(client_id, token):
            _log.info("[auth] registration rejected client_id=%s", client_id)

            await self._send_control_message(
                writer,
                ErrorMessage(
                    code="unauthorized",
                    message="invalid client credentials",
                ),
            )
            await self._close_writer(writer)
            return

        tunnel_id = secrets.token_hex(8)

        try:
            public_server = await asyncio.start_server(
                partial(self._handle_public_connection, tunnel_id),
                self._config.public_host,
                public_port,
            )
        except OSError as error:
            await self._send_control_message(
                writer,
                ErrorMessage(
                    code="public_port_unavailable",
                    message=(
                        f"cannot listen public port "
                        f"{public_port}: {error}"
                    ),
                ),
            )
            await self._close_writer(writer)
            return

        data_token = secrets.token_urlsafe(32)
        tunnel = RegisteredTCPTunnel(
            tunnel_id=tunnel_id,
            client_id=client_id,
            data_token=data_token,
            public_host=self._config.public_host,
            public_port=public_port,
            control_writer=writer,
            public_server=public_server,
        )

        try:
            await self._registry.add(tunnel)
        except ValueError as error:
            public_server.close()
            await public_server.wait_closed()

            await self._send_control_message(
                writer,
                ErrorMessage(
                    code="public_port_already_registered",
                    message=str(error),
                ),
            )
            await self._close_writer(writer)
            return

        public_task: asyncio.Task[None] | None = None

        try:
            # После добавления туннеля в реестр любая ошибка должна приводить
            # к его удалению и освобождению публичного порта.
            await self._send_control_message(
                writer,
                RegisteredMessage(
                    tunnel_id=tunnel_id,
                    data_token=data_token,
                    public_port=public_port,
                ),
            )

            _log.info(
                "[control] tunnel registered client_id=%s tunnel_id=%s public_port=%s",
                client_id,
                tunnel_id,
                public_port,
            )

            async with public_server:
                public_task = asyncio.create_task(public_server.serve_forever())

                try:
                    # После регистрации control-соединение используется сервером
                    # для отправки уведомлений клиенту. Здесь мы читаем его только
                    # для обнаружения EOF - момента, когда клиент отключился.
                    while await reader.read(1024):
                        pass
                except (ConnectionError, asyncio.IncompleteReadError):
                    # Разрыв сети и принудительное завершение клиента также означают,
                    # что туннель больше нельзя считать активным.
                    pass
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
            _log.info(f"[public] unknown tunnel_id={tunnel_id}")
            await self._close_writer(writer)
            return

        connection_id = secrets.token_hex(8)

        # Внешний сокет нельзя сразу связать с локальным сервисом: сервер не
        # имеет прямого доступа к сети клиента. Он временно сохраняется, пока
        # клиент не создаст исходящее data-соединение.
        added = await self._registry.put_pending_public_connection(
            tunnel_id,
            connection_id,
            PendingTCPConnection(reader=reader, writer=writer),
        )
        if not added:
            await self._close_writer(writer)
            return

        _log.info(f"[public] new connection tunnel_id={tunnel_id} connection_id={connection_id}")

        try:
            await self._send_control_message(
                tunnel.control_writer,
                NewConnectionMessage(
                    tunnel_id=tunnel_id,
                    connection_id=connection_id,
                ),
            )
        except (ConnectionError, OSError) as error:
            _log.info(f"[public] cannot notify client: {error}")
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
        data_token = message.data_token

        tunnel = await self._registry.get_by_id(tunnel_id)
        if tunnel is None:
            _log.warning("[auth] data connection rejected: unknown tunnel")
            await self._close_writer(writer)
            return

        if not hmac.compare_digest(tunnel.data_token, data_token):
            _log.warning("[auth] data connection rejected tunnel_id=%s", tunnel_id)
            await self._close_writer(writer)
            return

        public_connection = await self._registry.pop_pending_public_connection(
            tunnel_id,
            connection_id,
        )
        if public_connection is None:
            _log.info(f"[data] unknown tunnel_id={tunnel_id} connection_id={connection_id}")
            await self._close_writer(writer)
            return

        _log.info(f"[data] bridge started tunnel_id={tunnel_id} connection_id={connection_id}")

        try:
            await self._bridge(
                public_connection.reader,
                public_connection.writer,
                reader,
                writer,
            )
        finally:
            await self._close_writer(
                public_connection.writer,
            )
            await self._close_writer(writer)

        _log.info(f"[data] bridge closed tunnel_id={tunnel_id} connection_id={connection_id}")

    async def _unregister_tunnel(self, tunnel_id: str) -> None:
        """Удалить туннель, закрыть listener и незавершённые подключения."""
        tunnel = await self._registry.remove(tunnel_id)
        if tunnel is None:
            return

        tunnel.public_server.close()
        await tunnel.public_server.wait_closed()

        for connection_id, connection in list(tunnel.pending_public_connections.items()):
            _log.info(f"[control] close pending connection_id={connection_id}")
            await self._close_writer(connection.writer)

        tunnel.pending_public_connections.clear()
        await self._close_writer(tunnel.control_writer)

        _log.info(
            "[control] tunnel unregistered tunnel_id=%s public_port=%s",
            tunnel.tunnel_id,
            tunnel.public_port,
        )

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        """Идемпотентно инициировать закрытие TCP-потока и дождаться его."""
        writer.close()
        with contextlib.suppress(ConnectionError, RuntimeError):
            await writer.wait_closed()
