"""Серверная часть сервиса обратного TCP-туннелирования."""

import asyncio
import hmac
import contextlib
import secrets
import logging
from functools import partial
from typing import Any

from transmitters import ABCTransmitter

from .registry import PendingTCPConnection, RegisteredTCPTunnel, TCPTunnelRegistry, ABCTunnelRegistry
from .authentication import ABCClientAuthenticator


_log = logging.getLogger(__name__)


class TCPTunnelServer:
    """Управляет control-соединениями, публичными портами и TCP-мостами.

    Сервер принимает на одном control-порту два вида соединений клиента:

    * `register` - долговременное управляющее соединение туннеля;
    * `data` - отдельное соединение для одного внешнего TCP-подключения.

    Для каждого зарегистрированного туннеля сервер динамически запускает
    отдельный публичный listener. Пользовательский трафик сервер не разбирает:
    байты копируются между внешним сокетом и data-сокетом клиента.
    """

    def __init__(
        self,
        *,
        transmitter: ABCTransmitter,
        authenticator: ABCClientAuthenticator,
        control_host: str,
        control_port: int,
        public_host: str,
    ) -> None:
        self._transmitter = transmitter
        self._authenticator = authenticator
        self._control_host = control_host
        self._control_port = control_port
        self._public_host = public_host
        self._registry: ABCTunnelRegistry = TCPTunnelRegistry()

    async def run(self) -> None:
        """Запустить общий control-listener и обслуживать его бесконечно."""
        control_server = await asyncio.start_server(
            self._handle_control,
            self._control_host,
            self._control_port,
        )

        _log.info(f"[server] control listening on {self._control_host}:{self._control_port}")

        async with control_server:
            await control_server.serve_forever()

    async def _handle_control(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Классифицировать новое клиентское соединение по первому сообщению.

        На одном TCP-порту принимаются и управляющие, и data-соединения.
        Первое length-prefixed JSON-сообщение содержит поле `type`, по которому
        соединение передаётся нужному обработчику.
        """
        try:
            message = await self._transmitter.read_json(reader)
        except Exception:
            await self._close_writer(writer)
            return

        message_type = message.get("type")

        if message_type == "register":
            await self._handle_register(message, reader, writer)
            return

        if message_type == "data":
            await self._handle_data(message, reader, writer)
            return

        _log.info(f"[server] unknown message type: {message_type}")
        await self._close_writer(writer)

    async def _handle_register(
        self,
        message: dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Зарегистрировать туннель и запустить его публичный TCP-listener.

        Управляющий `writer` сохраняется на всё время жизни туннеля. Через него
        сервер сообщает клиенту о новых внешних подключениях. Завершение этого
        соединения инициирует очистку публичного listener и состояния туннеля.
        """
        public_port = message.get("public_port")
        client_id = message.get("client_id")
        token = message.get("token")

        if not isinstance(client_id, str) or not isinstance(token, str):
            await self._transmitter.send_json(
                writer,
                {
                    "type": "error",
                    "code": "invalid_credentials",
                    "message": "client_id and token must be strings",
                },
            )
            await self._close_writer(writer)
            return

        if not isinstance(public_port, int):
            await self._transmitter.send_json(writer, {
                "type": "error",
                "message": "public_port must be int",
            })
            await self._close_writer(writer)
            return

        if not self._authenticator.authenticate(client_id, token):
            _log.info(
                "[auth] registration rejected client_id=%s",
                client_id,
            )

            await self._transmitter.send_json(
                writer,
                {
                    "type": "error",
                    "code": "unauthorized",
                    "message": "invalid client credentials",
                },
            )
            await self._close_writer(writer)
            return

        tunnel_id = secrets.token_hex(8)

        try:
            public_server = await asyncio.start_server(
                partial(self._handle_public_connection, tunnel_id),
                self._public_host,
                public_port,
            )
        except OSError as error:
            await self._transmitter.send_json(writer, {
                "type": "error",
                "message": f"cannot listen public port {public_port}: {error}",
            })
            await self._close_writer(writer)
            return

        data_token = secrets.token_urlsafe(32)
        tunnel = RegisteredTCPTunnel(
            tunnel_id=tunnel_id,
            client_id=client_id,
            data_token=data_token,
            public_host=self._public_host,
            public_port=public_port,
            control_writer=writer,
            public_server=public_server,
        )

        try:
            await self._registry.add(tunnel)
        except ValueError as error:
            public_server.close()
            await public_server.wait_closed()

            await self._transmitter.send_json(writer, {
                "type": "error",
                "message": str(error),
            })
            await self._close_writer(writer)
            return

        public_task: asyncio.Task[None] | None = None

        try:
            # После добавления туннеля в реестр любая ошибка должна приводить
            # к его удалению и освобождению публичного порта.
            await self._transmitter.send_json(writer, {
                "type": "registered",
                "tunnel_id": tunnel_id,
                "data_token": data_token,
                "public_port": public_port,
            })

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
            await self._transmitter.send_json(tunnel.control_writer, {
                "type": "new_connection",
                "tunnel_id": tunnel_id,
                "connection_id": connection_id,
            })
        except Exception as error:
            _log.info(f"[public] cannot notify client: {error}")
            await self._registry.pop_pending_public_connection(tunnel_id, connection_id)
            await self._close_writer(writer)

    async def _handle_data(
        self,
        message: dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Сопоставить data-канал клиента с ожидающим внешним соединением."""
        tunnel_id = message.get("tunnel_id")
        connection_id = message.get("connection_id")
        data_token = message.get("data_token")

        if not isinstance(tunnel_id, str) or not isinstance(connection_id, str):
            _log.info("[data] invalid tunnel_id or connection_id")
            await self._close_writer(writer)
            return

        if not isinstance(data_token, str):
            _log.info("[data] invalid data_token")
            await self._close_writer(writer)
            return

        tunnel = await self._registry.get_by_id(tunnel_id)
        if tunnel is None:
            _log.warning("[auth] data connection rejected: unknown tunnel")
            await self._close_writer(writer)
            return

        if not hmac.compare_digest(tunnel.data_token, data_token):
            _log.warning(
                "[auth] data connection rejected tunnel_id=%s",
                tunnel_id,
            )
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

        await self._bridge(
            public_connection.reader,
            public_connection.writer,
            reader,
            writer,
        )

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

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Копировать непрозрачный поток байтов в одном направлении."""
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break

                writer.write(data)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            await self._close_writer(writer)

    async def _bridge(
        self,
        left_reader: asyncio.StreamReader,
        left_writer: asyncio.StreamWriter,
        right_reader: asyncio.StreamReader,
        right_writer: asyncio.StreamWriter,
    ) -> None:
        """Создать двунаправленный TCP-мост между двумя соединениями."""
        task1 = asyncio.create_task(self._pipe(left_reader, right_writer))
        task2 = asyncio.create_task(self._pipe(right_reader, left_writer))

        _, pending = await asyncio.wait(
            {task1, task2},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

        await self._close_writer(left_writer)
        await self._close_writer(right_writer)
