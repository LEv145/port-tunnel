"""Клиентская часть обратного TCP-туннеля."""

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any

from transmitters import ABCTransmitter


_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TCPTunnelClientConfig:
    """Неизменяемые параметры одного TCP-туннеля."""

    server_host: str
    control_port: int
    client_id: str
    token: str
    local_host: str
    local_port: int
    public_port: int


class TCPTunnelClient:
    """Регистрирует TCP-туннель и обслуживает control-канал.

    Объект владеет управляющим соединением и фоновыми задачами
    отдельных data-соединений.

    Только `_run_control_loop` читает из control StreamReader.
    """

    def __init__(
        self,
        *,
        transmitter: ABCTransmitter,
        config: TCPTunnelClientConfig,
    ) -> None:
        self._transmitter = transmitter
        self._config = config

        self._control_reader: asyncio.StreamReader | None = None
        self._control_writer: asyncio.StreamWriter | None = None

        self._tunnel_id: str | None = None
        self._data_token: str | None = None

        self._connection_tasks: set[asyncio.Task[None]] = set()

    async def run(self) -> None:
        """Подключиться, зарегистрировать туннель и читать команды сервера."""
        try:
            await self._open_control_connection()
            await self._register_tunnel()
            await self._run_control_loop()
        except asyncio.IncompleteReadError:
            _log.warning("[client] control connection closed by server")
        except ConnectionError as error:
            _log.warning("[client] control connection lost: %s", error)
        finally:
            await self.close()

    async def close(self) -> None:
        """Остановить data-задачи и закрыть управляющее соединение."""
        tasks = tuple(self._connection_tasks)

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._connection_tasks.clear()

        if self._control_writer is not None:
            await self._close_writer(self._control_writer)

        self._control_reader = None
        self._control_writer = None
        self._tunnel_id = None
        self._data_token = None

    async def _open_control_connection(self) -> None:
        """Открыть постоянное управляющее соединение с сервером."""
        reader, writer = await asyncio.open_connection(
            self._config.server_host,
            self._config.control_port,
        )

        self._control_reader = reader
        self._control_writer = writer

    async def _register_tunnel(self) -> None:
        """Авторизоваться и получить идентификаторы туннеля."""
        reader, writer = self._require_control_connection()

        await self._transmitter.send_json(
            writer,
            {
                "type": "register",
                "client_id": self._config.client_id,
                "token": self._config.token,
                "local_port": self._config.local_port,
                "public_port": self._config.public_port,
            },
        )

        response = await self._transmitter.read_json(reader)

        if response.get("type") == "error":
            message = response.get("message", "registration failed")
            raise RuntimeError(str(message))

        tunnel_id = response.get("tunnel_id")
        data_token = response.get("data_token")

        if not isinstance(tunnel_id, str):
            raise RuntimeError("Server did not return tunnel_id")

        if not isinstance(data_token, str):
            raise RuntimeError("Server did not return data_token")

        self._tunnel_id = tunnel_id
        self._data_token = data_token

        _log.info(
            "[client] registered tunnel_id=%s public_port=%s",
            tunnel_id,
            self._config.public_port,
        )

    async def _run_control_loop(self) -> None:
        """Последовательно читать и обрабатывать сообщения control-канала."""
        reader, _ = self._require_control_connection()

        while True:
            message = await self._transmitter.read_json(reader)
            await self._handle_control_message(message)


    async def _handle_control_message(
        self,
        message: dict[str, Any],
    ) -> None:
        """Передать управляющее сообщение нужному обработчику."""
        message_type = message.get("type")

        if message_type == "new_connection":
            self._handle_new_connection_message(message)
            return

        _log.info(
            "[client] unknown control message type=%r",
            message_type,
        )

    def _handle_new_connection_message(
        self,
        message: dict[str, Any],
    ) -> None:
        """Проверить команду и запустить задачу data-канала."""
        tunnel_id = self._require_tunnel_id()

        message_tunnel_id = message.get("tunnel_id")
        connection_id = message.get("connection_id")

        if message_tunnel_id != tunnel_id:
            _log.warning(
                "[client] ignored message for another tunnel",
            )
            return

        if not isinstance(connection_id, str):
            _log.warning("[client] invalid connection_id")
            return

        task = asyncio.create_task(
            self._serve_connection(connection_id),
            name=f"data-connection-{connection_id}",
        )

        self._connection_tasks.add(task)
        task.add_done_callback(self._on_connection_task_done)


    def _on_connection_task_done(
        self,
        task: asyncio.Task[None],
    ) -> None:
        """Удалить завершившуюся задачу и получить её исключение."""
        self._connection_tasks.discard(task)

        if task.cancelled():
            return

        error = task.exception()

        if error is not None:
            _log.error(
                "[client] data connection task failed",
                exc_info=(
                    type(error),
                    error,
                    error.__traceback__,
                ),
            )

    async def _serve_connection(
        self,
        connection_id: str,
    ) -> None:
        """Создать data-канал и связать его с локальным TCP-сервисом."""
        server_writer: asyncio.StreamWriter | None = None
        local_writer: asyncio.StreamWriter | None = None

        try:
            server_reader, server_writer = await asyncio.open_connection(
                self._config.server_host,
                self._config.control_port,
            )

            await self._transmitter.send_json(
                server_writer,
                {
                    "type": "data",
                    "tunnel_id": self._require_tunnel_id(),
                    "connection_id": connection_id,
                    "data_token": self._require_data_token(),
                },
            )

            local_reader, local_writer = await asyncio.open_connection(
                self._config.local_host,
                self._config.local_port,
            )

            _log.info(
                "[client] data bridge started connection_id=%s",
                connection_id,
            )

            await self._bridge(
                server_reader,
                server_writer,
                local_reader,
                local_writer,
            )

        except (ConnectionError, OSError) as error:
            _log.warning(
                "[client] cannot serve connection_id=%s: %s",
                connection_id,
                error,
            )
        finally:
            if server_writer is not None:
                await self._close_writer(server_writer)

            if local_writer is not None:
                await self._close_writer(local_writer)

    async def _bridge(
        self,
        left_reader: asyncio.StreamReader,
        left_writer: asyncio.StreamWriter,
        right_reader: asyncio.StreamReader,
        right_writer: asyncio.StreamWriter,
    ) -> None:
        """Передавать TCP-байты одновременно в обоих направлениях."""
        tasks = {
            asyncio.create_task(
                self._pipe(left_reader, right_writer),
            ),
            asyncio.create_task(
                self._pipe(right_reader, left_writer),
            ),
        }

        try:
            await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Копировать TCP-байты в одном направлении."""
        try:
            while True:
                data = await reader.read(64 * 1024)

                if not data:
                    return

                writer.write(data)
                await writer.drain()
        except ConnectionError:
            return

    def _require_control_connection(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Вернуть control-соединение или сообщить об ошибке состояния."""
        if self._control_reader is None or self._control_writer is None:
            raise RuntimeError("Control connection is not established")

        return self._control_reader, self._control_writer


    def _require_tunnel_id(self) -> str:
        """Вернуть идентификатор зарегистрированного туннеля."""
        if self._tunnel_id is None:
            raise RuntimeError("Tunnel is not registered")

        return self._tunnel_id


    def _require_data_token(self) -> str:
        """Вернуть временный токен data-соединений."""
        if self._data_token is None:
            raise RuntimeError("Data token is not available")

        return self._data_token


    async def _close_writer(
        self,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Закрыть TCP-поток и дождаться освобождения транспорта."""
        writer.close()

        with contextlib.suppress(
            ConnectionError,
            RuntimeError,
        ):
            await writer.wait_closed()
