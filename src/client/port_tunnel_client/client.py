"""Клиентская часть обратного TCP-туннеля."""

import asyncio
import contextlib
import logging
from dataclasses import dataclass

from port_tunnel_protocol import (
    ControlMessage,
    DataMessage,
    ErrorMessage,
    InvalidControlMessageError,
    NewConnectionMessage,
    RegisteredMessage,
    RegisterMessage,
)
from port_tunnel_common.codecs import ABCMessageCodec
from port_tunnel_common.mixins import ProtocolTransmitterMixin, BridgeMixin


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


class TCPTunnelClient(ProtocolTransmitterMixin, BridgeMixin):
    """Регистрирует TCP-туннель и обслуживает control-канал.

    Объект владеет управляющим соединением и фоновыми задачами
    отдельных data-соединений.

    Только `_run_control_loop` читает из control StreamReader.
    """

    def __init__(
        self,
        *,
        config: TCPTunnelClientConfig,
        transmitter: ABCMessageCodec,
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
        except InvalidControlMessageError as error:
            _log.warning("[client] invalid control message error_count=%s", error.error_count)
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

        await self._send_control_message(
            writer,
            RegisterMessage(
                client_id=self._config.client_id,
                token=self._config.token,
                local_port=self._config.local_port,
                public_port=self._config.public_port,
            ),
        )

        response = await self._read_control_message(reader)
        match response:
            case RegisteredMessage(
                tunnel_id=tunnel_id,
                data_token=data_token,
                public_port=public_port,
            ):
                if public_port != self._config.public_port:
                    raise RuntimeError(
                        "Server returned an unexpected public port: "
                        f"expected={self._config.public_port}, "
                        f"received={public_port}"
                    )

                self._tunnel_id = tunnel_id
                self._data_token = data_token

                _log.info( "[client] registered tunnel_id=%s public_port=%s", tunnel_id, public_port)

            case ErrorMessage(
                code=error_code,
                message=error_message,
            ):
                raise RuntimeError(
                    "Tunnel registration failed: "
                    f"code={error_code!r}, "
                    f"message={error_message}"
                )

            case _:
                raise RuntimeError(
                    "Unexpected registration response: "
                    f"{type(response).__name__}"
                )

    async def _run_control_loop(self) -> None:
        """Последовательно читать и обрабатывать сообщения control-канала."""
        reader, _ = self._require_control_connection()

        while True:
            message = await self._read_control_message(reader)
            await self._handle_control_message(message)

    async def _handle_control_message(
        self,
        message: ControlMessage,
    ) -> None:
        """Передать типизированное сообщение соответствующему обработчику."""
        match message:
            case NewConnectionMessage():
                self._handle_new_connection_message(message)

            case ErrorMessage(
                code=error_code,
                message=error_message,
            ):
                _log.warning("[client] server error code=%r message=%s", error_code, error_message,)

            case _:
                _log.warning(
                    "[client] unexpected control message type=%s",
                    type(message).__name__,
                )

    def _handle_new_connection_message(
        self,
        message: NewConnectionMessage,
    ) -> None:
        """Проверить команду и запустить задачу data-канала."""
        expected_tunnel_id = self._require_tunnel_id()

        if message.tunnel_id != expected_tunnel_id:
            _log.warning(
                "[client] ignored connection for another tunnel expected=%s received=%s",
                expected_tunnel_id,
                message.tunnel_id,
            )
            return

        task = asyncio.create_task(
            self._serve_connection(message.connection_id),
            name=f"data-connection-{message.connection_id}",
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

            await self._send_control_message(
                server_writer,
                DataMessage(
                    tunnel_id=self._require_tunnel_id(),
                    connection_id=connection_id,
                    data_token=self._require_data_token(),
                ),
            )

            local_reader, local_writer = await asyncio.open_connection(
                self._config.local_host,
                self._config.local_port,
            )

            _log.info("[client] data bridge started connection_id=%s", connection_id)

            await self._bridge(
                server_reader,
                server_writer,
                local_reader,
                local_writer,
            )

        except (ConnectionError, OSError) as error:
            _log.warning("[client] cannot serve connection_id=%s: %s", connection_id, error)
        finally:
            if server_writer is not None:
                await self._close_writer(server_writer)

            if local_writer is not None:
                await self._close_writer(local_writer)

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

        with contextlib.suppress(ConnectionError, RuntimeError):
            await writer.wait_closed()
