"""Клиентская часть обратного TCP-туннеля."""

import asyncio
import logging
from dataclasses import dataclass

from port_tunnel_common.channels import ControlChannel, ControlChannelStateError
from port_tunnel_common.codecs import ABCMessageCodec
from port_tunnel_common.mixins import BridgeMixin, StreamUtilsMixin
from port_tunnel_protocol import (
    ControlMessage,
    DataMessage,
    ErrorMessage,
    InvalidControlMessageError,
    NewConnectionMessage,
    RegisteredMessage,
    RegisterMessage,
)


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


class TCPTunnelClient(BridgeMixin, StreamUtilsMixin):
    """Регистрирует TCP-туннель и обслуживает его управляющий канал."""

    def __init__(self, *, config: TCPTunnelClientConfig, codec: ABCMessageCodec) -> None:
        self._codec = codec
        self._control_channel: ControlChannel | None = None
        self._config = config

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
        except (ConnectionError, OSError, ControlChannelStateError) as error:
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
        channel = self._control_channel
        self._control_channel = None

        if channel is not None:
            await channel.close()

        self._tunnel_id = None
        self._data_token = None

    async def _open_control_connection(self) -> None:
        """Открыть постоянное управляющее соединение с сервером."""
        self._control_channel = await ControlChannel.connect(
            host=self._config.server_host,
            port=self._config.control_port,
            codec=self._codec,
        )

    async def _register_tunnel(self) -> None:
        """Авторизоваться и получить идентификаторы туннеля."""
        channel = self._require_control_channel()
        await channel.send(
            RegisterMessage(
                client_id=self._config.client_id,
                token=self._config.token,
                local_port=self._config.local_port,
                public_port=self._config.public_port,
            ),
        )

        response = await channel.receive()

        match response:
            case RegisteredMessage(tunnel_id=tunnel_id, data_token=data_token, public_port=public_port):
                if public_port != self._config.public_port:
                    raise RuntimeError(
                        f"Server returned an unexpected public port: expected={self._config.public_port}, "
                        f"received={public_port}"
                    )

                self._tunnel_id = tunnel_id
                self._data_token = data_token
                _log.info("[client] registered tunnel_id=%s public_port=%s", tunnel_id, public_port)

            case ErrorMessage(code=error_code, message=error_message):
                raise RuntimeError(f"Tunnel registration failed: code={error_code!r}, message={error_message}")

            case _:
                raise RuntimeError(f"Unexpected registration response: {type(response).__name__}")

    async def _run_control_loop(self) -> None:
        """Последовательно читать и обрабатывать сообщения control-канала."""
        channel = self._require_control_channel()

        while True:
            message = await channel.receive()
            await self._handle_control_message(message)

    async def _handle_control_message(self, message: ControlMessage) -> None:
        """Передать типизированное сообщение соответствующему обработчику."""
        match message:
            case NewConnectionMessage():
                self._handle_new_connection_message(message)

            case ErrorMessage(
                code=error_code,
                message=error_message,
            ):
                _log.warning("[client] server error code=%r message=%s", error_code, error_message)

            case _:
                _log.warning("[client] unexpected control message type=%s", type(message).__name__)

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

    def _on_connection_task_done(self, task: asyncio.Task[None]) -> None:
        """Удалить завершившуюся задачу и получить её исключение."""
        self._connection_tasks.discard(task)

        if task.cancelled():
            return

        error = task.exception()

        if error is not None:
            _log.error(
                "[client] data connection task failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _serve_connection(self, connection_id: str) -> None:
        """Создать data-канал и связать его с локальным TCP-сервисом."""
        data_channel: ControlChannel | None = None
        server_writer: asyncio.StreamWriter | None = None
        local_writer: asyncio.StreamWriter | None = None

        try:
            data_channel = await ControlChannel.connect(
                host=self._config.server_host,
                port=self._config.control_port,
                codec=self._codec,
            )
            await data_channel.send(
                DataMessage(
                    tunnel_id=self._require_tunnel_id(),
                    connection_id=connection_id,
                    data_token=self._require_data_token(),
                ),
            )

            server_reader, server_writer = data_channel.detach()
            data_channel = None
            local_reader, local_writer = await asyncio.open_connection(
                self._config.local_host,
                self._config.local_port,
            )

            _log.info("[client] data bridge started connection_id=%s", connection_id)
            await self._bridge(server_reader, server_writer, local_reader, local_writer)

        except (ConnectionError, OSError) as error:
            _log.warning("[client] cannot serve connection_id=%s: %s", connection_id, error)
        finally:
            if data_channel is not None:
                await data_channel.close()

            if server_writer is not None:
                await self._close_writer(server_writer)

            if local_writer is not None:
                await self._close_writer(local_writer)

    def _require_control_channel(self) -> ControlChannel:
        """Вернуть открытый управляющий канал."""
        if self._control_channel is None:
            raise RuntimeError("Control channel is not established")

        return self._control_channel

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
