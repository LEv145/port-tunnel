"""CLI и клиентская часть обратного TCP-туннеля."""

import asyncio
import logging

import typer
from transmitters import ABCTransmitter, TCPTransmitter

from .setups.logging import setup_logging


setup_logging(logging.INFO)
_log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def main(
    server_host: str = typer.Option(),
    control_port: int = 7000,
    local_host: str = "127.0.0.1",
    local_port: int = typer.Option(),
    public_port: int = 30001,
) -> None:
    """Запустить клиентский агент для одного локального TCP-сервиса."""
    asyncio.run(async_main(**locals()))


async def async_main(
    server_host: str,
    local_host: str,
    local_port: int,
    control_port: int,
    public_port: int,
) -> None:
    """Зарегистрировать туннель и обслуживать команды сервера.

    Эта корутина поддерживает долговременное control-соединение. По каждому
    сообщению `new_connection` она запускает отдельную задачу, создающую
    data-соединение к серверу и соединение к локальному сервису.
    """
    transmitter = TCPTransmitter()

    reader, writer = await asyncio.open_connection(
        server_host,
        control_port,
    )

    await transmitter.send_json(
        writer,
        {
            "type": "register",
            "local_port": local_port,
            "public_port": public_port,
        },
    )

    response = await transmitter.read_json(reader)
    _log.info(f"[client] registered: {response}")

    if response.get("type") == "error":
        raise RuntimeError(response.get("message"))

    tunnel_id = response.get("tunnel_id")
    if not isinstance(tunnel_id, str):
        raise RuntimeError("Server did not return tunnel_id")

    while True:
        message = await transmitter.read_json(reader)

        if message.get("type") == "new_connection":
            message_tunnel_id = message.get("tunnel_id")
            connection_id = message.get("connection_id")

            if message_tunnel_id != tunnel_id:
                _log.info(f"[client] ignored connection for another tunnel: {message}")
                continue

            if not isinstance(connection_id, str):
                _log.info(f"[client] invalid connection_id: {message}")
                continue

            # Задача создаётся отдельно, чтобы несколько внешних соединений
            # одного туннеля обслуживались конкурентно.
            asyncio.create_task(
                _handle_new_connection(
                    transmitter=transmitter,
                    server_host=server_host,
                    control_port=control_port,
                    local_host=local_host,
                    local_port=local_port,
                    tunnel_id=tunnel_id,
                    connection_id=connection_id,
                )
            )
        else:
            _log.info(f"[client] unknown message: {message}")


async def _handle_new_connection(
    transmitter: ABCTransmitter,
    server_host: str,
    control_port: int,
    local_host: str,
    local_port: int,
    tunnel_id: str,
    connection_id: str,
) -> None:
    """Создать data-канал и связать его с локальным TCP-сервисом."""
    _log.info(f"[client] new connection_id={connection_id}")

    # Data-соединение является исходящим, поэтому проходит через NAT так же,
    # как обычное подключение к сайту.
    server_reader, server_writer = await asyncio.open_connection(
        server_host,
        control_port,
    )

    await transmitter.send_json(
        server_writer,
        {
            "type": "data",
            "tunnel_id": tunnel_id,
            "connection_id": connection_id,
        },
    )

    try:
        local_reader, local_writer = await asyncio.open_connection(
            local_host,
            local_port,
        )
    except Exception as error:
        _log.info(f"[client] cannot connect to local service: {error}")
        server_writer.close()
        return

    await _bridge(server_reader, server_writer, local_reader, local_writer)


async def _bridge(
    left_reader: asyncio.StreamReader,
    left_writer: asyncio.StreamWriter,
    right_reader: asyncio.StreamReader,
    right_writer: asyncio.StreamWriter,
) -> None:
    """Передавать байты одновременно в обоих направлениях до разрыва связи."""
    task1 = asyncio.create_task(_pipe(left_reader, right_writer))
    task2 = asyncio.create_task(_pipe(right_reader, left_writer))

    _, pending = await asyncio.wait(
        {task1, task2},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

    left_writer.close()
    right_writer.close()


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Копировать TCP-байты в одном направлении блоками до 64 КиБ."""
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
        writer.close()


if __name__ == "__main__":
    app()
