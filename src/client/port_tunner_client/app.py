import asyncio
import logging

import typer
import colorlog

from transmitters import ABCTransmitter, TCPTransmitter


_handler = colorlog.StreamHandler()
_handler.setFormatter(colorlog.ColoredFormatter("%(log_color)s%(levelname)s:%(name)s:%(message)s"))
_log = colorlog.getLogger(__name__)
_log.addHandler(_handler)
_log.setLevel(logging.INFO)

app = typer.Typer()


@app.command()
def main(
    server_host: str = typer.Option(),
    control_port: int = 7000,
    local_host: str = "127.0.0.1",
    local_port: int = typer.Option(),
    public_port: int = 30001,
) -> None:
    asyncio.run(async_main(**locals()))


async def async_main(
    server_host: str,
    local_host: str,
    local_port: int,
    control_port: int,
    public_port: int,
) -> None:
    transmitter = TCPTransmitter()

    reader, writer = await asyncio.open_connection(
        server_host,
        control_port,
    )

    await transmitter.send_json(writer, {
        "type": "register",
        "local_port": local_port,
        "public_port": public_port,
    })

    response = await transmitter.read_json(reader)
    _log.info(f"[client] registered: {response}")

    while True:
        message = await transmitter.read_json(reader)

        if message.get("type") == "new_connection":
            asyncio.create_task(
                _handle_new_connection(
                    transmitter=transmitter,
                    server_host=server_host,
                    control_port=control_port,
                    local_host=local_host,
                    local_port=local_port,
                    connection_id=message["connection_id"],
                )
            )
        else:
            _log.info(f"[client] unknown message: {message}")


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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


async def _bridge(
    left_reader: asyncio.StreamReader,
    left_writer: asyncio.StreamWriter,
    right_reader: asyncio.StreamReader,
    right_writer: asyncio.StreamWriter,
) -> None:
    task1 = asyncio.create_task(_pipe(left_reader, right_writer))
    task2 = asyncio.create_task(_pipe(right_reader, left_writer))

    done, pending = await asyncio.wait(
        {task1, task2},
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

    left_writer.close()
    right_writer.close()


async def _handle_new_connection(
    transmitter: ABCTransmitter,
    server_host: str,
    control_port: int,
    local_host: str,
    local_port: int,
    connection_id: str,
) -> None:
    _log.info(f"[client] new connection_id={connection_id}")

    server_reader, server_writer = await asyncio.open_connection(
        server_host,
        control_port,
    )

    await transmitter.send_json(server_writer, {
        "type": "data",
        "connection_id": connection_id,
    })

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


if __name__ == "__main__":
    app()
