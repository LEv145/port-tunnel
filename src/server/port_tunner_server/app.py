import asyncio
import secrets
import logging
from functools import partial

from pydantic import BaseModel
import typer
import colorlog

from transmitters import ABCTransmitter, TCPTransmitter


_handler = colorlog.StreamHandler()
_handler.setFormatter(colorlog.ColoredFormatter("%(log_color)s%(levelname)s:%(name)s:%(message)s"))
_log = colorlog.getLogger(__name__)
_log.addHandler(_handler)
_log.setLevel(logging.INFO)


class TunnelState(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    control_writer: asyncio.StreamWriter | None = None
    pending_public_connections: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}


state = TunnelState()
app = typer.Typer()


@app.command()
def main(
    control_host: str = "0.0.0.0",
    control_port: int = 7000,
    public_host: str = "0.0.0.0",
    public_port: int = 30001,
) -> None:
    asyncio.run(async_main(**locals()))


async def async_main(
    control_host: str,
    control_port: int,
    public_host: str,
    public_port: int,
) -> None:
    transmitter = TCPTransmitter()

    control_server = await asyncio.start_server(
        partial(_handle_control_or_data, transmitter),
        control_host,
        control_port,
    )

    public_server = await asyncio.start_server(
        partial(_handle_public_connection, transmitter),
        public_host,
        public_port,
    )

    _log.info(f"[server] control listening on {control_host}:{control_port}")
    _log.info(f"[server] public listening on {public_host}:{public_port}")

    async with control_server, public_server:
        await asyncio.gather(
            control_server.serve_forever(),
            public_server.serve_forever(),
        )


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


async def _handle_control_or_data(
    transmitter: ABCTransmitter,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        message = await transmitter.read_json(reader)
    except Exception:
        writer.close()
        return

    message_type = message.get("type")

    if message_type == "register":
        state.control_writer = writer

        await transmitter.send_json(
            writer,
            {
                "type": "registered",
                "public_port": message.get("public_port"),
            }
        )

        _log.info("[control] client registered")

        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            if state.control_writer is writer:
                state.control_writer = None

    elif message_type == "data":
        connection_id = message.get("connection_id")

        public_pair = state.pending_public_connections.pop(connection_id, None)
        if public_pair is None:
            _log.info(f"[data] unknown connection_id={connection_id}")
            writer.close()
            return

        public_reader, public_writer = public_pair
        _log.info(f"[data] bridge started connection_id={connection_id}")

        await _bridge(public_reader, public_writer, reader, writer)

        _log.info(f"[data] bridge closed connection_id={connection_id}")

    else:
        _log.info(f"[server] unknown message type: {message_type}")
        writer.close()


async def _handle_public_connection(
    transmitter: ABCTransmitter,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    if state.control_writer is None:
        _log.info("[public] no client connected")
        writer.close()
        return

    connection_id = secrets.token_hex(8)
    state.pending_public_connections[connection_id] = (reader, writer)

    _log.info(f"[public] new connection_id={connection_id}")

    await transmitter.send_json(
        state.control_writer,
        {
            "type": "new_connection",
            "connection_id": connection_id,
        }
    )


if __name__ == "__main__":
    app()
