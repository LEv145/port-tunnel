import asyncio
import contextlib
import secrets
import logging
from functools import partial
from typing import Any

from transmitters import ABCTransmitter

from .tcp_tunnel_registry import TCPTunnelRegistry, RegisteredTCPTunnel, PendingTCPConnection


_log = logging.getLogger(__name__)


class TCPTunnelServer:
    def __init__(
        self,
        *,
        transmitter: ABCTransmitter,
        control_host: str,
        control_port: int,
        public_host: str,
    ) -> None:
        self._transmitter = transmitter
        self._control_host = control_host
        self._control_port = control_port
        self._public_host = public_host
        self._registry = TCPTunnelRegistry()

    async def run(self) -> None:
        control_server = await asyncio.start_server(
            self._handle_control_or_data,
            self._control_host,
            self._control_port,
        )

        _log.info(f"[server] control listening on {self._control_host}:{self._control_port}")

        async with control_server:
            await control_server.serve_forever()

    async def _handle_control_or_data(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            message = await self._transmitter.read_json(reader)
        except Exception:
            await self._close_writer(writer)
            return

        message_type = message.get("type")

        if message_type == "register":
            await self._handle_register(message, writer)
            return

        if message_type == "data":
            await self._handle_data(message, reader, writer)
            return

        _log.info(f"[server] unknown message type: {message_type}")
        await self._close_writer(writer)

    async def _handle_register(
        self,
        message: dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        public_port_raw = message.get("public_port")
        if not isinstance(public_port_raw, int):
            await self._transmitter.send_json(writer, {
                "type": "error",
                "message": "public_port must be int",
            })
            await self._close_writer(writer)
            return

        tunnel_id = secrets.token_hex(8)

        try:
            public_server = await asyncio.start_server(
                partial(self._handle_public_connection, tunnel_id),
                self._public_host,
                public_port_raw,
            )
        except OSError as error:
            await self._transmitter.send_json(writer, {
                "type": "error",
                "message": f"cannot listen public port {public_port_raw}: {error}",
            })
            await self._close_writer(writer)
            return

        tunnel = RegisteredTCPTunnel(
            tunnel_id=tunnel_id,
            public_host=self._public_host,
            public_port=public_port_raw,
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

        await self._transmitter.send_json(writer, {
            "type": "registered",
            "tunnel_id": tunnel_id,
            "public_port": public_port_raw,
        })

        _log.info(f"[control] tunnel registered tunnel_id={tunnel_id} public_port={public_port_raw}")

        async with public_server:
            public_task = asyncio.create_task(public_server.serve_forever())

            try:
                while not writer.is_closing():
                    await asyncio.sleep(3600)
            finally:
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
        tunnel = await self._registry.get_by_id(tunnel_id)
        if tunnel is None:
            _log.info(f"[public] unknown tunnel_id={tunnel_id}")
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
        tunnel_id = message.get("tunnel_id")
        connection_id = message.get("connection_id")

        if not isinstance(tunnel_id, str) or not isinstance(connection_id, str):
            _log.info("[data] invalid tunnel_id or connection_id")
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
        tunnel = await self._registry.remove(tunnel_id)
        if tunnel is None:
            return

        tunnel.public_server.close()
        await tunnel.public_server.wait_closed()

        for connection_id, connection in list(tunnel.pending_public_connections.items()):
            _log.info(f"[control] close pending connection_id={connection_id}")
            await self._close_writer(connection.writer)

        _log.info(f"[control] tunnel unregistered tunnel_id={tunnel_id}")

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
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
        task1 = asyncio.create_task(self._pipe(left_reader, right_writer))
        task2 = asyncio.create_task(self._pipe(right_reader, left_writer))

        done, pending = await asyncio.wait(
            {task1, task2},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

        await self._close_writer(left_writer)
        await self._close_writer(right_writer)

    async def _close_writer(self, writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(ConnectionError, RuntimeError):
            await writer.wait_closed()
