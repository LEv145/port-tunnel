import asyncio
from dataclasses import dataclass, field


@dataclass(slots=True)
class PendingTCPConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@dataclass(slots=True)
class RegisteredTCPTunnel:
    tunnel_id: str
    public_host: str
    public_port: int
    control_writer: asyncio.StreamWriter
    public_server: asyncio.Server
    pending_public_connections: dict[str, PendingTCPConnection] = field(default_factory=dict)


class TCPTunnelRegistry:
    def __init__(self) -> None:
        self._tunnels_by_id: dict[str, RegisteredTCPTunnel] = {}
        self._tunnel_ids_by_public_port: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def add(self, tunnel: RegisteredTCPTunnel) -> None:
        async with self._lock:
            if tunnel.public_port in self._tunnel_ids_by_public_port:
                raise ValueError(f"public port already used: {tunnel.public_port}")

            self._tunnels_by_id[tunnel.tunnel_id] = tunnel
            self._tunnel_ids_by_public_port[tunnel.public_port] = tunnel.tunnel_id

    async def remove(self, tunnel_id: str) -> RegisteredTCPTunnel | None:
        async with self._lock:
            tunnel = self._tunnels_by_id.pop(tunnel_id, None)
            if tunnel is None:
                return None

            self._tunnel_ids_by_public_port.pop(tunnel.public_port, None)
            return tunnel

    async def get_by_id(self, tunnel_id: str) -> RegisteredTCPTunnel | None:
        async with self._lock:
            return self._tunnels_by_id.get(tunnel_id)

    async def put_pending_public_connection(
        self,
        tunnel_id: str,
        connection_id: str,
        connection: PendingTCPConnection,
    ) -> bool:
        async with self._lock:
            tunnel = self._tunnels_by_id.get(tunnel_id)
            if tunnel is None:
                return False

            tunnel.pending_public_connections[connection_id] = connection
            return True

    async def pop_pending_public_connection(
        self,
        tunnel_id: str,
        connection_id: str,
    ) -> PendingTCPConnection | None:
        async with self._lock:
            tunnel = self._tunnels_by_id.get(tunnel_id)
            if tunnel is None:
                return None

            return tunnel.pending_public_connections.pop(connection_id, None)
