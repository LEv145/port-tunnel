"""CLI-точка входа серверной части Port Tunnel."""

import os
import asyncio
import logging
import json
from typing import Any

import typer
from port_tunnel_common.codecs import ControlMessageCodec
from port_tunnel_common.setups.logging import setup_logging

from .authentication import StaticTokenAuthenticator
from .server import TCPTunnelServer, TCPTunnelServerConfig


setup_logging(logging.INFO)
_log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def main(
    control_host: str = "0.0.0.0",
    control_port: int = 7000,
    public_host: str = "0.0.0.0",
) -> None:
    """Запустить сервер управления TCP-туннелями."""
    client_tokens = _load_client_tokens()
    asyncio.run(
        async_main(
            control_host=control_host,
            control_port=control_port,
            public_host=public_host,
            client_tokens=client_tokens,
        ),
    )


async def async_main(
    control_host: str,
    control_port: int,
    public_host: str,
    client_tokens: dict[str, str],
) -> None:
    """Собрать зависимости сервера и передать управление event loop."""
    config = TCPTunnelServerConfig(
        control_host=control_host,
        control_port=control_port,
        public_host=public_host,
    )
    server = TCPTunnelServer(
        config=config,
        codec=ControlMessageCodec(),
        authenticator=StaticTokenAuthenticator(tokens=client_tokens),
    )
    await server.run()


def _load_client_tokens() -> dict[str, str]:
    """Загрузить разрешённые токены из `PORT_TUNNEL_TOKENS`."""
    raw_tokens = os.environ.get("PORT_TUNNEL_TOKENS")
    if raw_tokens is None:
        raise RuntimeError("PORT_TUNNEL_TOKENS is not configured")

    parsed: Any = json.loads(raw_tokens)
    if not isinstance(parsed, dict):
        raise RuntimeError("PORT_TUNNEL_TOKENS must contain a JSON object")

    tokens: dict[str, str] = {}

    for client_id, token in parsed.items():
        if not isinstance(client_id, str) or not isinstance(token, str):
            raise RuntimeError("PORT_TUNNEL_TOKENS must map string client IDs to string tokens")

        if not client_id or not token:
            raise RuntimeError("Client IDs and tokens must not be empty")

        tokens[client_id] = token

    return tokens


if __name__ == "__main__":
    app()
