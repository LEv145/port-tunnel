"""CLI-точка входа клиентской части Port Tunnel."""

import asyncio
import logging
import os

import typer
from port_tunnel_common.codecs import ControlMessageCodec
from port_tunnel_common.setups.logging import setup_logging

from .client import TCPTunnelClient, TCPTunnelClientConfig


setup_logging(logging.INFO)

app = typer.Typer()


@app.command()
def main(
    server_host: str = typer.Option(),
    client_id: str = typer.Option(),
    control_port: int = 7000,
    local_host: str = "127.0.0.1",
    local_port: int = typer.Option(),
    public_port: int = 30001,
) -> None:
    """Запустить клиентский агент для одного TCP-сервиса."""
    token = os.environ.get("PORT_TUNNEL_TOKEN")

    if token is None:
        raise RuntimeError(
            "PORT_TUNNEL_TOKEN is not configured",
        )

    config = TCPTunnelClientConfig(
        server_host=server_host,
        control_port=control_port,
        client_id=client_id,
        token=token,
        local_host=local_host,
        local_port=local_port,
        public_port=public_port,
    )

    client = TCPTunnelClient(codec=ControlMessageCodec(), config=config)

    asyncio.run(client.run())


if __name__ == "__main__":
    app()
