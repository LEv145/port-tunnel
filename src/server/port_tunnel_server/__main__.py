import asyncio
import logging

import typer
from transmitters import TCPTransmitter

from .tcp_tunnel_server import TCPTunnelServer
from .setups.logging import setup_logging


setup_logging(logging.INFO)
_log = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def main(
    control_host: str = "0.0.0.0",
    control_port: int = 7000,
    public_host: str = "0.0.0.0",
) -> None:
    asyncio.run(
        async_main(
            control_host=control_host,
            control_port=control_port,
            public_host=public_host,
        )
    )


async def async_main(
    control_host: str,
    control_port: int,
    public_host: str,
) -> None:
    server = TCPTunnelServer(
        transmitter=TCPTransmitter(),
        control_host=control_host,
        control_port=control_port,
        public_host=public_host,
    )
    await server.run()


if __name__ == "__main__":
    app()
