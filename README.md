# Port Tunnel

A reverse TCP tunneling service written in Python and asyncio.

It provides access to TCP services running behind NAT via a server with a public IP address, using separate connections for management and data transfer.

## Features

- Multiple simultaneous TCP tunnels
- Separate management and data connections
- Typed management protocol based on Pydantic
- Client authorization via persistent tokens
- Temporary `data_token` for each tunnel
- Heartbeat `ping/pong`
- Automatic deletion of an unavailable tunnel
- Release of the public port
- Closure of pending and active connections when the tunnel is deleted
- Limitation of the control message size
- Bidirectional transmission of arbitrary TCP traffic

UDP, TLS, and automatic client reconnection are not implemented in the current version.

## Local run

From the project root, run:

```powershell
uv sync --all-packages
```

### Server

```powershell
$env:PORT_TUNNEL_TOKENS='{"lev":"secret-token"}'

uv run --package port-tunnel-server python -m port_tunnel_server `
    --control-host 127.0.0.1 `
    --control-port 7000 `
    --public-host 127.0.0.1 `
    --heartbeat-interval 15 `
    --heartbeat-timeout 45
```

### Local service

```powershell
python -m http.server 8080
```

### Client

```powershell
$env:PORT_TUNNEL_TOKEN='secret-token'

uv run --package port-tunnel-client python -m port_tunnel_client `
    --server-host 127.0.0.1 `
    --client-id lev `
    --control-port 7000 `
    --local-port 8080 `
    --public-port 30001
```

### Check

```powershell
curl.exe http://127.0.0.1:30001
```

A detailed diagram of the components and connections is provided in [ARCHITECTURE.md](ARCHITECTURE.md).
