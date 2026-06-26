# Port Tunnel

`port-tunnel` — учебный сервис обратного TCP-туннелирования на Python 3.14+.

Он позволяет опубликовать локальное TCP-приложение через сервер с публичным IP-адресом без ручной настройки
проброса портов на маршрутизаторе клиента.

## Возможности

- несколько одновременных TCP-туннелей;
- отдельные управляющие и data-соединения;
- типизированный управляющий протокол на Pydantic;
- авторизация клиентов по постоянным токенам;
- временный `data_token` для каждого туннеля;
- heartbeat `ping/pong`;
- автоматическое удаление недоступного туннеля;
- освобождение публичного порта;
- закрытие pending- и active-соединений при удалении туннеля;
- ограничение размера управляющего сообщения;
- двунаправленная передача произвольного TCP-трафика.

UDP, TLS и автоматическое переподключение клиента в текущей версии не реализованы.

## Локальный запуск

Из корня проекта выполните:

```powershell
uv sync --all-packages
```

### Сервер

```powershell
$env:PORT_TUNNEL_TOKENS='{"lev":"secret-token"}'

uv run --package port-tunnel-server python -m port_tunnel_server `
    --control-host 127.0.0.1 `
    --control-port 7000 `
    --public-host 127.0.0.1 `
    --heartbeat-interval 15 `
    --heartbeat-timeout 45
```

### Локальный сервис

```powershell
python -m http.server 8080
```

### Клиент

```powershell
$env:PORT_TUNNEL_TOKEN='secret-token'

uv run --package port-tunnel-client python -m port_tunnel_client `
    --server-host 127.0.0.1 `
    --client-id lev `
    --control-port 7000 `
    --local-port 8080 `
    --public-port 30001
```

### Проверка

```powershell
curl.exe http://127.0.0.1:30001
```

Подробная схема компонентов и соединений приведена в [ARCHITECTURE.md](ARCHITECTURE.md).
