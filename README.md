# Port Tunnel

Сервис обратного TCP-туннелирования, написанный на Python и asyncio.

Он предоставляет доступ к TCP-сервисам, работающим за NAT, через сервер с общедоступным IP-адресом, используя отдельные соединения для управления и передачи данных.

## Возможности

- Несколько одновременных TCP-туннелей
- Отдельные управляющие и data-соединения
- Типизированный управляющий протокол на Pydantic
- Авторизация клиентов по постоянным токенам
- Временный `data_token` для каждого туннеля
- Heartbeat `ping/pong`
- Автоматическое удаление недоступного туннеля
- Освобождение публичного порта
- Закрытие pending- и active-соединений при удалении туннеля
- Ограничение размера управляющего сообщения
- Двунаправленная передача произвольного TCP-трафика

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
