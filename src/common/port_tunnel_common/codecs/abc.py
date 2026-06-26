import abc
import asyncio


class ABCMessageCodec(abc.ABC):
    """Кодирует JSON-объекты в TCP-поток и декодирует их обратно."""

    @abc.abstractmethod
    async def send_json(self, writer: asyncio.StreamWriter, message: dict[str, object]) -> None:
        """Отправить один JSON-объект с префиксом длины."""

    @abc.abstractmethod
    async def read_json(self, reader: asyncio.StreamReader) -> dict[str, object]:
        """Прочитать один JSON-объект с префиксом длины."""
