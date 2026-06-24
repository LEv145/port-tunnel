import abc
import typing as tp


class ABCTransmitter(abc.ABC):
    async def send_json(self, writer: tp.Any, message: dict[str, tp.Any]) -> None:
        pass

    async def read_json(self, reader: tp.Any) -> dict[str, tp.Any]:
        pass
