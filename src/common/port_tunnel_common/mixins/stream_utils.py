import asyncio
import contextlib


class StreamUtilsMixin:
    async def _close_writer(
        self,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Закрыть TCP-поток и дождаться освобождения транспорта."""
        writer.close()

        with contextlib.suppress(ConnectionError, RuntimeError):
            await writer.wait_closed()
