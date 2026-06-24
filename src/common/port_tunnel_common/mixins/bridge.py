import asyncio


class BridgeMixin:
    async def _bridge(
        self,
        left_reader: asyncio.StreamReader,
        left_writer: asyncio.StreamWriter,
        right_reader: asyncio.StreamReader,
        right_writer: asyncio.StreamWriter,
    ) -> None:
        """Передавать TCP-байты одновременно в обоих направлениях."""
        tasks = {
            asyncio.create_task(self._pipe(left_reader, right_writer)),
            asyncio.create_task(self._pipe(right_reader, left_writer)),
        }

        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

    async def _pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Копировать TCP-байты в одном направлении."""
        try:
            while True:
                data = await reader.read(64 * 1024)

                if not data:
                    return

                writer.write(data)
                await writer.drain()
        except ConnectionError:
            return
