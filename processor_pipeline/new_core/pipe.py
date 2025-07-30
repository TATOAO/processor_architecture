import asyncio
from logging import Logger
from .core_interfaces import PipeInterface
from typing import Any, Optional, Dict, AsyncGenerator
from pydantic import BaseModel


class PipeStatistics(BaseModel):
    """
    Statistics about the pipe.
    """
    historic_put_count: int = 0
    historic_get_count: int = 0


class AsyncPipe(PipeInterface):
    """
    Asynchronous pipe implementation using asyncio.Queue.

    This pipe implementation is designed to be used in asynchronous environments.
    It provides a thread-safe way to put and get data from the pipe.

    Attributes:
        queue (asyncio.Queue): The underlying queue for storing data.
    """
    def __init__(self, maxsize: int = -1, pipe_id: Optional[str] = None, logger: Logger = Logger("AsyncPipe")):
        ### core attributes
        self._pipe_id = pipe_id
        self.queue = asyncio.Queue(maxsize)
        self.logger = logger

        #### metadata
        self.metadata = {}
        self.statistics = PipeStatistics()


    async def put(self, data: Any) -> None:
        self.statistics.historic_put_count += 1
        await self.queue.put(data)

    async def get(self, timeout: Optional[float] = None) -> Any:
        self.statistics.historic_get_count += 1
        return await self.queue.get()
    
    async def __aiter__(self) -> AsyncGenerator[Any, None]:
        while True:
            data = await self.get()
            if data is None:
                break
            yield data

    async def is_empty(self) -> bool:
        return self.queue.empty()
    
    async def size(self) -> int:
        return self.queue.qsize()
    
    async def close(self) -> None:
        self.queue.put_nowait(None)

    async def statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the pipe.

        Returns:
            Dict[str, Any]: A dictionary containing the statistics of the pipe.
        """
        return self.statistics.model_dump()