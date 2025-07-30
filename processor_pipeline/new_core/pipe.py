import asyncio
import uuid
from logging import Logger
from .core_interfaces import PipeInterface, PipeMeta
from typing import Any, Optional, Dict, AsyncGenerator, List, Tuple
from pydantic import BaseModel
from collections import deque



class PipeStatistics(BaseModel):
    """
    Statistics about the pipe.
    """
    historic_put_count: int = 0
    historic_get_count: int = 0


class AsyncPipe(PipeInterface, metaclass=PipeMeta):
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


    @classmethod
    def generate_random_message_id(cls, data: Any) -> str:
        return f"{hash(data)}_{uuid.uuid4()}"

    async def put(self, data: Any) -> None:
        self.statistics.historic_put_count += 1
        if data is None:
            await self.queue.put(None)
            return
        message_id = self.generate_random_message_id(data)
        await self.queue.put((message_id, data))

    async def get(self, timeout: Optional[float] = None) -> Any:
        self.statistics.historic_get_count += 1
        data = await self.queue.get()
        if data is None:
            return None
        message_id, data = data
        return (message_id, data)

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

class BufferPipe(AsyncPipe):
    """
    Buffer pipe implementation using asyncio.Queue.
    """
    def __init__(self, maxsize: int = -1, pipe_id: Optional[str] = None, logger: Logger = Logger("AsyncPipe"), buffer_size: int = 100):
        super().__init__(maxsize, pipe_id, logger)
        self.buffer_size = buffer_size
        self.buffer_map = {}

        # store id lists
        self.messages_ids = []
        # store id: data
        self.buffer = {}
        # save message id to list of messages id that are in the buffer
        self.buffer_map = {}
    

    async def put(self, data: Any) -> None:
        self.statistics.historic_put_count += 1

        if data is None:
            await self.queue.put(None)
            return

        message_id = self.generate_random_message_id(data)

        self.messages_ids.append(message_id)
        self.buffer_map[message_id] = self.messages_ids[-self.buffer_size-1:-1]
        self.buffer[message_id] = data

        await self.queue.put((message_id, data))
    
    
    async def peek(self, message_id: str, n: int = 1) -> List[Tuple[str, Any]]:

        number_to_peek = min(self.buffer_size, n)

        # get the last n messages ids
        last_n_messages_ids = self.buffer_map[message_id][-number_to_peek:]
        # get the data for the last n messages ids
        data = [self.buffer[message_id] for message_id in last_n_messages_ids]
        return list(zip(last_n_messages_ids, data))
    


# python -m processor_pipeline.new_core.pipe
if __name__ == "__main__":
    print(BufferPipe.generate_random_message_id(1))
    