import asyncio
import uuid
from loguru import logger
from .core_interfaces import PipeInterface, PipeMeta
from typing import Any, Optional, Dict, AsyncGenerator, List, Tuple, Set
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
    meta = {
        "name": "AsyncPipe"
    }
    def __init__(self, maxsize: int = -1, pipe_id: Optional[str] = None, logger=logger):
        ### core attributes
        if pipe_id is None:
            self._pipe_id = f"pipe_{str(uuid.uuid4())}"
        else:
            self._pipe_id = pipe_id

        self.queue = asyncio.Queue(maxsize)
        self.logger = logger or logger.bind(name="AsyncPipe")
        # ensure logger has default session_id to avoid formatting issues when not bound
        try:
            self.logger = self.logger.bind(session_id="-")
        except Exception:
            pass
        
        # Observer/peek mechanism
        self._observers: Dict[str, asyncio.Queue] = {}
        self._observer_lock = asyncio.Lock()

        #### metadata
        self.metadata = {}
        self.statistics = PipeStatistics()


    @classmethod
    def generate_random_message_id(cls, data: Any) -> str:
        try:
            return f"{hash(data)}_{uuid.uuid4()}"
        except Exception as e:
            logger.debug(f"Error generating message id: {e}: {data}")
            logger.debug(f"Error generating message id: {e}")
            return f"{type(data)}_{uuid.uuid4()}"

    async def put(self, data: Any) -> None:
        self.statistics.historic_put_count += 1
        if data is None:
            await self.queue.put(None)
            # Also notify all observers of the None (end of stream)
            async with self._observer_lock:
                for observer_queue in self._observers.values():
                    await observer_queue.put(None)
            return
            
        message_id = self.generate_random_message_id(data)
        self.logger.debug(f"[{self._pipe_id}] [PUT] data: {data} with message_id: {message_id}")
        
        # Put data in main queue
        await self.queue.put((message_id, data))
        
        # Broadcast to all observers
        async with self._observer_lock:
            for observer_queue in self._observers.values():
                await observer_queue.put((message_id, data))

    async def get(self, timeout: Optional[float] = None) -> Any:

        self.statistics.historic_get_count += 1
        data = await self.queue.get()
        if data is None:
            return None
        message_id, data = data
        self.logger.debug(f"[{self._pipe_id}] [GET] data: {data} with message_id: {message_id}")
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
        # Also close all observer queues
        async with self._observer_lock:
            for observer_queue in self._observers.values():
                observer_queue.put_nowait(None)

    async def register_observer(self, observer_id: Optional[str] = None) -> str:
        """Register a new observer and return its ID for peeking"""
        if observer_id is None:
            observer_id = f"observer_{str(uuid.uuid4())}"
        
        async with self._observer_lock:
            if observer_id in self._observers:
                raise ValueError(f"Observer {observer_id} already exists")
            self._observers[observer_id] = asyncio.Queue()
        
        self.logger.debug(f"[{self._pipe_id}] Registered observer: {observer_id}")
        return observer_id

    async def unregister_observer(self, observer_id: str) -> None:
        """Unregister an observer"""
        async with self._observer_lock:
            if observer_id in self._observers:
                # Close the observer queue
                self._observers[observer_id].put_nowait(None)
                del self._observers[observer_id]
                self.logger.debug(f"[{self._pipe_id}] Unregistered observer: {observer_id}")

    async def peek_aiter(self, observer_id: Optional[str] = None) -> AsyncGenerator[Any, None]:
        """Create an async iterator for peeking without consuming from main queue"""
        if observer_id is None:
            observer_id = await self.register_observer()
        
        if observer_id not in self._observers:
            raise ValueError(f"Observer {observer_id} not found")
        
        observer_queue = self._observers[observer_id]
        
        try:
            while True:
                data = await observer_queue.get()
                if data is None:
                    break
                yield data
        finally:
            # Clean up observer when done
            await self.unregister_observer(observer_id)

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
    meta = {
        "name": "BufferPipe"
    }
    def __init__(self, maxsize: int = -1, pipe_id: Optional[str] = None, logger=logger, buffer_size: int = 100):
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
            # Also notify all observers of the None (end of stream)
            async with self._observer_lock:
                for observer_queue in self._observers.values():
                    await observer_queue.put(None)
            return

        message_id = self.generate_random_message_id(data)

        self.messages_ids.append(message_id)
        self.buffer_map[message_id] = self.messages_ids[-self.buffer_size-1:-1]
        self.buffer[message_id] = data

        # Put data in main queue
        await self.queue.put((message_id, data))
        
        # Broadcast to all observers
        async with self._observer_lock:
            for observer_queue in self._observers.values():
                await observer_queue.put((message_id, data))
    
    
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
    