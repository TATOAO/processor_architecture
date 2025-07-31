
import asyncio
import time

async def input_generator(name: str, count: int, delay: float):
    """
    An asynchronous generator to simulate a task that yields items periodically.
    
    Args:
        name: The base name for the yielded items (e.g., 'A').
        count: The number of items to yield.
        delay: The time in seconds to wait between yielding each item.
    """
    # print(f"-> Starting generator {name} (delay: {delay}s)")
    for i in range(1, count + 1):
        await asyncio.sleep(delay)
        yield f"{name}{i}"
    # print(f"<- Finished generator {name}")

## ----------------------------------------------------------------
## ASAP Mode Processor
## ----------------------------------------------------------------

async def asap_processor(*async_gens):
    """
    Yields items from multiple async generators as soon as they become available.
    The order is determined by which item is ready first (fastest task wins).
    """
    # Create an iterator for each generator
    iterators = [gen.__aiter__() for gen in async_gens]
    
    # Create a mapping from a running task to its source iterator.
    # This allows us to fetch the next item from the correct generator later.
    tasks_to_iter = {
        asyncio.create_task(it.__anext__()): it for it in iterators
    }

    while tasks_to_iter:
        # Wait for the first task to complete
        done, _ = await asyncio.wait(
            tasks_to_iter.keys(), return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            # Get the iterator that this completed task belonged to
            iterator = tasks_to_iter.pop(task)
            try:
                result = task.result()
                yield result
                # Schedule the next item from the same generator
                new_task = asyncio.create_task(iterator.__anext__())
                tasks_to_iter[new_task] = iterator
            except StopAsyncIteration:
                # This generator is exhausted, so we do nothing and let it finish.
                pass

## ----------------------------------------------------------------
## Ordered Mode Processor
## ----------------------------------------------------------------

async def ordered_processor(*async_gens):
    """
    Executes generators concurrently but yields their results in the order
    of the input generators. This matches your example where results from
    faster generators are buffered while waiting for slower ones to finish.
    """
    queues = [asyncio.Queue() for _ in async_gens]

    async def _producer(generator, queue):
        """Helper coroutine to run a generator and put its items into a queue."""
        async for item in generator:
            await queue.put(item)
        await queue.put(None)  # Put a sentinel value to mark the end

    # Start all producer tasks concurrently
    producer_tasks = [
        asyncio.create_task(_producer(gen, q))
        for gen, q in zip(async_gens, queues)
    ]

    # Consume from the queues one by one, in the original order
    for i, q in enumerate(queues):
        while True:
            item = await q.get()
            if item is None:  # Sentinel reached, this generator is done
                # Ensure the producer task has fully completed
                await producer_tasks[i]
                break
            yield item

## ----------------------------------------------------------------
## Main Demonstration
## ----------------------------------------------------------------

async def main():
    """Runs a demonstration of both processing modes."""
    
    print("This script demonstrates two ways to process items from concurrent generators.")
    
    # --- ASAP Mode Demo ---
    print("\n" + "-"*50)
    print("## ⚡ ASAP Mode")
    print("Yields items as soon as they are ready, fastest first.")
    print("Expected: B1, A1, B2, A2, A3")
    print("-"*20)
    
    start_time = time.monotonic()
    
    async for item in asap_processor(
        input_generator("A", 3, 0.5),  # Slower generator
        input_generator("B", 2, 0.3)   # Faster generator
    ):
        elapsed = time.monotonic() - start_time
        print(f"Received {item} at {elapsed:.2f} seconds")

    # --- Ordered Mode Demo ---
    print("\n" + "-"*50)
    print("## 順番待ちモード (Ordered Mode)")
    print("Yields all items from generator A, then all from generator B.")
    print("Expected: A1, A2, A3, then B1 and B2 immediately.")
    print("-"*20)

    start_time = time.monotonic()

    async for item in ordered_processor(
        input_generator("A", 3, 0.5),
        input_generator("B", 2, 0.3)
    ):
        elapsed = time.monotonic() - start_time
        print(f"Received {item} at {elapsed:.2f} seconds")
    print("-"*50)


# python -m processor_pipeline.new_core.gemeni_try
if __name__ == "__main__":
    asyncio.run(main())