import asyncio
import time
from processor_pipeline.new_core.processor import AsyncProcessor
from processor_pipeline.new_core.pipe import AsyncPipe
from typing import Any

class AsyncProcessorWithBackgroundTask(AsyncProcessor):
    meta = {
        "name": "AsyncProcessorWithBackgroundTask",
        "input_pipe_type": "BufferPipe",
        "output_pipe_type": "BufferPipe",
    }

    async def process(self, data: Any, *args, **kwargs) -> Any:
        for i in data:
            print(i)
            yield (i)

# python -m processor_pipeline.new_core.tests.test_dynamic_adding_background_task_to_asyncprocessor
if __name__ == "__main__":
    async def main():

        input_pipe = AsyncPipe(pipe_id="input_pipe_test1")
        output_pipe = AsyncPipe(pipe_id="output_pipe_test1")

        start_time = time.time()

        processor = AsyncProcessorWithBackgroundTask()
        processor.register_input_pipe(input_pipe)
        processor.register_output_pipe(output_pipe)



        async def background_task():
            await asyncio.sleep(10)
            print("background task completed")

        async def intake_task():
            for i in range(10):
                await asyncio.sleep(1)
                asyncio.create_task(input_pipe.put([i]))

                if i == 5:
                    processor.add_background_task(asyncio.create_task(background_task()))

            await input_pipe.put(None)

        intake_task = asyncio.create_task(intake_task())
        await processor.execute()



        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")
        await intake_task


    asyncio.run(main())