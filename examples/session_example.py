from typing import Any, AsyncGenerator
from processor_pipeline.core.processor import Processor, AsyncProcessor
from processor_pipeline.core.pipeline import Pipeline, AsyncPipeline


class FetchDataProcessor(AsyncProcessor):
    meta = {
        "name": "FetchDataProcessor",
        "input_type": Any,
        "output_type": str,
    }

    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[str, None]:
        # Ignore input data for this example
        yield '1'
        yield '2'
        yield '3'


class CollectDataProcessor(AsyncProcessor):
    meta = {
        "name": "CollectDataProcessor",
        "input_type": str,
        "output_type": list,
    }

    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[list, None]:
        result = []
        async for item in data:
            result.append(item)
        
        self.session['result'] = result

        yield 'a'
        yield 'b'
        yield 'c'



class PrintDataProcessor(AsyncProcessor):
    meta = {
        "name": "PrintDataProcessor",
        "input_type": list,
        "output_type": None,
    }

    async def process(self, data: AsyncGenerator[list, None]) -> AsyncGenerator[None, None]:

        print(f"Session result: {self.session['result']}")

        async for item in data:
            print(f"Received: {item}")
            print(f"Session result: {self.session['result']}")
            yield 'aa'


# 
if __name__ == "__main__":

    async def main():
        pipeline = AsyncPipeline([
            FetchDataProcessor(),
            CollectDataProcessor(),
            PrintDataProcessor(),
        ])

        async for item in pipeline.astream('no use'):
            print(f"main: {item}")
            print(f"main: Session result: {pipeline.session['result']}")
    
    import asyncio
    asyncio.run(main())
        