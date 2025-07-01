from typing import Dict, List, Annotated, AsyncGenerator
from processor import Processor, AsyncProcessor
from pipeline import Pipeline, AsyncPipeline
import asyncio
import datetime

class ChunkProcessor(Processor):
    meta = {
        "name": "chunk",
        "input_type": Annotated[str, "Raw text document"],
        "output_type": Annotated[List[Dict[str, str]], "List of chunks"]
    }

    def process(self, text: str) -> List[Dict[str, str]]:
        return [{"chunk": s.strip()} for s in text.split('.') if s.strip()]


class AsyncChunkProcessor(AsyncProcessor):
    meta = {
        "name": "chunk",
        "input_type": Annotated[str, "Raw text document"],
        "output_type": Annotated[Dict[str, str], "Individual chunks"]
    }

    async def process(self, text: AsyncGenerator[str, None]) -> AsyncGenerator[Dict[str, str], None]:
        async for full_text in text:
            # Split the full text into sentences and yield each as a chunk
            for sentence in full_text.split('.'):
                await asyncio.sleep(1)
                if sentence.strip():
                    yield {"chunk": sentence.strip()}


class EntityProcessor(Processor):
    meta = {
        "name": "entity",
        "input_type": Annotated[List[Dict[str, str]], "Chunks"],
        "output_type": Annotated[List[Dict[str, str]], "Extracted entities"]
    }

    def process(self, chunks: List[Dict[str, str]]) -> List[Dict[str, str]]:
        entities = []
        for chunk in chunks:
            if "Alice" in chunk.get("chunk", ""):
                entities.append({"entity": "Alice", "type": "PERSON"})
            elif "Bob" in chunk.get("chunk", ""):
                entities.append({"entity": "Bob", "type": "PERSON"})
            elif "Charlie" in chunk.get("chunk", ""):
                entities.append({"entity": "Charlie", "type": "PERSON"})
            elif "Diana" in chunk.get("chunk", ""):
                entities.append({"entity": "Diana", "type": "PERSON"})
            elif "Eve" in chunk.get("chunk", ""):
                entities.append({"entity": "Eve", "type": "PERSON"})

        return entities


class AsyncEntityProcessor(AsyncProcessor):
    meta = {
        "name": "entity",
        "input_type": Annotated[Dict[str, str], "Individual chunks"],
        "output_type": Annotated[Dict[str, str], "Extracted entities"]
    }

    async def process(self, chunks: AsyncGenerator[Dict[str, str], None]) -> AsyncGenerator[Dict[str, str], None]:
        async for chunk in chunks:
            print(f"Processing chunk: {chunk} at {datetime.datetime.now()}")
            await asyncio.sleep(2)
            chunk_text = chunk.get("chunk", "")
            if "Alice" in chunk_text:
                yield {"entity": "Alice", "type": "PERSON"}
            elif "Bob" in chunk_text:
                yield {"entity": "Bob", "type": "PERSON"}
            elif "Charlie" in chunk_text:
                yield {"entity": "Charlie", "type": "PERSON"}
            elif "Diana" in chunk_text:
                yield {"entity": "Diana", "type": "PERSON"}
            elif "Eve" in chunk_text:
                yield {"entity": "Eve", "type": "PERSON"}
            print(f"Processed chunk: {chunk} at {datetime.datetime.now()}")

# --- Demo Usage ---

if __name__ == "__main__":
    # Sync Pipeline Demo
    print("=== Sync Pipeline Demo ===")
    pipeline = Pipeline([
        ChunkProcessor(),
        EntityProcessor()
    ])

    result = pipeline.run("Alice went to Wonderland. Bob stayed home.")
    print("Sync result:", result)
    
    # Async Pipeline Demo
    print("\n=== Async Pipeline Demo ===")
    import asyncio
    
    async def async_demo():
        async_pipeline = AsyncPipeline([
            AsyncChunkProcessor(),
            AsyncEntityProcessor()
        ])
        
        import time
        start_time = time.time()
        result = await async_pipeline.run("Alice went to Wonderland. Bob stayed home. Charlie visited Diana.")
        print("Async result:", result)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")
    
    # Run the async demo
    asyncio.run(async_demo()) 