from typing import Dict, List, Annotated, AsyncGenerator
from processor import Processor, AsyncProcessor
from pipeline import Pipeline, AsyncPipeline
import asyncio
import datetime
import random

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
                await asyncio.sleep(0.01)
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


class RandomRealizationChunkProcessor(AsyncProcessor):
    meta = {
        "name": "random_chunk",
        "input_type": Annotated[str, "Raw text document"],
        "output_type": Annotated[Dict[str, str], "Individual chunks realized at random times"]
    }

    async def _realize_chunk(self, chunk_text: str, chunk_id: int) -> Dict[str, str]:
        """Simulate chunk processing with random delay"""
        random_delay = random.uniform(0.5, 3.0)  # Random delay between 0.5 and 3 seconds
        print(f"Chunk {chunk_id} '{chunk_text[:20]}...' will realize in {random_delay:.2f} seconds")
        await asyncio.sleep(random_delay)
        realized_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        return {
            "chunk": chunk_text,
            "chunk_id": chunk_id,
            "realized_at": realized_time,
            "delay": f"{random_delay:.2f}s"
        }

    async def process(self, text: AsyncGenerator[str, None]) -> AsyncGenerator[Dict[str, str], None]:
        async for full_text in text:
            # Split the full text into sentences
            sentences = [s.strip() for s in full_text.split('.') if s.strip()]
            
            if not sentences:
                return
            
            print(f"Starting random realization of {len(sentences)} chunks...")
            
            # Create tasks for all chunks to process concurrently
            tasks = [
                asyncio.create_task(self._realize_chunk(sentence, i))
                for i, sentence in enumerate(sentences)
            ]
            
            # Yield chunks as they complete, not necessarily in order
            for completed_task in asyncio.as_completed(tasks):
                result = await completed_task
                print(f"✓ Chunk {result['chunk_id']} realized: '{result['chunk'][:30]}...'")
                yield result

# --- Demo Usage ---

# python -m processor_customized
if __name__ == "__main__":
    import asyncio
    import time
    
    async def async_demo():
        print("=== Original Async Pipeline Demo ===")
        async_pipeline = AsyncPipeline([
            AsyncChunkProcessor(),
            AsyncEntityProcessor()
        ])
        
        start_time = time.time()
        result = await async_pipeline.run("Alice went to Wonderland. Bob stayed home. Charlie visited Diana.")
        print("Async result:", result)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time:.2f} seconds\n")
    
    async def random_realization_demo():
        print("=== Random Realization Chunk Processor Demo ===")
        
        # Create a simple async generator to feed text
        async def text_generator():
            yield "Alice went to Wonderland. Bob stayed home. Charlie visited Diana. Eve went shopping. Frank cooked dinner. Grace read a book."
        
        processor = RandomRealizationChunkProcessor()
        
        print("Processing text with random realization times...\n")
        start_time = time.time()
        
        chunks = []
        async for chunk in processor.process(text_generator()):
            chunks.append(chunk)
            print(f"Received chunk {chunk['chunk_id']}: {chunk['chunk']} (realized at {chunk['realized_at']}, took {chunk['delay']})")
        
        end_time = time.time()
        print(f"\nTotal processing time: {end_time - start_time:.2f} seconds")
        print(f"Processed {len(chunks)} chunks in random order")
        
        # Show the order chunks were realized vs original order
        print("\nRealization order vs Original order:")
        for i, chunk in enumerate(chunks):
            print(f"  Realized #{i+1}: Chunk {chunk['chunk_id']} - '{chunk['chunk'][:40]}...'")
    
    async def sync_demo():
        print("=== Sync Pipeline Demo ===")
        pipeline = AsyncPipeline([
            RandomRealizationChunkProcessor(),
            AsyncEntityProcessor()
        ], max_concurrent_tasks=1)
        result = await pipeline.run("Alice went to Wonderland. Bob stayed home. Charlie visited Diana. Eve went shopping. Frank cooked dinner. Grace read a book.")
        print("Sync result:", result)
    
    # Run demos
    async def main():
        # await async_demo()
        # await random_realization_demo()
        import time
        start_time = time.time()
        await sync_demo()
        end_time = time.time()
        print(f"Time taken: {end_time - start_time} seconds")
    
    asyncio.run(main()) 