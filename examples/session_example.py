from processor_pipeline.core.processor import Processor, AsyncProcessor
from processor_pipeline.core.pipeline import Pipeline, AsyncPipeline
from typing import AsyncGenerator, Any

# Example 1: Synchronous Processors with Session
class DataCollector(Processor):
    meta = {
        "name": "data_collector",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data: str) -> str:
        # Store some metadata in the session
        self.session["original_length"] = len(data)
        self.session["processed_at"] = "data_collector"
        print(f"DataCollector: Stored original_length={len(data)} in session")
        return data.upper()

class DataAnalyzer(Processor):
    meta = {
        "name": "data_analyzer", 
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data: str) -> str:
        # Access data from previous processor via session
        original_length = self.session.get("original_length", "unknown")
        processed_at = self.session.get("processed_at", "unknown")
        
        print(f"DataAnalyzer: Retrieved from session - original_length={original_length}, processed_at={processed_at}")
        
        # Add more data to session
        self.session["word_count"] = len(data.split())
        self.session["analyzed_at"] = "data_analyzer"
        
        return f"Analyzed: {data} (words: {len(data.split())})"

class DataReporter(Processor):
    meta = {
        "name": "data_reporter",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data: str) -> str:
        # Access all session data
        original_length = self.session.get("original_length", "unknown")
        word_count = self.session.get("word_count", "unknown")
        processed_at = self.session.get("processed_at", "unknown")
        analyzed_at = self.session.get("analyzed_at", "unknown")
        
        print(f"DataReporter: Session data - original_length={original_length}, word_count={word_count}")
        print(f"DataReporter: Processing history - {processed_at} -> {analyzed_at}")
        
        return f"Final Report: {data} | Original chars: {original_length} | Words: {word_count}"

# Example 2: Asynchronous Processors with Session
class AsyncDataCollector(AsyncProcessor):
    meta = {
        "name": "async_data_collector",
        "input_type": str,
        "output_type": str,
    }
    
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for item in data:
            # Store metadata in session
            self.session["original_length"] = len(item)
            self.session["processed_at"] = "async_data_collector"
            print(f"AsyncDataCollector: Stored original_length={len(item)} in session")
            yield item.upper()

class AsyncDataAnalyzer(AsyncProcessor):
    meta = {
        "name": "async_data_analyzer",
        "input_type": str,
        "output_type": str,
    }
    
    async def process(self, data: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
        async for item in data:
            # Access session data
            original_length = self.session.get("original_length", "unknown")
            processed_at = self.session.get("processed_at", "unknown")
            
            print(f"AsyncDataAnalyzer: Retrieved from session - original_length={original_length}, processed_at={processed_at}")
            
            # Add more data to session
            self.session["word_count"] = len(item.split())
            self.session["analyzed_at"] = "async_data_analyzer"
            
            yield f"Analyzed: {item} (words: {len(item.split())})"

def run_sync_example():
    """Run synchronous pipeline example with session sharing."""
    print("=== Synchronous Pipeline with Session ===")
    
    # Create processors
    collector = DataCollector()
    analyzer = DataAnalyzer()
    reporter = DataReporter()
    
    # Create pipeline
    pipeline = Pipeline([collector, analyzer, reporter])
    
    # Run pipeline
    result = pipeline.run("Hello world from processor pipeline")
    print(f"Final result: {result}")
    print(f"Final session state: {pipeline.session}")
    print()

async def run_async_example():
    """Run asynchronous pipeline example with session sharing."""
    print("=== Asynchronous Pipeline with Session ===")
    
    # Create async processors
    collector = AsyncDataCollector()
    analyzer = AsyncDataAnalyzer()
    
    # Create async pipeline
    pipeline = AsyncPipeline([collector, analyzer])
    
    # Run pipeline
    results = await pipeline.run("Hello world from async processor pipeline")
    print(f"Final results: {results}")
    print(f"Final session state: {pipeline.session}")
    print()

if __name__ == "__main__":
    # Run synchronous example
    run_sync_example()
    
    # Run asynchronous example
    import asyncio
    asyncio.run(run_async_example()) 