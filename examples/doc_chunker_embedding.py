from processor_pipeline import AsyncPipeline, AsyncProcessor
import asyncio
import random
import os
import json
import datetime
from typing import AsyncGenerator, Any, Dict

# Custom callback that saves all processing data to log.log
def log_saver_callback(processor: Any, input_data: Any, output_data: Any, 
                      execution_id: str, step_index: int, *args, **kwargs) -> None:
    """
    Custom callback function that saves all processing data to log.log file.
    Now saves complete middle products including full chunks and vectors.
    """
    save_data = processor.get_save_data(input_data, output_data, execution_id, step_index)
    
    # Add more detailed information - save full data instead of truncating
    save_data.update({
        "detailed_input": str(input_data) if input_data is not None else None,
        "detailed_output": output_data,  # Save full output data including chunks and vectors
        "output_data_type": str(type(output_data).__name__),
        "processor_step": step_index,
    })
    
    # For specific processor types, add more structured data
    if hasattr(processor, 'meta') and processor.meta.get('name') == 'text_chunker_processor':
        if isinstance(output_data, list):
            save_data.update({
                "all_chunks": output_data,  # Save all chunks
                "chunk_count": len(output_data),
                "chunk_lengths": [len(chunk) for chunk in output_data]
            })
    
    elif hasattr(processor, 'meta') and processor.meta.get('name') == 'vector_generator_processor':
        if isinstance(output_data, dict):
            save_data.update({
                "chunk_vector_pair": output_data,  # Save individual chunk and vector
                "vector_dimensions": len(output_data.get("vector", [])) if output_data.get("vector") else 0,
                "chunk_index": output_data.get("chunk_index"),
                "individual_processing_time": output_data.get("processing_time", 0)
            })
    
    # Save to log.log file
    log_entry = f"[{datetime.datetime.now().isoformat()}] {json.dumps(save_data, default=str, indent=2)}\n"
    
    with open("log.log", "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(f"Logged step {step_index}: {processor.meta.get('name', 'unknown')} processing")


class TxtToTextProcessor(AsyncProcessor):
    """Processor that reads .txt files and returns their content as plain text"""
    
    meta = {
        "name": "txt_to_text_processor",
        "input_type": str,  # File path
        "output_type": str,  # Plain text
    }
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[str, None]:
        async for file_path in data:
            if not file_path.endswith('.txt'):
                raise ValueError(f"Expected .txt file, got: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read text file content
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    text_content = file.read()
                yield text_content
            except UnicodeDecodeError:
                # Try with different encoding if UTF-8 fails
                with open(file_path, 'r', encoding='latin-1') as file:
                    text_content = file.read()
                yield text_content
    
    def get_save_data(self, input_data: Any, output_data: Any, execution_id: str, step_index: int) -> Dict[str, Any]:
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "input_file_path": str(input_data) if input_data else None,
            "output_text_length": len(output_data) if output_data else 0,
            "output_preview": output_data[:200] if output_data else None,
            "full_text_content": output_data,  # Save complete text content
        })
        return base_data


class TextChunkerProcessor(AsyncProcessor):
    """Processor that chunks text with fixed window size and overlapping"""
    
    meta = {
        "name": "text_chunker_processor", 
        "input_type": str,  # Plain text
        "output_type": list,  # List of text chunks
    }
    
    def __init__(self, window_size: int = 500, overlap: int = 100):
        self.window_size = window_size
        self.overlap = overlap
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[list, None]:
        async for text in data:
            chunks = self._chunk_text(text, self.window_size, self.overlap)
            yield chunks
    
    def _chunk_text(self, text: str, window_size: int, overlap: int) -> list:
        """Split text into overlapping chunks"""
        if not text:
            return []
        
        chunks = []
        start = 0
        text_length = len(text)
        
        while start < text_length:
            end = min(start + window_size, text_length)
            chunk = text[start:end]
            chunks.append(chunk)
            
            # Move start position for next chunk (with overlap)
            start += window_size - overlap
            
            # Break if we've reached the end
            if end == text_length:
                break
        
        return chunks
    
    def get_save_data(self, input_data: Any, output_data: Any, execution_id: str, step_index: int) -> Dict[str, Any]:
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "input_text_length": len(input_data) if input_data else 0,
            "num_chunks_generated": len(output_data) if output_data else 0,
            "window_size": self.window_size,
            "overlap": self.overlap,
            "first_chunk_preview": output_data[0][:100] if output_data else None,
        })
        return base_data


class VectorGeneratorProcessor(AsyncProcessor):
    """Processor that generates vectors from text chunks using fake HTTP requests"""
    
    meta = {
        "name": "vector_generator_processor",
        "input_type": list,  # List of text chunks  
        "output_type": dict,  # Dict with chunk and vector per yield
    }
    
    async def process(self, data: AsyncGenerator[Any, None]) -> AsyncGenerator[dict, None]:
        async for chunks in data:
            async for vector_result in self._generate_vectors_for_chunks(chunks):
                yield vector_result
    
    async def after_process(self, input_data: Any, output_data: Any, execution_id: str, step_index: int, *args, **kwargs) -> None:
        """
        Override this method to define what should be done after processing data.
        By default, does nothing. And nothing is returned.
        """
        print(f"After process: saving shits: {output_data}")
    
    async def _generate_vectors_for_chunks(self, chunks: list) -> AsyncGenerator[dict, None]:
        """Generate vectors for each chunk using fake HTTP requests with asyncio.as_completed"""
        
        # Create coroutines for all chunks
        chunk_tasks = [
            self._process_single_chunk(i, chunk, len(chunks))
            for i, chunk in enumerate(chunks)
        ]
        
        # Process chunks as they complete
        for coro in asyncio.as_completed(chunk_tasks):
            result = await coro
            yield result
    
    async def _process_single_chunk(self, chunk_index: int, chunk: str, total_chunks: int) -> dict:
        """Process a single chunk and return its vector"""
        # Simulate HTTP request with random delay
        processing_time = await self._fake_http_request(chunk)
        
        # Generate random vector (5 dimensions) using only float
        vector = [random.random() for _ in range(5)]
        
        print(f"Generated vector for chunk {chunk_index+1}/{total_chunks} (took {processing_time:.3f}s)")
        
        return {
            "chunk_index": chunk_index,
            "chunk": chunk,
            "vector": vector,
            "processing_time": processing_time
        }
    
    async def _fake_http_request(self, text: str) -> float:
        """Simulate HTTP request to generate vector from text"""
        # Random delay to simulate network request
        delay = random.random()  # 0-1 seconds
        await asyncio.sleep(delay)
        return delay
    
    def get_save_data(self, input_data: Any, output_data: Any, execution_id: str, step_index: int) -> Dict[str, Any]:
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "num_input_chunks": len(input_data) if input_data else 0,
            "chunk_index": output_data.get("chunk_index") if output_data else None,
            "processing_time": output_data.get("processing_time") if output_data else 0,
            "vector_dimension": len(output_data.get("vector", [])) if output_data else 0,
        })
        return base_data


async def main():
    """Main function to demonstrate the complete pipeline"""
    
    # Clear the log file at the start
    if os.path.exists("log.log"):
        os.remove("log.log")
    
    print("Starting Document Processing Pipeline...")
    print("=" * 50)
    
    # Create processors
    txt_processor = TxtToTextProcessor()
    chunker_processor = TextChunkerProcessor(window_size=500, overlap=100)  
    vector_processor = VectorGeneratorProcessor()
    
    # Create pipeline
    pipeline = AsyncPipeline([
        txt_processor,
        chunker_processor, 
        vector_processor
    ])
    
    sample_txt_path = "examples/sample.txt"
    
    try:
        # Run the pipeline
        print(f"Processing file: {sample_txt_path}")
        results = await pipeline.run(
            input_data=sample_txt_path,
            callback=log_saver_callback
        )
        
        print("\n" + "=" * 50)
        print("Pipeline completed successfully!")
        print(f"Final results:")
        
        # Count vector results and calculate total processing time
        vector_results = [r for r in results if isinstance(r, dict) and "vector" in r]
        total_processing_time = sum(r.get("processing_time", 0) for r in vector_results)
        
        if vector_results:
            print(f"- Generated {len(vector_results)} vectors (5-dim each)")
            print(f"- Total processing time: {total_processing_time:.3f}s")
            print(f"- Vector example: {vector_results[0]['vector']}")
        else:
            print("- No vector results found")
        
        print(f"\nAll intermediate processing data saved to: log.log")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")


# python examples/doc_chunker_embedding.py
if __name__ == "__main__":
    asyncio.run(main())