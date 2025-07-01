"""
Example demonstrating dynamic processor creation using exec().

This example shows how to:
1. Create processor templates
2. Execute processor code from strings
3. Execute processor code from files
4. Use processors created via exec() in pipelines
"""

import os
import sys

# Add the processor_pipeline to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processor_pipeline import (
    execute_processor_code,
    execute_processor_code_from_file,
    create_processor_template,
    list_registered_processors,
    create_processor_from_registry,
    create_pipeline_from_config
)

def demonstrate_processor_template():
    """Demonstrate creating processor templates"""
    print("=== Processor Template Generation ===")
    
    # Generate a sync processor template
    sync_template = create_processor_template(
        name="data_formatter",
        input_type="dict",
        output_type="str",
        is_async=False
    )
    
    print("Generated sync processor template:")
    print(sync_template)
    print()
    
    # Generate an async processor template
    async_template = create_processor_template(
        name="async_validator",
        input_type="str", 
        output_type="bool",
        is_async=True
    )
    
    print("Generated async processor template:")
    print(async_template)

def demonstrate_exec_simple_processor():
    """Demonstrate creating a simple processor using exec()"""
    print("\n=== Creating Processor via exec() ===")
    
    # Define a simple processor code
    processor_code = '''
class GreetingProcessor(Processor):
    """A processor that adds greetings to text"""
    
    meta = {
        "name": "greeting_processor",
        "input_type": str,
        "output_type": str,
    }
    
    def __init__(self, greeting="Hello", **kwargs):
        super().__init__()
        self.greeting = greeting
    
    def process(self, data):
        return f"{self.greeting}, {data}!"
    
    def get_save_data(self, input_data, output_data, execution_id, step_index):
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "greeting_used": self.greeting,
            "input_text": input_data,
            "output_text": output_data
        })
        return base_data
'''
    
    # Execute the code and register the processor
    registered = execute_processor_code(processor_code)
    print(f"Registered processors: {registered}")
    
    # Test the processor
    processor = create_processor_from_registry("greeting_processor", greeting="Hi")
    result = processor.process("World")
    print(f"Processor result: '{result}'")

def demonstrate_exec_complex_processor():
    """Demonstrate creating a more complex processor with dependencies"""
    print("\n=== Creating Complex Processor via exec() ===")
    
    complex_code = '''
import re
import json

class TextAnalyzerProcessor(Processor):
    """Analyzes text and returns various statistics"""
    
    meta = {
        "name": "text_analyzer_processor",
        "input_type": str,
        "output_type": dict,
    }
    
    def __init__(self, include_words=True, include_sentences=True, **kwargs):
        super().__init__()
        self.include_words = include_words
        self.include_sentences = include_sentences
    
    def process(self, data):
        analysis = {
            "character_count": len(data),
            "character_count_no_spaces": len(data.replace(" ", "")),
        }
        
        if self.include_words:
            words = data.split()
            analysis.update({
                "word_count": len(words),
                "unique_words": len(set(word.lower() for word in words)),
                "average_word_length": sum(len(word) for word in words) / len(words) if words else 0
            })
        
        if self.include_sentences:
            sentences = re.split(r'[.!?]+', data)
            sentences = [s.strip() for s in sentences if s.strip()]
            analysis.update({
                "sentence_count": len(sentences),
                "average_sentence_length": sum(len(s) for s in sentences) / len(sentences) if sentences else 0
            })
        
        return analysis
    
    def get_save_data(self, input_data, output_data, execution_id, step_index):
        base_data = super().get_save_data(input_data, output_data, execution_id, step_index)
        base_data.update({
            "analysis_settings": {
                "include_words": self.include_words,
                "include_sentences": self.include_sentences
            },
            "full_analysis": output_data
        })
        return base_data
'''
    
    # Execute with additional globals for dependencies
    additional_globals = {
        're': __import__('re'),
        'json': __import__('json')
    }
    
    registered = execute_processor_code(complex_code, globals_dict=additional_globals)
    print(f"Registered processors: {registered}")
    
    # Test the complex processor
    analyzer = create_processor_from_registry("text_analyzer_processor", include_words=True, include_sentences=True)
    text = "Hello world! This is a test. How are you doing today?"
    result = analyzer.process(text)
    print(f"Text analysis result: {result}")

def demonstrate_exec_from_file():
    """Demonstrate executing processor code from a file"""
    print("\n=== Creating Processor from File ===")
    
    # Create a temporary processor file
    processor_file_content = '''
class NumberProcessorFromFile(Processor):
    """Processor that performs operations on numbers"""
    
    meta = {
        "name": "number_processor_from_file",
        "input_type": int,
        "output_type": dict,
    }
    
    def __init__(self, operation="square", **kwargs):
        super().__init__()
        self.operation = operation
    
    def process(self, data):
        if self.operation == "square":
            result = data ** 2
        elif self.operation == "cube":
            result = data ** 3
        elif self.operation == "double":
            result = data * 2
        else:
            result = data
        
        return {
            "input": data,
            "operation": self.operation,
            "result": result
        }
'''
    
    # Write to temporary file
    temp_filename = "temp_processor.py"
    with open(temp_filename, 'w') as f:
        f.write(processor_file_content)
    
    try:
        # Execute from file
        registered = execute_processor_code_from_file(temp_filename)
        print(f"Registered processors from file: {registered}")
        
        # Test the processor
        number_proc = create_processor_from_registry("number_processor_from_file", operation="cube")
        result = number_proc.process(5)
        print(f"Number processor result: {result}")
        
    finally:
        # Clean up temp file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            print(f"Cleaned up {temp_filename}")

def demonstrate_pipeline_with_exec_processors():
    """Demonstrate using exec-created processors in a pipeline"""
    print("\n=== Pipeline with exec() Created Processors ===")
    
    # Create a data preprocessing processor
    preprocessing_code = '''
class DataPreprocessor(Processor):
    """Preprocesses text data by cleaning and normalizing"""
    
    meta = {
        "name": "data_preprocessor",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data):
        # Remove extra whitespace and convert to lowercase
        cleaned = " ".join(data.split()).lower()
        # Remove special characters (keep letters, numbers, spaces)
        import re
        cleaned = re.sub(r'[^a-z0-9\\s]', '', cleaned)
        return cleaned
'''
    
    # Register the preprocessor
    execute_processor_code(preprocessing_code, globals_dict={'re': __import__('re')})
    
    # Create a pipeline configuration using both new and existing processors
    pipeline_config = [
        {"name": "data_preprocessor"},
        {"name": "greeting_processor", "params": {"greeting": "Processed"}},
        {"name": "text_analyzer_processor", "params": {"include_words": True, "include_sentences": False}}
    ]
    
    # Create and run the pipeline
    pipeline = create_pipeline_from_config(pipeline_config, "sync")
    
    input_data = "Hello World!!! This is a TEST with Special Characters @#$%"
    print(f"Input: {input_data}")
    
    result = pipeline.run(input_data)
    print(f"Pipeline result: {result}")

def demonstrate_safe_mode():
    """Demonstrate safe mode execution"""
    print("\n=== Safe Mode Execution ===")
    
    # This processor tries to use a potentially dangerous function
    safe_code = '''
class SafeProcessor(Processor):
    """A processor that works in safe mode"""
    
    meta = {
        "name": "safe_processor",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data):
        # This works in safe mode
        return data.upper()
'''
    
    # This will work in safe mode
    try:
        registered = execute_processor_code(safe_code, safe_mode=True)
        print(f"Safe processor registered: {registered}")
        
        safe_proc = create_processor_from_registry("safe_processor")
        result = safe_proc.process("hello safe world")
        print(f"Safe processor result: {result}")
        
    except Exception as e:
        print(f"Error in safe mode: {e}")
    
    # Demonstrate potentially unsafe code (commented out for safety)
    print("\nNote: Unsafe operations like file I/O, network calls, or system commands")
    print("are restricted in safe mode to prevent potential security issues.")

def main():
    """Main demonstration function"""
    print("Dynamic Processor Execution Demo")
    print("=" * 50)
    
    demonstrate_processor_template()
    demonstrate_exec_simple_processor()
    demonstrate_exec_complex_processor()
    demonstrate_exec_from_file()
    demonstrate_pipeline_with_exec_processors()
    demonstrate_safe_mode()
    
    print("\n" + "=" * 50)
    print("All registered processors:")
    processors = list_registered_processors()
    for name, info in processors.items():
        description = info.get('description', 'No description') or 'No description'
        print(f"  - {name}: {description[:60]}...")
    
    print("\nDemo completed! You can now:")
    print("1. Generate templates: create_processor_template()")
    print("2. Execute code: execute_processor_code(code_string)")
    print("3. Execute from files: execute_processor_code_from_file(filepath)")
    print("4. Use exec-created processors in pipelines normally")

if __name__ == "__main__":
    main() 