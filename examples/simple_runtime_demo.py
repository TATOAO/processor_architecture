"""
Simple demonstration of runtime processor creation using exec().

This example shows the most common use case: creating a processor from a code string.
"""

import sys
import os

# Add the processor_pipeline to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processor_pipeline import (
    execute_processor_code,
    create_processor_from_registry,
    create_pipeline_from_config,
    list_registered_processors
)

def main():
    print("Simple Runtime Processor Demo")
    print("=" * 40)
    
    # 1. Create a simple processor using exec()
    print("Step 1: Creating a processor using exec()")
    
    processor_code = '''
class UpperCaseProcessor(Processor):
    """Converts text to uppercase"""
    
    meta = {
        "name": "uppercase_processor",
        "input_type": str,
        "output_type": str,
    }
    
    def process(self, data):
        return data.upper()
'''
    
    # Execute the code - this registers the processor automatically
    registered = execute_processor_code(processor_code)
    print(f"✓ Registered processors: {registered}")
    
    # 2. Use the processor
    print("\nStep 2: Using the processor")
    processor = create_processor_from_registry("uppercase_processor")
    result = processor.process("hello world!")
    print(f"Input: 'hello world!' → Output: '{result}'")
    
    # 3. Create another processor
    print("\nStep 3: Creating a number processor")
    
    number_processor_code = '''
class SquareProcessor(Processor):
    """Squares a number"""
    
    meta = {
        "name": "square_processor", 
        "input_type": int,
        "output_type": int,
    }
    
    def process(self, data):
        return data ** 2
'''
    
    execute_processor_code(number_processor_code)
    
    square_proc = create_processor_from_registry("square_processor")
    result = square_proc.process(5)
    print(f"Input: 5 → Output: {result}")
    
    # 4. Show all registered processors
    print("\nStep 4: All registered processors")
    processors = list_registered_processors()
    for name, info in processors.items():
        if name:  # Skip None entries
            print(f"  - {name}: {info.get('description', 'No description')}")
    
    print("\nDemo completed! 🎉")
    print("\nKey takeaways:")
    print("• Use execute_processor_code() to create processors from strings")
    print("• Processors are automatically registered and can be used immediately")
    print("• Define your processor class with proper meta information")
    print("• Use create_processor_from_registry() to instantiate processors")

if __name__ == "__main__":
    main() 