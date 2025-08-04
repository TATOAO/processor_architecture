#!/usr/bin/env python3
"""
Test script to verify that both import patterns work correctly.
"""

def test_old_import():
    """Test the old import pattern: from processor_pipeline import AsyncProcessor"""
    print("Testing old import pattern...")
    try:
        from processor_pipeline import AsyncProcessor
        print("✓ Successfully imported AsyncProcessor from processor_pipeline")
        print(f"  Type: {type(AsyncProcessor)}")
        print(f"  Module: {AsyncProcessor.__module__}")
        return True
    except Exception as e:
        print(f"✗ Failed to import AsyncProcessor from processor_pipeline: {e}")
        return False

def test_new_import():
    """Test the new import pattern: from processor_pipeline.new_core import AsyncProcessor"""
    print("\nTesting new import pattern...")
    try:
        from processor_pipeline.new_core import AsyncProcessor
        print("✓ Successfully imported AsyncProcessor from processor_pipeline.new_core")
        print(f"  Type: {type(AsyncProcessor)}")
        print(f"  Module: {AsyncProcessor.__module__}")
        return True
    except Exception as e:
        print(f"✗ Failed to import AsyncProcessor from processor_pipeline.new_core: {e}")
        return False

# python -m examples.test_import
if __name__ == "__main__":
    test_old_import()
    test_new_import()