from typing import Dict, List, Annotated
from processor import Processor
from pipeline import Pipeline

class ChunkProcessor(Processor):
    meta = {
        "name": "chunk",
        "input_type": Annotated[str, "Raw text document"],
        "output_type": Annotated[List[Dict[str, str]], "List of chunks"]
    }

    def process(self, text: str) -> List[Dict[str, str]]:
        return [{"chunk": s.strip()} for s in text.split('.') if s.strip()]

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
        return entities

# --- Demo Usage ---

if __name__ == "__main__":
    pipeline = Pipeline([
        ChunkProcessor(),
        EntityProcessor()
    ])

    result = pipeline.run("Alice went to Wonderland. Bob stayed home.")
    print(result) 