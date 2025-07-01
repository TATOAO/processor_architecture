from typing import List, Any, get_origin, get_args, Annotated, AsyncGenerator
from processor import Processor, AsyncProcessor

# --- Type Checking Utility ---
def match_types(output_type: Any, input_type: Any) -> bool:
    def resolve(t):
        if get_origin(t) is Annotated:
            return get_args(t)[0]
        return t

    return resolve(output_type) == resolve(input_type)

class Pipeline:
    def __init__(self, processors: List[Processor]):
        self.processors = processors
        self._check_compatibility()

    def _check_compatibility(self):
        for i in range(len(self.processors) - 1):
            p1, p2 = self.processors[i], self.processors[i + 1]
            out_type = p1.get_meta()["output_type"]
            in_type = p2.get_meta()["input_type"]
            if not match_types(out_type, in_type):
                raise TypeError(
                    f"Incompatible types: '{p1.meta['name']}' outputs {out_type}, "
                    f"but '{p2.meta['name']}' expects {in_type}"
                )

    def run(self, data: Any) -> Any:
        for processor in self.processors:
            data = processor.process(data)
        return data


class AsyncPipeline(Pipeline):
    def __init__(self, processors: List[AsyncProcessor]):
        super().__init__(processors)

    
    async def run(self, input_data: Any) -> List[Any]:
        stream = self._to_async_stream([input_data])  # start with a 1-item stream
        for processor in self.processors:
            stream = processor.process(stream)  # each returns a new async generator

        # Collect final output
        results = []
        async for item in stream:
            results.append(item)
        return results

    def _to_async_stream(self, items: List[Any]) -> AsyncGenerator[Any, None]:
        async def gen():
            for item in items:
                yield item
        return gen()