# python -m processor_pipeline.new_core.test_registory_meta
if __name__ == "__main__":
    # have to import in the runtime to register the processor
    from .test import TestProcessor

    from .core_interfaces import ProcessorMeta
    for processor_class in ProcessorMeta.registry: 
        print(processor_class)


    # print(TestProcessor.meta)
    print(ProcessorMeta.registry.get("TestProcessor"))
    print(ProcessorMeta.registry.get("TestProcessor").meta)
