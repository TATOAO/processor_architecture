import asyncio


async def task1():
    await asyncio.sleep(1)
    return "task1"

async def task2():
    await asyncio.sleep(4)
    return "task2"

async def task3():
    await asyncio.sleep(3)
    return "task3"


async def main():
    task1 = asyncio.create_task(task1())
    task2 = asyncio.create_task(task2())
    task3 = asyncio.create_task(task3())
    # await asyncio.gather(task1, task2, task3)


    # for task in asyncio.as_completed([task1, task2, task3]):
    #     result = await task
    #     print(result)

    for task in [task1, task2, task3]:
        result = await task
        print(result)


# python -m examples.draft_how_to_return_in_order
if __name__ == "__main__":
    asyncio.run(main())