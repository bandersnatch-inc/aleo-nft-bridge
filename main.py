import asyncio
from tasks import (
    create_accounts_if_needed,

)
from aleo import scan_records
import env


async def sync():
    #await create_accounts_if_needed()
    scan_res = await scan_records("AViewKey1hKNmqi83PpwjCTSSUCZT2rX3p3jcktNXzJZnk6rSqq4b", start=120376, end=120378)

    print(scan_res)
    

async def periodic():
    while True:
        await sync()
        await asyncio.sleep(env.TASK_PERIOD_S)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(periodic())

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass