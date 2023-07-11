import asyncio
from mint import (
    create_accounts_if_needed,
    get_mint_requests,
    handle_active_mint_request,
    mint_scan_records,
)
from burn import (
    get_burn_requests,
    handle_active_burn_request,
    burn_scan_records,
)
from aleo import get_height

import env
import traceback
from project_utils import format_error

rec = True


def merge_requests(burn_requests, mint_requests):
    burn_requests = [
        {
            **burn_request,
            "type": "burn",
        }
        for burn_request in burn_requests
    ]
    mint_requests = [
        {
            **mint_request,
            "type": "mint",
        }
        for mint_request in mint_requests
    ]
    requests = sorted(
        burn_requests + mint_requests,
        key=lambda request: request["creation_block_height"],
        reverse=True,
    )
    return requests


async def sync():
    rec = not rec
    try:
        cur_height = await get_height()
        await create_accounts_if_needed()
        [mint_requests, burn_requests] = await asyncio.gather(
            get_mint_requests(cur_height),
            get_burn_requests(cur_height),
        )
        asyncio.gather(
            asyncio.gather(
                *[
                    mint_scan_records(request, cur_height)
                    for request in mint_requests
                ],
            ),
            asyncio.gather(
                *[
                    burn_scan_records(request, cur_height)
                    for request in burn_requests
                ],
            ),
        )
        mint_requests = [
            mint_request
            for mint_request in mint_requests
            if mint_request.get("scan_credits_output") is not None
        ]
        burn_requests = [
            burn_request
            for burn_request in burn_requests
            if burn_request.get("transfer_leos_output") is not None
        ]
        requests = merge_requests(burn_requests, mint_requests)
        for request in requests:
            try:
                if request["type"] == "mint":
                    await handle_active_mint_request(request, cur_height)
                else:
                    await handle_active_burn_request(request, cur_height)
            except Exception as e:
                print(format_error(e))

    except Exception as e:
        print(traceback.format_exc())


async def periodic():
    while True:
        await sync()
        await asyncio.sleep(env.TASK_PERIOD_S)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(periodic())

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
