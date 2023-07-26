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
from aleo import (
    get_height,
    get_recent_transitions,
    get_transaction_id,
    get_transaction,
    get_block_transactions,
)
import random

from aws_utils import dynamodb_get, dynamodb_update, dynamodb_scan

import env
import traceback
from project_utils import format_error


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


in_progress = False


async def sync():
    global in_progress
    if in_progress:
        return
    in_progress = True
    try:
        cur_height = await get_height()
        await create_accounts_if_needed()
        [
            mint_requests,
            burn_requests,
            mint_transfer_transactions,
            burn_transfer_transactions,
        ] = await asyncio.gather(
            get_mint_requests(cur_height),
            get_burn_requests(cur_height),
            get_recent_transfer_transactions_mint(cur_height),
            get_recent_transfer_transactions_burn(cur_height),
        )
        await asyncio.gather(
            asyncio.gather(
                *[
                    mint_scan_records(
                        request, cur_height, mint_transfer_transactions
                    )
                    for request in mint_requests
                ],
            ),
            asyncio.gather(
                *[
                    burn_scan_records(
                        request, cur_height, burn_transfer_transactions
                    )
                    for request in burn_requests
                ],
            ),
        )
        mint_requests = [
            mint_request
            for mint_request in mint_requests
            if mint_request.get("scan_pp_output") is not None
        ]
        burn_requests = [
            burn_request
            for burn_request in burn_requests
            if burn_request.get("scan_pp_output") is not None
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
        in_progress = False
    except Exception as e:
        in_progress = False
        print(traceback.format_exc())


async def get_recent_transfer_transactions_mint(cur_height):
    return await dynamodb_scan(
        env.KNOWN_TRANSACTION_IDS_TABLE,
        filter_expression="discovery_height >= :height_limit and attribute_not_exists(used_already)",
        ExpressionAttributeValues={
            ":height_limit": {
                "N": str(cur_height - env.REQUESTS_SCAN_HEIGHT_LIMIT)
            }
        },
    )


async def get_recent_transfer_transactions_burn(cur_height):
    return await dynamodb_scan(
        env.KNOWN_BURN_TRANSACTION_IDS_TABLE,
        filter_expression="discovery_height >= :height_limit and attribute_not_exists(used_already)",
        ExpressionAttributeValues={
            ":height_limit": {
                "N": str(cur_height - env.REQUESTS_SCAN_HEIGHT_LIMIT)
            }
        },
    )


async def periodic():
    await asyncio.gather(periodic_transactions(), periodic_sync())


async def periodic_sync():
    while True:
        try:
            await sync()
        except Exception as e:
            print(format_error(e))
        await asyncio.sleep(env.SYNC_TASK_PERIOD_S)


async def periodic_transactions():
    while True:
        try:
            await sync_transactions()
        except Exception as e:
            print(format_error(e))
        await asyncio.sleep(env.TRANSACTIONS_TASK_PERIOD_S)


async def sync_block(height):
    transactions = await get_block_transactions(height)
    to_execute = []
    for transaction in transactions:
        if (
            transaction.get("status") != "accepted"
            or transaction.get("type") != "execute"
        ):
            continue
        transaction = transaction.get("transaction")
        if not transaction or transaction.get("type") != "execute":
            continue
        transaction_id = transaction.get("id")

        execution = transaction.get("execution")
        if not execution:
            continue

        transitions = execution.get("transitions")
        if not transitions:
            continue

        to_execute += [
            sync_transition(transaction_id, transition, height)
            for transition in transitions
        ]

    await asyncio.gather(*to_execute)


async def sync_transition(transaction_id, transition, height):
    program_id = transition.get("program")
    if program_id == env.PRIVACY_PRIDE_PROGRAM_ID:
        known_transaction_ids_table = env.KNOWN_TRANSACTION_IDS_TABLE
        function_name = "transfer_private"
    elif program_id == env.ALEO_STORE_PROGRAM_ID:
        known_transaction_ids_table = env.KNOWN_BURN_TRANSACTION_IDS_TABLE
        function_name = "transfer_token_private"
    else:
        return

    if function_name != transition.get("function"):
        return

    outputs = transition.get("outputs")
    if not outputs:
        return
    encrypted_record = outputs[0].get("value")

    await dynamodb_update(
        known_transaction_ids_table,
        {"transaction_id": transaction_id},
        {
            "encrypted_record": encrypted_record,
            "discovery_height": height,
        },
    )


async def sync_transactions():
    cur_height = await get_height()
    last_known_block = (
        await dynamodb_get(
            env.KNOWN_BLOCKS_TABLE,
            {"block_height": 0},
        )
    ).block_height_value

    new_last_known_block = last_known_block

    while new_last_known_block <= cur_height:
        try:
            new_last_known_block += 1
            if new_last_known_block in env.BLOCKS_TO_IGNORE.split(","):
                continue
            await sync_block(new_last_known_block)
        except Exception as e:
            new_last_known_block -= 1
            print(format_error(e))
            break

    if new_last_known_block != last_known_block:
        await dynamodb_update(
            env.KNOWN_BLOCKS_TABLE,
            {"block_height": 0},
            {"block_height_value": new_last_known_block},
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(periodic())

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
