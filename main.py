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


async def sync_transition(
    transition_id,
    height,
    known_transaction_ids_table,
    known_transition_ids_table,
    program_id,
):
    function_name = (
        "transfer_private"
        if program_id == env.PRIVACY_PRIDE_PROGRAM_ID
        else "transfer_token_private"
    )
    transaction_id = await get_transaction_id(transition_id)
    if not transaction_id:
        await dynamodb_update(
            known_transition_ids_table,
            {"transition_id": transition_id},
            {"known_already": True},
        )
        return
    transaction = await get_transaction(transaction_id)
    if not transaction:
        return
    transitions = transaction.get("execution").get("transitions")
    if not transitions or transitions[0].get("function") != function_name:
        return await dynamodb_update(
            known_transition_ids_table,
            {"transition_id": transition_id},
            {"known_already": True},
        )
    encrypted_record = transitions[0].get("outputs")[0].get("value")

    await asyncio.gather(
        dynamodb_update(
            known_transaction_ids_table,
            {"transaction_id": transaction_id},
            {
                "encrypted_record": encrypted_record,
                "discovery_height": height,
            },
        ),
        dynamodb_update(
            known_transition_ids_table,
            {"transition_id": transition_id},
            {"known_already": True},
        ),
    )


async def sync_transactions(
    program_id,
    known_transaction_ids_table,
    known_transition_ids_table,
):
    [transition_ids, cur_height] = await asyncio.gather(
        get_recent_transitions(program_id), get_height()
    )
    known_transitions = await asyncio.gather(
        *[
            dynamodb_get(
                known_transition_ids_table,
                {"transition_id": transition_id},
            )
            for transition_id in transition_ids
        ],
    )
    unknown_transition_ids = [
        transition_id
        for [transition_id, known_transition] in zip(
            transition_ids, known_transitions
        )
        if known_transition is None
    ]

    await asyncio.gather(
        *[
            sync_transition(
                transition_id,
                cur_height,
                known_transaction_ids_table,
                known_transition_ids_table,
                program_id,
            )
            for transition_id in unknown_transition_ids
        ]
    )

    await asyncio.gather(
        *[
            dynamodb_update(
                known_transition_ids_table,
                {"transition_id": transition_id},
                {"known_already": True},
            )
            for transition_id in unknown_transition_ids
        ],
    )


async def periodic():
    await asyncio.gather(periodic_transactions(), periodic_sync())


async def periodic_sync():
    while True:
        await sync()
        await asyncio.sleep(env.SYNC_TASK_PERIOD_S)


async def periodic_transactions():
    while True:
        await sync_transactions(
            env.PRIVACY_PRIDE_PROGRAM_ID,
            env.KNOWN_TRANSACTION_IDS_TABLE,
            env.KNOWN_TRANSITION_IDS_TABLE,
        )
        await asyncio.sleep(1 + random.random())
        await sync_transactions(
            env.ALEO_STORE_PROGRAM_ID,
            env.KNOWN_BURN_TRANSACTION_IDS_TABLE,
            env.KNOWN_BURN_TRANSITION_IDS_TABLE,
        )
        await asyncio.sleep(env.TRANSACTIONS_TASK_PERIOD_S)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(periodic())

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
