import asyncio
from aws_utils import dynamodb_update, dynamodb_scan
from aleo import (
    create_account,
    scan_records, 
    get_height, 
    mint_leos, 
    transfer_credits,
    credits_to_leo
)
import env
from boto3.dynamodb.conditions import Attr
import time


async def create_and_store_account():
    private_key, view_key, address = create_account()
    await dynamodb_update(
        env.ALEO_ADDRESSES_TABLE,
        {"address": {'S': address}},
        {
            "view_key": {'Value': {'S': view_key}},
            "private_key": {'Value': {'S': private_key}},
        },
    )


async def create_accounts_if_needed():
    addresses = await dynamodb_scan(env.ALEO_ADDRESSES_TABLE)
    addresses_to_create = env.WANTED_ADDRESSES - len(addresses)

    if addresses_to_create <= 0:
        return
    
    await asyncio.gather(*[
        create_and_store_account()
        for _ in range(addresses_to_create)
    ])


async def scan_mint_request_addresses():
    cur_height = await get_height()
    height_limit = cur_height - env.MINT_REQUESTS_SCAN_HEIGHT_LIMIT
    requests = await dynamodb_scan(
        env.ALEO_MINT_REQUESTS_TABLE,
        filter_expression=Attr('creation_block_height').gte(height_limit).And(
            Attr('received_credits').eq(False)
        ).And(
            Attr('sent_credits').eq(True)
        ),
    )
    await asyncio.gather(*[
        handle_active_mint_request(request, cur_height)
        for request in requests
    ])

request_in_progress = {}


async def handle_active_mint_request(request, cur_height):
    request_id = request['request_id']['S']
    creation_block_height = request['creation_block_height']['N']
    recipient_address = request['recipient_address']['S']
    recipient_view_key = request['recipient_view_key']['S']
    recipient_private_key = request['recipient_private_key']['S']

    user_address = request['user_address']['S']
    if request_in_progress.get(request_id, False):
        return
    
    request_in_progress[request_id] = True

    try:
        records = await scan_records(
            recipient_view_key,
            begin=creation_block_height-1,
            end=cur_height,
        )

        if not records:
            del request_in_progress[request_id]
            return
        record = records[0]
        amount = int(record.split(',')[1].replace(' ', '')[13:-11])
        leo_amount = credits_to_leo(amount)

        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {
                "request_id": {'S': request_id},
            },
            {
                "received_credits": {'Value': {'BOOL': True}},
                "amount": {'Value': {'N': str(amount)}},
                "record": {'Value': {'S': record}},
            }
        )

        await asyncio.gather(
            make_credit_transfer(
                request_id,
                recipient_private_key,
                amount,
                record
            ),
            make_leos_mint(
                request_id,
                user_address,
                leo_amount
            )
        )
    except Exception as e:
        print(e)
        del request_in_progress[request_id]
        return


async def make_credit_transfer(
    request_id, recipient_private_key, amount, record
):
    res = await transfer_credits(
        recipient_private_key,
        env.TREASURY_ADDRESS,
        amount,
        record,
        fee_record
    )
    await dynamodb_update(
        env.ALEO_MINT_REQUESTS_TABLE,
        {
            "request_id": {'S': request_id},
        },
        {
            "transfered_credits": {'Value': {'BOOL': True}}
        }
    )


async def make_leos_mint(request_id, user_address, amount, record):
    res = await mint_leos(
        user_address,
        amount
    )
    await dynamodb_update(
        env.ALEO_MINT_REQUESTS_TABLE,
        {
            "request_id": {'S': request_id},
        },
        {
            "minted_leos": {'Value': {'BOOL': True}},
            "leos_amount": {'Value': {'N': str(amount)}},
        }
    )