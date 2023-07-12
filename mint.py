import asyncio
from aws_utils import dynamodb_update, dynamodb_scan
from aleo import (
    create_account,
    scan_records,
    get_height,
    mint_leos,
    transfer_credits,
    split_credit,
    get_transaction_outputs,
    get_treasury_records,
    push_treasury_records,
    join_credits,
)
import env
from project_utils import record_to_amount, format_error


MINT_TOTAL_FEE = (
    env.MINT_LEOS_FEE + env.JOIN_CREDITS_FEE + env.TRANSFER_CREDITS_FEE
)


async def create_and_store_account():
    private_key, view_key, address = await create_account()
    await dynamodb_update(
        env.ALEO_ADDRESSES_TABLE,
        {"address": address},
        {
            "view_key": view_key,
            "private_key": private_key,
        },
    )


async def create_accounts_if_needed():
    addresses = await dynamodb_scan(env.ALEO_ADDRESSES_TABLE)
    addresses_to_create = env.WANTED_ADDRESSES - len(addresses)

    if addresses_to_create <= 0:
        return

    await asyncio.gather(
        *[create_and_store_account() for _ in range(addresses_to_create)]
    )


async def get_mint_requests(cur_height):
    height_limit = cur_height - env.REQUESTS_SCAN_HEIGHT_LIMIT
    requests = await dynamodb_scan(
        env.ALEO_MINT_REQUESTS_TABLE,
        filter_expression=f"(creation_block_height >= :height_limit or attribute_exists(scan_credits_output)) and attribute_not_exists(scan_join_output) and attribute_not_exists(not_possible)",
        ExpressionAttributeValues={":height_limit": {"N": str(height_limit)}},
    )
    return requests


async def mint_scan_records(request, cur_height):
    try:
        request_id = request["request_id"]
        creation_block_height = request["creation_block_height"]
        recipient_view_key = request["recipient_view_key"]
        scan_credits_output = request.get("scan_credits_output", None)
        request["scan_credits_output"] = await make_scan_credits(
            request_id,
            recipient_view_key,
            creation_block_height,
            cur_height,
            scan_credits_output,
        )
    except Exception as e:
        pass


async def handle_active_mint_request(request, cur_height):
    request_id = request["request_id"]
    recipient_view_key = request["recipient_view_key"]
    recipient_private_key = request["recipient_private_key"]
    user_address = request["user_address"]

    scan_credits_output = request.get("scan_credits_output", None)
    split_output = request.get("split_output", None)
    scan_split_output = request.get("scan_split_output", None)
    transfer_credits_output = request.get("transfer_credits_output", None)
    scan_transfer_output = request.get("scan_transfer_output", None)
    mint_leos_output = request.get("mint_leos_output", None)
    scan_mint_output = request.get("scan_mint_output", None)
    scan_former_output = request.get("scan_former_output", None)
    join_output = request.get("join_output", None)
    scan_join_output = request.get("scan_join_output", None)

    try:
        has_worked = not bool(split_output)
        split_output = await make_split(
            request_id,
            recipient_private_key,
            scan_credits_output["received_record"],
            split_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_split_output = await make_scan_split(
            request_id,
            split_output["tx_id"],
            recipient_view_key,
            scan_split_output,
        )
        has_worked = not bool(split_output)
        transfer_credits_output = await make_transfer_credits(
            request_id,
            recipient_private_key,
            scan_credits_output["received_amount"],
            scan_split_output["for_treasury_record"],
            scan_split_output["fee_record"],
            transfer_credits_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_output = await make_scan_transfer(
            request_id, transfer_credits_output["tx_id"], scan_transfer_output
        )
        has_worked = not bool(mint_leos_output)
        mint_leos_output = await make_mint_leos(
            request_id,
            user_address,
            scan_credits_output["received_amount"],
            scan_transfer_output["fee_record"],
            mint_leos_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_mint_output = await make_scan_mint(
            request_id,
            mint_leos_output["tx_id"],
            scan_mint_output,
        )
        scan_former_output = await make_scan_former(
            request_id,
            scan_former_output,
        )
        has_worked = not bool(join_output)
        join_output = await make_join(
            request_id,
            scan_former_output["payment_record"],
            scan_mint_output["fee_output_record"],
            scan_former_output["fee_record"],
            join_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_join_output = await make_scan_join(
            request_id,
            join_output["tx_id"],
            scan_join_output,
        )
    except Exception as e:
        print(format_error(e))
        return


async def make_scan_credits(
    request_id,
    recipient_view_key,
    creation_block_height,
    cur_height,
    scan_credits_output,
):
    if scan_credits_output:
        return scan_credits_output
    records = await scan_records(
        recipient_view_key,
        start=creation_block_height - 1,
        end=cur_height,
    )

    if not records:
        raise Exception("No records found.")
    received_record = records[0]
    received_amount = record_to_amount(received_record)

    if received_amount <= MINT_TOTAL_FEE:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"not_possible": True},
        )
        raise Exception("Not enough credits.")

    scan_credits_output = {
        "received_amount": received_amount,
        "received_record": received_record,
    }
    await dynamodb_update(
        env.ALEO_MINT_REQUESTS_TABLE,
        {"request_id": request_id},
        {"scan_credits_output": scan_credits_output},
    )
    return scan_credits_output


async def make_split(request_id, recipient_private_key, record, split_output):
    if split_output:
        return split_output
    try:
        tx_id = await split_credit(
            recipient_private_key,
            record,
            env.TRANSFER_CREDITS_FEE,
        )

        split_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"split_output": split_output},
        )
        return split_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"split_error": format_error(e)},
        )
        raise e


async def make_scan_split(
    request_id,
    transaction_id,
    recipient_view_key,
    scan_split_output,
):
    if scan_split_output:
        return scan_split_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id, recipient_view_key
        )
        split_records = all_records[f"{env.ALEO_CREDITS_PROGRAM_ID}/split"]
        if not split_records:
            raise Exception("No records found.")

        scan_split_output = {
            "fee_record": split_records[0],
            "for_treasury_record": split_records[1],
        }
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_split_output": scan_split_output},
        )
        return scan_split_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_split_error": format_error(e)},
        )
        raise e


async def make_transfer_credits(
    request_id,
    recipient_private_key,
    received_amount,
    amount_record,
    fee_record,
    transfer_credits_output,
):
    if transfer_credits_output:
        return transfer_credits_output
    try:
        tx_id = await transfer_credits(
            recipient_private_key,
            env.MINT_ACCOUNT_ADDRESS,
            received_amount - env.TRANSFER_CREDITS_FEE,
            amount_record,
            fee_record,
        )
        transfer_credits_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_credits_output": transfer_credits_output},
        )
        return transfer_credits_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_credits_error": format_error(e)},
        )
        raise e


async def make_scan_transfer(
    request_id,
    transaction_id,
    scan_transfer_output,
):
    if scan_transfer_output:
        return scan_transfer_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )
        transfer_records = all_records[
            f"{env.ALEO_CREDITS_PROGRAM_ID}/transfer_private"
        ]

        scan_transfer_output = {"fee_record": transfer_records[0]}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_output": scan_transfer_output},
        )
        return scan_transfer_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_error": format_error(e)},
        )
        raise e


async def make_mint_leos(
    request_id,
    user_address,
    received_amount,
    fee_record,
    mint_leos_output,
):
    if mint_leos_output:
        return mint_leos_output
    try:
        minted_leos = received_amount - MINT_TOTAL_FEE
        res = await mint_leos(
            env.MINT_ACCOUNT_PRIVATE_KEY, user_address, minted_leos, fee_record
        )
        mint_leos_output = {
            "tx_id": res,
            "minted_leos": minted_leos,
        }

        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"mint_leos_output": mint_leos_output},
        )
        return mint_leos_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"mint_leos_error": format_error(e)},
        )
        raise e


async def make_scan_mint(
    request_id,
    transaction_id,
    scan_mint_output,
):
    if scan_mint_output:
        return scan_mint_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )
        transfer_records = all_records[f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"]

        scan_mint_output = {"fee_output_record": transfer_records[0]}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_mint_output": scan_mint_output},
        )
        return scan_mint_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_mint_error": format_error(e)},
        )
        raise e


async def make_scan_former(
    request_id,
    scan_former_output,
):
    if scan_former_output:
        return scan_former_output
    try:
        payment_record, fee_record = await get_treasury_records()

        scan_former_output = {
            "payment_record": payment_record,
            "fee_record": fee_record,
        }
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_former_output": scan_former_output},
        )
        return scan_former_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_scan_former_error": format_error(e)},
        )
        raise e


async def make_join(
    request_id,
    payment_record,
    scan_fee_record,
    fee_record,
    join_output,
):
    if join_output:
        return join_output
    try:
        res = await join_credits(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            payment_record,
            scan_fee_record,
            fee_record,
        )

        join_output = {"tx_id": res}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"join_output": join_output},
        )
        return join_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"join_error": format_error(e)},
        )
        raise e


async def make_scan_join(
    request_id,
    transaction_id,
    scan_join_output,
):
    if scan_join_output:
        return scan_join_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )
        fee_records = all_records[f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"]
        joined_records = all_records[f"{env.ALEO_CREDITS_PROGRAM_ID}/join"]

        scan_join_output = {
            "fee_output_record": fee_records[0],
            "joined_record": joined_records[0],
        }
        await push_treasury_records(
            [
                scan_join_output["fee_output_record"],
                scan_join_output["joined_record"],
            ]
        )
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_join_output": scan_join_output},
        )
        return scan_join_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_join_error": format_error(e)},
        )
        raise e
