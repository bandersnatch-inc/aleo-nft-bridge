import asyncio
from aws_utils import dynamodb_update, dynamodb_scan
from aleo import (
    create_account,
    transfer_credits,
    get_transaction_outputs,
    get_treasury_records,
    push_treasury_records,
    decrypt_record,
    transfer_pp,
    mint_private,
    encode_string64,
)
import env
from project_utils import record_to_amount, format_error, record_to_pp_data


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
        filter_expression=f"(creation_block_height >= :height_limit or attribute_exists(scan_pp_output)) and attribute_not_exists(scan_mint_output) and attribute_not_exists(not_possible)",
        ExpressionAttributeValues={":height_limit": {"N": str(height_limit)}},
    )
    return requests


async def mint_scan_records(request, cur_height, mint_transfer_transactions):
    try:
        request_id = request["request_id"]
        creation_block_height = request["creation_block_height"]
        recipient_view_key = request["recipient_view_key"]
        scan_pp_output = request.get("scan_pp_output", None)
        request["scan_pp_output"] = await make_scan_credits(
            request_id,
            recipient_view_key,
            creation_block_height,
            cur_height,
            mint_transfer_transactions,
            scan_pp_output,
        )
    except Exception as e:
        pass


async def handle_active_mint_request(request, cur_height):
    request_id = request["request_id"]
    recipient_view_key = request["recipient_view_key"]
    recipient_private_key = request["recipient_private_key"]
    user_address = request["user_address"]
    recipient_address = request["recipient_address"]

    scan_pp_output = request.get("scan_pp_output", None)
    scan_former_output = request.get("scan_former_output", None)
    transfer_credits_fees_output = request.get(
        "transfer_credits_fees_output", None
    )
    scan_transfer_credits_fees_output = request.get(
        "scan_transfer_credits_fees_output", None
    )
    transfer_pp_output = request.get("transfer_pp_output", None)
    scan_transfer_pp_output = request.get("scan_transfer_pp_output", None)
    mint_output = request.get("mint_output", None)
    scan_mint_output = request.get("scan_mint_output", None)

    try:
        scan_former_output = await make_scan_former(
            request_id, scan_former_output
        )
        has_worked = not bool(transfer_credits_fees_output)
        transfer_credits_fees_output = await make_transfer_credits_fees(
            request_id,
            recipient_address,
            scan_former_output["payment_record"],
            scan_former_output["fee_record"],
            transfer_credits_fees_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_credits_fees_output = (
            await make_scan_transfer_credits_fees(
                request_id,
                transfer_credits_fees_output["tx_id"],
                recipient_view_key,
                scan_transfer_credits_fees_output,
            )
        )
        has_worked = not bool(transfer_pp_output)
        transfer_pp_output = await make_transfer_pp(
            request_id,
            recipient_private_key,
            scan_pp_output["received_record"],
            scan_transfer_credits_fees_output["transfer_output_record"],
            transfer_pp_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_pp_output = await make_scan_transfer_pp(
            request_id,
            scan_pp_output["pp_data"]["token_number"],
            transfer_pp_output["tx_id"],
            scan_transfer_pp_output,
        )
        has_worked = not bool(mint_output)
        mint_output = await make_mint(
            request_id,
            scan_former_output["collection_record"],
            scan_pp_output["pp_data"],
            user_address,
            scan_transfer_credits_fees_output["payment_output_record"],
            mint_output,
        )
        if has_worked:
            await asyncio.sleep(15)

        scan_mint_output = await make_scan_mint(
            request_id,
            mint_output["tx_id"],
            scan_transfer_credits_fees_output["fee_output_record"],
            scan_mint_output,
        )

    except Exception as e:
        print(format_error(e))
        return


async def decrypt_transaction_record(transaction, recipient_view_key):
    return {
        "transaction_id": transaction["transaction_id"],
        "record": await decrypt_record(
            transaction["encrypted_record"], recipient_view_key
        ),
    }


async def make_scan_credits(
    request_id,
    recipient_view_key,
    creation_block_height,
    cur_height,
    mint_transfer_transactions,
    scan_pp_output,
):
    if scan_pp_output:
        return scan_pp_output

    decrypted = await asyncio.gather(
        *[
            decrypt_transaction_record(transaction, recipient_view_key)
            for transaction in mint_transfer_transactions
        ]
    )
    decrypted = [dec for dec in decrypted if dec["record"]]
    if len(decrypted) == 0:
        raise Exception("No records found.")

    received_record = decrypted[0]["record"]
    transaction_id = decrypted[0]["transaction_id"]

    pp_data = record_to_pp_data(received_record)

    scan_pp_output = {
        "pp_data": pp_data,
        "received_record": received_record,
        "transaction_id": transaction_id,
    }
    await asyncio.gather(
        dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_pp_output": scan_pp_output},
        ),
        dynamodb_update(
            env.KNOWN_TRANSACTION_IDS_TABLE,
            {"transaction_id": transaction_id},
            {"used_already": True},
        ),
    )
    return scan_pp_output


async def make_scan_former(
    request_id,
    scan_former_output,
):
    if scan_former_output:
        return scan_former_output
    try:
        (
            payment_record,
            fee_record,
            collection_record,
        ) = await get_treasury_records()

        scan_former_output = {
            "payment_record": payment_record,
            "fee_record": fee_record,
            "collection_record": collection_record,
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


async def make_transfer_credits_fees(
    request_id,
    recipient_address,
    amount_record,
    fee_record,
    transfer_credits_fees_output,
):
    if transfer_credits_fees_output:
        return transfer_credits_fees_output
    try:
        tx_id = await transfer_credits(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            recipient_address,
            env.TRANSFER_LEOS_FEE,
            amount_record,
            fee_record,
        )
        transfer_credits_fees_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_credits_fees_output": transfer_credits_fees_output},
        )
        return transfer_credits_fees_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_credits_fees_error": format_error(e)},
        )
        raise e


async def make_scan_transfer_credits_fees(
    request_id,
    transaction_id,
    recipient_view_key,
    scan_transfer_credits_fees_output,
):
    if scan_transfer_credits_fees_output:
        return scan_transfer_credits_fees_output
    try:
        all_records_mint_account = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )
        all_records_recipient = await get_transaction_outputs(
            transaction_id,
            recipient_view_key,
        )

        scan_transfer_credits_fees_output = {
            "fee_output_record": all_records_mint_account[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
            "transfer_output_record": all_records_recipient[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/transfer_private"
            ][0],
            "payment_output_record": all_records_mint_account[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/transfer_private"
            ][1],
        }
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {
                "scan_transfer_credits_fees_output": scan_transfer_credits_fees_output
            },
        )
        return scan_transfer_credits_fees_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_credits_fees_error": format_error(e)},
        )
        raise e


async def make_transfer_pp(
    request_id,
    recipient_private_key,
    leo_record,
    fee_record,
    transfer_pp_output,
):
    if transfer_pp_output:
        return transfer_pp_output
    try:
        tx_id = await transfer_pp(
            recipient_private_key,
            leo_record,
            env.MINT_ACCOUNT_ADDRESS,
            fee_record,
        )
        transfer_pp_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_pp_output": transfer_pp_output},
        )
        return transfer_pp_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_pp_error": format_error(e)},
        )
        raise e


async def make_scan_transfer_pp(
    request_id,
    token_number,
    transaction_id,
    scan_transfer_pp_output,
):
    if scan_transfer_pp_output:
        return scan_transfer_pp_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )

        scan_transfer_pp_output = {
            "pp_record": all_records[
                f"{env.PRIVACY_PRIDE_PROGRAM_ID}/transfer_private"
            ][0],
        }
        await asyncio.gather(
            dynamodb_update(
                env.ALEO_MINT_REQUESTS_TABLE,
                {"request_id": request_id},
                {"scan_transfer_pp_output": scan_transfer_pp_output},
            ),
            dynamodb_update(
                env.PRIVACY_PRIDE_STORED_TOKEN_RECORDS_TABLE,
                {"token_number": token_number},
                {"token_record": scan_transfer_pp_output["pp_record"]},
            ),
        )
        return scan_transfer_pp_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_pp_error": format_error(e)},
        )
        raise e


async def make_mint(
    request_id,
    collection_record,
    pp_data,
    user_address,
    fee_record,
    mint_output,
):
    if mint_output:
        return mint_output
    try:
        tx_id = await mint_private(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            collection_record,
            pp_data["token_number"],
            user_address,
            encode_string64(str(pp_data["token_id"]) + ".json"),
            fee_record,
        )
        mint_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"mint_output": mint_output},
        )
        return mint_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"mint_error": format_error(e)},
        )
        raise e


async def make_scan_mint(
    request_id,
    transaction_id,
    to_store_record,
    scan_mint_output,
):
    if scan_mint_output:
        return scan_mint_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )

        scan_mint_output = {
            "collection_output_record": all_records[
                f"{env.ALEO_STORE_PROGRAM_ID}/mint_private"
            ][1],
            "payment_output_record": to_store_record,
            "fee_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
        }

        await asyncio.gather(
            dynamodb_update(
                env.ALEO_MINT_REQUESTS_TABLE,
                {"request_id": request_id},
                {"scan_mint_output": scan_mint_output},
            ),
            push_treasury_records(
                [
                    (scan_mint_output["payment_output_record"], False),
                    (scan_mint_output["fee_output_record"], False),
                    (scan_mint_output["collection_output_record"], True),
                ]
            ),
        )
        return scan_mint_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_mint_error": format_error(e)},
        )
        raise e
