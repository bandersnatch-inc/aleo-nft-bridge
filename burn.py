import asyncio
from aws_utils import dynamodb_update, dynamodb_scan
from aleo import (
    create_account,
    transfer_credits,
    get_transaction_outputs,
    get_treasury_records,
    get_stored_token_record,
    push_treasury_records,
    decrypt_record,
    encode_string64,
    transfer_token_private,
    burn_private,
    transfer_pp,
)
import env
from project_utils import record_to_amount, format_error, record_to_as_data


async def get_burn_requests(cur_height):
    height_limit = cur_height - env.REQUESTS_SCAN_HEIGHT_LIMIT
    requests = await dynamodb_scan(
        env.ALEO_BURN_REQUESTS_TABLE,
        filter_expression=f"(creation_block_height >= :height_limit or attribute_exists(scan_pp_output)) and attribute_not_exists(scan_transfer_pp_output) and attribute_not_exists(not_possible)",
        ExpressionAttributeValues={":height_limit": {"N": str(height_limit)}},
    )
    return requests


async def burn_scan_records(request, cur_height, burn_transfer_transactions):
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
            burn_transfer_transactions,
            scan_pp_output,
        )
    except Exception as e:
        pass


async def handle_active_burn_request(request, cur_height):
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
    transfer_as_output = request.get("transfer_as_output", None)
    scan_transfer_as_output = request.get("scan_transfer_as_output", None)
    burn_output = request.get("burn_output", None)
    scan_burn_output = request.get("scan_burn_output", None)
    transfer_pp_output = request.get("transfer_pp_output", None)
    scan_transfer_pp_output = request.get("scan_transfer_pp_output", None)

    try:
        scan_former_output = await make_scan_former(
            request_id,
            scan_pp_output["as_data"]["token_number"],
            scan_former_output,
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
        has_worked = not bool(transfer_as_output)
        transfer_as_output = await make_transfer_as(
            request_id,
            recipient_private_key,
            scan_pp_output["received_record"],
            scan_transfer_credits_fees_output["transfer_output_record"],
            transfer_as_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_as_output = await make_scan_transfer_as(
            request_id,
            scan_pp_output["as_data"]["token_number"],
            transfer_as_output["tx_id"],
            scan_transfer_as_output,
        )
        has_worked = not bool(burn_output)
        burn_output = await make_burn(
            request_id,
            scan_former_output["collection_record"],
            scan_transfer_as_output["as_record"],
            scan_transfer_credits_fees_output["payment_output_record"],
            burn_output,
        )
        if has_worked:
            await asyncio.sleep(15)

        scan_burn_output = await make_scan_burn(
            request_id,
            burn_output["tx_id"],
            scan_burn_output,
        )

        has_worked = not bool(transfer_pp_output)
        transfer_pp_output = await make_transfer_pp(
            request_id,
            user_address,
            scan_former_output["stored_token_record"],
            scan_transfer_credits_fees_output["fee_output_record"],
            transfer_pp_output,
        )
        if has_worked:
            await asyncio.sleep(15)

        scan_transfer_pp_output = await make_scan_transfer_pp(
            request_id,
            scan_burn_output["fee_output_record"],
            scan_burn_output["collection_output_record"],
            transfer_pp_output["tx_id"],
            scan_transfer_pp_output,
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
    burn_transfer_transactions,
    scan_pp_output,
):
    if scan_pp_output:
        return scan_pp_output

    decrypted = await asyncio.gather(
        *[
            decrypt_transaction_record(transaction, recipient_view_key)
            for transaction in burn_transfer_transactions
        ]
    )
    decrypted = [dec for dec in decrypted if dec["record"]]
    if len(decrypted) == 0:
        raise Exception("No records found.")

    received_record = decrypted[0]["record"]
    transaction_id = decrypted[0]["transaction_id"]

    try:
        as_data = record_to_as_data(received_record)
    except:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"not_possible": True},
        )
        raise Exception("Not possible.")

    scan_pp_output = {
        "as_data": as_data,
        "received_record": received_record,
        "transaction_id": transaction_id,
    }
    await asyncio.gather(
        dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
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
    token_number,
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

        try:
            stored_token_record = await get_stored_token_record(token_number)
        except Exception as e:
            await asyncio.gather(
                push_treasury_records(
                    [
                        (payment_record, False),
                        (fee_record, False),
                        (collection_record, True),
                    ]
                ),
                dynamodb_update(
                    env.ALEO_BURN_REQUESTS_TABLE,
                    {"request_id": request_id},
                    {"not_possible": True},
                ),
            )
            raise e

        scan_former_output = {
            "payment_record": payment_record,
            "fee_record": fee_record,
            "collection_record": collection_record,
            "stored_token_record": stored_token_record,
        }
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_former_output": scan_former_output},
        )
        return scan_former_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
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
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_credits_fees_output": transfer_credits_fees_output},
        )
        return transfer_credits_fees_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
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
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {
                "scan_transfer_credits_fees_output": scan_transfer_credits_fees_output
            },
        )
        return scan_transfer_credits_fees_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_credits_fees_error": format_error(e)},
        )
        raise e


async def make_transfer_as(
    request_id,
    recipient_private_key,
    leo_record,
    fee_record,
    transfer_as_output,
):
    if transfer_as_output:
        return transfer_as_output
    try:
        tx_id = await transfer_token_private(
            recipient_private_key,
            leo_record,
            env.MINT_ACCOUNT_ADDRESS,
            fee_record,
        )
        transfer_as_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_as_output": transfer_as_output},
        )
        return transfer_as_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_as_error": format_error(e)},
        )
        raise e


async def make_scan_transfer_as(
    request_id,
    token_number,
    transaction_id,
    scan_transfer_as_output,
):
    if scan_transfer_as_output:
        return scan_transfer_as_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )

        scan_transfer_as_output = {
            "as_record": all_records[
                f"{env.ALEO_STORE_PROGRAM_ID}/transfer_token_private"
            ][0],
        }
        await asyncio.gather(
            dynamodb_update(
                env.ALEO_BURN_REQUESTS_TABLE,
                {"request_id": request_id},
                {"scan_transfer_as_output": scan_transfer_as_output},
            ),
            dynamodb_update(
                env.PRIVACY_PRIDE_STORED_TOKEN_RECORDS_TABLE,
                {"token_number": token_number},
                {"token_record": scan_transfer_as_output["as_record"]},
            ),
        )
        return scan_transfer_as_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_as_error": format_error(e)},
        )
        raise e


async def make_burn(
    request_id,
    collection_record,
    token_record,
    fee_record,
    burn_output,
):
    if burn_output:
        return burn_output
    try:
        tx_id = await burn_private(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            collection_record,
            token_record,
            fee_record,
        )
        burn_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"burn_output": burn_output},
        )
        return burn_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"burn_error": format_error(e)},
        )
        raise e


async def make_scan_burn(
    request_id,
    transaction_id,
    scan_burn_output,
):
    if scan_burn_output:
        return scan_burn_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )

        scan_burn_output = {
            "collection_output_record": all_records[
                f"{env.ALEO_STORE_PROGRAM_ID}/burn_private"
            ][0],
            "fee_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
        }

        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_burn_output": scan_burn_output},
        )
        return scan_burn_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_mint_error": format_error(e)},
        )
        raise e


async def make_transfer_pp(
    request_id,
    address,
    pp_record,
    fee_record,
    transfer_pp_output,
):
    if transfer_pp_output:
        return transfer_pp_output
    try:
        tx_id = await transfer_pp(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            pp_record,
            address,
            fee_record,
        )
        transfer_pp_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_pp_output": transfer_pp_output},
        )
        return transfer_pp_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_pp_error": format_error(e)},
        )
        raise e


async def make_scan_transfer_pp(
    request_id,
    fee_output_record,
    collection_output_record,
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
            "payment_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
        }
        await asyncio.gather(
            dynamodb_update(
                env.ALEO_BURN_REQUESTS_TABLE,
                {"request_id": request_id},
                {"scan_transfer_pp_output": scan_transfer_pp_output},
            ),
            push_treasury_records(
                [
                    (scan_transfer_pp_output["payment_output_record"], False),
                    (fee_output_record, False),
                    (collection_output_record, True),
                ]
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
