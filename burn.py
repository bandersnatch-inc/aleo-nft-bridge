import asyncio
from aws_utils import dynamodb_update, dynamodb_scan
from aleo import (
    scan_records,
    transfer_leos,
    transfer_credits,
    get_transaction_outputs,
    get_treasury_records,
    push_treasury_records,
    join_credits,
    burn_leos,
)
import env
from project_utils import format_error, leos_record_to_amount


BURN_TOTAL_FEE = (
    env.TRANSFER_LEO_FEE + 2 * env.TRANSFER_CREDITS_FEE + env.BURN_LEOS_FEE
)


async def get_burn_requests(cur_height):
    height_limit = cur_height - env.REQUESTS_SCAN_HEIGHT_LIMIT
    requests = await dynamodb_scan(
        env.ALEO_BURN_REQUESTS_TABLE,
        filter_expression=f"(creation_block_height >= :height_limit or attribute_exists(scan_leos_output)) and attribute_not_exists(scan_burn_leos_output) and attribute_not_exists(not_possible)",
        ExpressionAttributeValues={":height_limit": {"N": str(height_limit)}},
    )
    return requests


async def burn_scan_records(request, cur_height):
    try:
        request_id = request["request_id"]
        creation_block_height = request["creation_block_height"]
        recipient_view_key = request["recipient_view_key"]
        scan_leos_output = request.get("scan_leos_output", None)
        request["scan_leos_output"] = await make_scan_leos(
            request_id,
            recipient_view_key,
            creation_block_height,
            cur_height,
            scan_leos_output,
        )
    except Exception as e:
        pass


async def handle_active_burn_request(request, cur_height):
    request_id = request["request_id"]
    recipient_view_key = request["recipient_view_key"]
    recipient_private_key = request["recipient_private_key"]
    user_address = request["user_address"]

    scan_leos_output = request.get("scan_leos_output", None)
    scan_former_output = request.get("scan_former_output", None)
    transfer_credits_fees_output = request.get(
        "transfer_credits_fees_output", None
    )
    scan_transfer_credits_fees_output = request.get(
        "scan_transfer_credits_fees_output", None
    )
    transfer_leos_output = request.get("transfer_leos_output", None)
    scan_transfer_leos_output = request.get("scan_transfer_leos_output", None)
    transfer_output = request.get("transfer_output", None)
    scan_transfer_output = request.get("scan_transfer_output", None)
    burn_leos_output = request.get("burn_leos_output", None)
    scan_burn_leos_output = request.get("scan_burn_leos_output", None)

    try:
        scan_former_output = await make_scan_former(
            request_id, scan_former_output
        )
        has_worked = not bool(transfer_credits_fees_output)
        transfer_credits_fees_output = await make_transfer_credits_fees(
            request_id,
            recipient_private_key,
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
        has_worked = not bool(transfer_leos_output)
        transfer_leos_output = await make_transfer_leos(
            request_id,
            recipient_private_key,
            scan_leos_output["received_output"],
            scan_transfer_credits_fees_output["transfer_output_record"],
            scan_leos_output["received_amount"],
            transfer_leos_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_leos_output = await make_scan_transfer_leos(
            request_id,
            transfer_leos_output["tx_id"],
            scan_transfer_leos_output,
        )
        has_worked = not bool(transfer_output)
        transfer_output = await make_transfer(
            request_id,
            user_address,
            scan_leos_output["received_amount"],
            scan_transfer_credits_fees_output["fee_output_record"],
            scan_transfer_credits_fees_output["payment_output_record"],
            transfer_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_transfer_output = await make_scan_transfer(
            request_id, transfer_output["tx_id"], scan_transfer_output
        )
        has_worked = not bool(burn_leos_output)
        burn_leos_output = await make_burn_leos(
            request_id,
            scan_transfer_leos_output["leos_record"],
            scan_leos_output["received_amount"],
            scan_transfer_output["fee_output_record"],
            burn_leos_output,
        )
        if has_worked:
            await asyncio.sleep(15)
        scan_burn_leos_output = await make_scan_burn_leos(
            request_id,
            burn_leos_output["tx_id"],
            scan_transfer_output["payment_output_record"],
            scan_burn_leos_output,
        )
    except Exception as e:
        print(format_error(e))
        return


async def make_scan_leos(
    request_id,
    recipient_view_key,
    creation_block_height,
    cur_height,
    scan_leos_output,
):
    if scan_leos_output:
        return scan_leos_output
    records = await scan_records(
        recipient_view_key,
        start=creation_block_height - 1,
        end=cur_height,
    )
    if not records:
        raise Exception("No records found.")
    received_record = records[0]
    received_amount = leos_record_to_amount(received_record)

    if received_amount <= BURN_TOTAL_FEE:
        await dynamodb_update(
            env.ALEO_MINT_REQUESTS_TABLE,
            {"request_id": request_id},
            {"not_possible": True},
        )
        raise Exception("Not enough credits.")

    scan_leos_output = {
        "received_amount": received_amount,
        "received_record": received_record,
    }
    await dynamodb_update(
        env.ALEO_BURN_REQUESTS_TABLE,
        {"request_id": request_id},
        {"scan_leos_output": scan_leos_output},
    )
    return scan_leos_output


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
    recipient_private_key,
    amount_record,
    fee_record,
    transfer_credits_fees_output,
):
    if transfer_credits_fees_output:
        return transfer_credits_fees_output
    try:
        tx_id = await transfer_credits(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            recipient_private_key,
            env.TRANSFER_LEO_FEE,
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


async def make_transfer_leos(
    request_id,
    recipient_private_key,
    leo_record,
    fee_record,
    received_amount,
    transfer_leos_output,
):
    if transfer_leos_output:
        return transfer_leos_output
    try:
        tx_id = await transfer_leos(
            recipient_private_key,
            leo_record,
            received_amount,
            env.MINT_ACCOUNT_ADDRESS,
            fee_record,
        )
        transfer_leos_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_leos_output": transfer_leos_output},
        )
        return transfer_leos_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_leos_error": format_error(e)},
        )
        raise e


async def make_scan_transfer_leos(
    request_id,
    transaction_id,
    scan_transfer_leos_output,
):
    if scan_transfer_leos_output:
        return scan_transfer_leos_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )

        scan_transfer_leos_output = {
            "leos_record": all_records[
                f"{env.ALEO_STORE_PROGRAM_ID}/transfer_leos"
            ][0],
        }
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_leos_output": scan_transfer_leos_output},
        )
        return scan_transfer_leos_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_leos_error": format_error(e)},
        )
        raise e


async def make_transfer(
    request_id,
    user_address,
    received_amount,
    amount_record,
    fee_record,
    transfer_output,
):
    if transfer_output:
        return transfer_output
    try:
        tx_id = await transfer_credits(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            user_address,
            env.TRANSFER_LEO_FEE,
            received_amount - BURN_TOTAL_FEE,
            amount_record,
            fee_record,
        )
        transfer_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_output": transfer_output},
        )
        return transfer_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"transfer_error": format_error(e)},
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
        scan_transfer_output = {
            "payment_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/transfer_private"
            ][1],
            "fee_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
        }
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_output": scan_transfer_output},
        )
        return scan_transfer_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_transfer_error": format_error(e)},
        )
        raise e


async def make_burn_leos(
    request_id,
    leo_record,
    received_amount,
    fee_record,
    burn_leos_output,
):
    if burn_leos_output:
        return burn_leos_output
    try:
        tx_id = await burn_leos(
            env.MINT_ACCOUNT_PRIVATE_KEY,
            leo_record,
            received_amount,
            env.MINT_ACCOUNT_ADDRESS,
            fee_record,
        )
        burn_leos_output = {"tx_id": tx_id}
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"burn_leos_output": burn_leos_output},
        )
        return burn_leos_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"burn_leos_error": format_error(e)},
        )
        raise e


async def make_scan_burn_leos(
    request_id, transaction_id, payment_output_record, scan_burn_leos_output
):
    if scan_burn_leos_output:
        return scan_burn_leos_output
    try:
        all_records = await get_transaction_outputs(
            transaction_id,
            env.MINT_ACCOUNT_VIEW_KEY,
        )
        scan_burn_leos_output = {
            "fee_output_record": all_records[
                f"{env.ALEO_CREDITS_PROGRAM_ID}/fee"
            ][0],
        }
        await push_treasury_records(
            [
                payment_output_record,
                scan_burn_leos_output["fee_output_record"],
            ]
        )
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_burn_leos_output": scan_burn_leos_output},
        )
        return scan_burn_leos_output
    except Exception as e:
        await dynamodb_update(
            env.ALEO_BURN_REQUESTS_TABLE,
            {"request_id": request_id},
            {"scan_burn_leos_error": format_error(e)},
        )
        raise e
