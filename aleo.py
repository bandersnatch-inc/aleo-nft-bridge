import subprocess
import env

from http_utils import get_request
import json

from aws_utils import dynamodb_scan, dynamodb_delete, dynamodb_update
import asyncio
from project_utils import utc_now_ms, ascync_run, record_to_amount


async def create_account():
    aleo_account_new_stdout = await ascync_run(
        [f"{env.CARGO_BIN_DIR_PATH}snarkos", "account", "new"]
    )

    stdout_lines = aleo_account_new_stdout.split("\n")
    private_key = stdout_lines[1].split(" ")[-1]
    view_key = stdout_lines[2].split(" ")[-1]
    address = stdout_lines[3].split(" ")[-1]

    return private_key, view_key, address


async def get_height():
    height_endpoint = f"{env.ALEO_API}/testnet3/latest/height"
    return int(await get_request(height_endpoint))


async def scan_records(
    view_key, endpoint=env.ALEO_API, start=0, end=None, last=None
):
    if last is not None:
        period_args = ["--last", str(last)]
    else:
        if end is None:
            end = await get_height()
        period_args = ["--start", str(start)]
        period_args += ["--end", str(end)]

    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "scan",
            "--endpoint",
            endpoint,
            "--view-key",
            view_key,
            *period_args,
        ]
    )
    stdout_lines = stdout.split("\n")
    if stdout_lines[3] == "No records found":
        return []
    records_string = "".join(stdout_lines[5:-2])
    record_strings = json.loads(records_string)
    return [record_string.replace(" ", "") for record_string in record_strings]


async def transfer_credits(
    private_key, receiver_address, amount, record, fee_record, priority_fee=0
):
    fee_record = f'"{fee_record}"'
    record = f'"{record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_CREDITS_PROGRAM_ID,
            "transfer_private",
            record,
            receiver_address,
            f"{amount}u64",
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
            "--fee",
            priority_fee,
            "--record",
            fee_record,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id


async def mint_leos(
    private_key, receiver_address, amount, fee_record, priority_fee=0
):
    amount = f"{amount}u64"
    fee_record = f'"{fee_record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "mint_leos",
            amount,
            receiver_address,
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
            "--fee",
            priority_fee,
            "--record",
            fee_record,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id


async def split_credit(
    private_key,
    record,
    amount,
):
    encoded_amount = f"{amount}u64"
    record = f'"{record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_CREDITS_PROGRAM_ID,
            "split",
            record,
            encoded_amount,
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id


async def join_credits(
    private_key,
    record1,
    record2,
    fee_record,
    priority_fee=0,
):
    record1 = f'"{record1}"'
    record2 = f'"{record2}"'
    fee_record = f'"{fee_record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_CREDITS_PROGRAM_ID,
            "join",
            record1,
            record2,
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
            "--fee",
            priority_fee,
            "--record",
            fee_record,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id


async def get_transaction(tx_id):
    transaction_endpoint = f"{env.ALEO_API}/testnet3/transaction/{tx_id}"
    return await get_request(transaction_endpoint)


async def get_transaction_outputs(tx_id, view_key):
    transaction = await get_transaction(tx_id)
    fee = transaction.get("fee")

    transitions = transaction["execution"]["transitions"] + (
        [transaction["fee"]["transition"]] if fee else []
    )
    outputs = {}
    for transition in transitions:
        transition_id = f'{transition["program"]}/{transition["function"]}'
        output_ciphers = transition["outputs"]
        outputs[transition_id] = [
            await decrypt_record(output_cipher["value"], view_key)
            for output_cipher in output_ciphers
        ]
    return outputs


async def decrypt_record(record, view_key):
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "decrypt",
            "--ciphertext",
            record,
            "--view-key",
            view_key,
        ]
    )
    return "".join(stdout.split("\n")[1:-1]).replace(" ", "")


async def get_treasury_records():
    treasury_records = await dynamodb_scan(env.ALEO_TREASURY_RECORDS_TABLE)
    records = []

    for record_scanned in treasury_records:
        records.append(
            {
                "record": record_scanned["record_id"],
                "amount": record_to_amount(record_scanned["record_id"]),
            }
        )
    records.sort(key=lambda x: x["amount"])
    if len(records) < 2:
        raise Exception("Not enough records in treasury.")

    biggest_record, smallest_record = records[-1], records[0]

    await asyncio.gather(
        dynamodb_delete(
            env.ALEO_TREASURY_RECORDS_TABLE,
            {"record_id": biggest_record["record"]},
        ),
        dynamodb_delete(
            env.ALEO_TREASURY_RECORDS_TABLE,
            {"record_id": smallest_record["record"]},
        ),
    )

    return smallest_record["record"], biggest_record["record"]


async def push_treasury_records(records):
    await asyncio.gather(
        *[
            dynamodb_update(
                env.ALEO_TREASURY_RECORDS_TABLE,
                {"record_id": record},
                {"used_already": False},
            )
            for record in records
        ]
    )


async def transfer_leos(
    private_key,
    amount_record,
    amount,
    receiver_address,
    fee_record,
    priority_fee=0,
):
    amount_record = f'"{amount_record}"'
    fee_record = f'"{fee_record}"'
    amount = f"{amount}u64"
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "transfer_leos",
            amount_record,
            amount,
            receiver_address,
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
            "--fee",
            priority_fee,
            "--record",
            fee_record,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id


async def burn_leos(
    private_key,
    amount_record,
    amount,
    fee_record,
    priority_fee=0,
):
    amount_record = f'"{amount_record}"'
    fee_record = f'"{fee_record}"'
    amount = f"{amount}u64"
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "burn_leos",
            amount_record,
            amount,
            "--query",
            env.ALEO_API,
            "--private-key",
            private_key,
            "--broadcast",
            env.ALEO_BROADCAST_ENDPOINT,
            "--fee",
            priority_fee,
            "--record",
            fee_record,
        ]
    )
    tx_id = stdout.split("\n")[5]
    if not len(tx_id) == 61 or not tx_id.startswith("at1"):
        raise Exception(stdout)
    return tx_id
