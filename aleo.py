import subprocess
import env

from http_utils import get_request
import json

from aws_utils import (
    dynamodb_scan,
    dynamodb_delete,
    dynamodb_update,
    dynamodb_get,
)
import asyncio
from project_utils import utc_now_ms, ascync_run, record_to_amount
import json
import re
from fake_useragent import UserAgent


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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
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
    record = "".join(stdout.split("\n")[1:-1]).replace(" ", "")
    if not record.startswith("{"):
        return ""
    return "".join(stdout.split("\n")[1:-1]).replace(" ", "")


async def get_treasury_records():
    treasury_records = await dynamodb_scan(env.ALEO_TREASURY_RECORDS_TABLE)
    records = []
    collection_record = None

    for record_scanned in treasury_records:
        if record_scanned.get("collection_record"):
            collection_record = record_scanned["record_id"]
            continue
        records.append(
            {
                "record": record_scanned["record_id"],
                "amount": record_to_amount(record_scanned["record_id"]),
            }
        )
    records.sort(key=lambda x: x["amount"])
    if len(records) < 2:
        raise Exception("Not enough records in treasury.")
    if not collection_record:
        raise Exception("No collection record in treasury.")

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
        dynamodb_delete(
            env.ALEO_TREASURY_RECORDS_TABLE,
            {"record_id": collection_record},
        ),
    )

    return (
        smallest_record["record"],
        biggest_record["record"],
        collection_record,
    )


async def get_stored_token_record(token_number):
    treasury_record = await dynamodb_get(
        env.PRIVACY_PRIDE_STORED_TOKEN_RECORDS_TABLE,
        {"token_number": token_number},
    )
    if not treasury_record:
        raise Exception("No corresponding token record in treasury.")

    await dynamodb_delete(
        env.PRIVACY_PRIDE_STORED_TOKEN_RECORDS_TABLE,
        {"token_number": token_number},
    )
    return treasury_record["token_record"]


async def push_treasury_records(records):
    await asyncio.gather(
        *[
            dynamodb_update(
                env.ALEO_TREASURY_RECORDS_TABLE,
                {"record_id": record},
                {"used_already": False, "collection_record": collection},
            )
            for (record, collection) in records
        ]
    )


async def transfer_pp(
    private_key,
    amount_record,
    receiver_address,
    fee_record,
    priority_fee=0,
):
    amount_record = f'"{amount_record}"'
    fee_record = f'"{fee_record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.PRIVACY_PRIDE_PROGRAM_ID,
            "transfer_private",
            amount_record,
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
    return tx_id


async def transfer_token_private(
    private_key,
    amount_record,
    receiver_address,
    fee_record,
    priority_fee=0,
):
    amount_record = f'"{amount_record}"'
    fee_record = f'"{fee_record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "transfer_token_private",
            amount_record,
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
    return tx_id


async def burn_private(
    private_key,
    collection_record,
    amount_record,
    fee_record,
    priority_fee=0,
):
    amount_record = f'"{amount_record}"'
    collection_record = f'"{collection_record}"'
    fee_record = f'"{fee_record}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "burn_private",
            collection_record,
            amount_record,
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
    return tx_id


async def get_transaction_id(transition_id):
    return await get_request(
        f"{env.ALEO_API}/testnet3/find/transactionID/{transition_id}",
    )


async def get_transaction(transaction_id):
    return await get_request(
        f"{env.ALEO_API}/testnet3/transaction/{transaction_id}",
    )


async def get_recent_transitions(program_id):
    ua = UserAgent()
    header = {"User-Agent": str(ua.chrome)}

    res = await get_request(
        f"{env.HAMP_API}/program?id={program_id}",
        headers=header,
        json_output=False,
    )

    transition_ids = re.findall(
        r"\<a\ href\=\"\/transition\?id\=(as1[0-9a-z-A-Z]+)\"\>", res
    )
    return transition_ids


async def mint_private(
    private_key,
    collection_record,
    token_number,
    user_address,
    metadata_uri,
    fee_record,
    priority_fee=0,
):
    collection_record = f'"{collection_record}"'
    fee_record = f'"{fee_record}"'
    metadata_uri = f'"{metadata_uri}"'
    stdout = await ascync_run(
        [
            f"{env.CARGO_BIN_DIR_PATH}snarkos",
            "developer",
            "execute",
            env.ALEO_STORE_PROGRAM_ID,
            "mint_private",
            collection_record,
            token_number,
            user_address,
            metadata_uri,
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
    tx_ids = re.findall(r"at1[a-z0-9]{58}", stdout)
    if not tx_ids:
        raise Exception(stdout)
    tx_id = tx_ids[0]
    return tx_id


def encode_string(string, part_amount, bits_per_part):
    part_len = bits_per_part // 8
    parts_str = []
    if len(string) > part_amount * bits_per_part // 8:
        raise Exception("String too long to be encoded.")
    for i in range(part_amount):
        parts_str.append(string[i * part_len : (i + 1) * part_len])
    parts_hex = [part_str.encode("utf-8").hex() for part_str in parts_str]
    parts_int = [
        int(part_hex, 16) if part_hex else 0 for part_hex in parts_hex
    ]
    out_str = "{" + "".join(
        [
            f"part{i}:{parts_int[i]}u{bits_per_part},"
            for i in range(part_amount)
        ]
    )
    out_str = out_str[:-1] + "}"
    return out_str


def encode_string64(string):
    return encode_string(string, 4, 128)
