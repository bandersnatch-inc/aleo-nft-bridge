import subprocess
import env

from http_utils import get_request
import json


from project_utils import utc_now_ms, ascync_run


def create_account():
    aleo_account_new_stdout = subprocess.run(
        ['aleo', 'account', 'new'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    ).stdout

    stdout_lines = aleo_account_new_stdout.split('\n')
    private_key = stdout_lines[4].split(' ')[-1]
    view_key = stdout_lines[5].split(' ')[-1]
    address = stdout_lines[6].split(' ')[-1]

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

    await ascync_run([
        'snarkos',
        'developer', 
        'scan', 
        "--endpoint", 
        endpoint, 
        "--view-key",
        view_key,
        *period_args
    ])
    stdout_lines = stdout.split('\n')
    try:
        records_string = "".join(stdout_lines[5:-2])
        record_strings = json.loads(records_string)
        return record_strings
    except:
        return []



async def transfer_credits(
    private_key,
    receiver_address,
    amount,
    record,
    fee_record
):
    await ascync_run([
        'snarkos',
        'developer', 
        'transfer-private', 
        "--query", 
        env.ALEO_API, 
        "--input-record",
        record,
        "--recipient",
        receiver_address,
        "--amount",
        str(amount),
        "--private-key",
        private_key,
        "--fee",
        env.TRANSFER_FEE,
        "--fee-record",
        fee_record
    ])


async def transfer_credits(
    private_key,
    receiver_address,
    amount,
    record,
    fee_record
):
    await ascync_run([
        'aleo',
        'execute',
        'transfer-private',
        "--query",
        env.ALEO_API,
        "--input-record",
        record,
        "--recipient",
        receiver_address,
        "--amount",
        str(amount),
        "--private-key",
        private_key,
        "--fee",
        "",
        "--fee-record",
        fee_record
    ])

