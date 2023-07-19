from datetime import datetime
import asyncio

from termcolor import colored
import traceback as tb

utc_now_ms = lambda: round(datetime.utcnow().timestamp() * 1000)


async def ascync_run(cmd):
    full_cmd = " ".join([str(el) for el in cmd])

    proc = await asyncio.create_subprocess_shell(
        full_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()
    stdout = stdout.decode()
    stderr = stderr.decode()

    print("")
    print("Executing:")
    print(colored(full_cmd, "blue"))
    if stdout:
        print("STDOUT:")
        print(colored(stdout, "green"))
    if stderr:
        print("STDERR:")
        print(colored(stderr, "red"))
    print("")

    return stdout


record_to_amount = lambda record: int(
    record.replace(" ", "").replace("\n", "").split(",")[1][13:-11]
)

leos_record_to_amount = lambda record: int(
    record.replace(" ", "").replace("\n", "").split(",")[1][7:-11]
)


format_error = lambda e: (
    f"{e}\n" + ("".join(tb.format_exception(None, e, e.__traceback__)))
)


def record_to_pp_data(record):
    bdata1, bdata2 = ":".join(record.split(":")[2:5])[:-8].split(",")
    part1_int, part2_int = int(bdata1[7:-12]), int(bdata2[6:-13])
    edition = int(record.split(":")[5][:-21])

    part1_str = bytearray.fromhex(hex(part1_int)[2:]).decode()[::-1]
    part2_str = bytearray.fromhex(hex(part2_int)[2:]).decode()[::-1]
    uri = part1_str + part2_str
    token_id = int(uri.split("/")[-1].split(".")[0])

    token_number = token_id + 2**64 * edition
    token_number_str = str(token_number) + "u128"

    return {
        "token_id": token_id,
        "edition": edition,
        "token_number": token_number_str,
    }
