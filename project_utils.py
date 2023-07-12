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

    print()
    print("Executing:")
    print(colored(full_cmd, "blue"))

    print("STDOUT:")
    print(colored(stdout, "green"))
    print("STDERR:")
    print(colored(stderr, "red"))
    print()

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
