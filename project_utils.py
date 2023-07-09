from datetime import datetime
import asyncio


utc_now_ms = lambda: round(datetime.utcnow().timestamp() * 1000)


async def ascync_run(cmd):
    proc = await asyncio.create_subprocess_shell(
        ' '.join(cmd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()
    stdout = stdout.decode()
    stderr = stderr.decode()

    if stderr:
        raise Exception(stderr)
    return stdout
