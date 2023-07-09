import aiohttp
import json


async def get_request(url, json_output=True):
    async with aiohttp.ClientSession() as aiohttp_session:
        async with aiohttp_session.get(url) as response:
            rep = await response.text()
            if json_output:
                rep = json.loads(rep)
            return rep