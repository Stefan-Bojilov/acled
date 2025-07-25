from datetime import date, datetime

import aiohttp

from dagster_acled.acled_client import AcledConfig


async def fetch_page(session: aiohttp.ClientSession, url: str, params: dict) -> list[dict]:
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        payload = await resp.json()
        return payload.get("data", [])


