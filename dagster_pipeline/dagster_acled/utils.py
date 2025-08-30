from pathlib import Path

import aiohttp
import yaml


async def fetch_page(session: aiohttp.ClientSession, url: str, params: dict) -> list[dict]:
    async with session.get(url, params=params) as resp:
        resp.raise_for_status()
        payload = await resp.json()
        return payload.get("data", [])


def load_config(config_path: str):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)