from pathlib import Path

import aiohttp
import yaml


async def fetch_page(session, url, params, headers=None):
    """
    Fetch_page function to support OAuth headers.
    """
    try:
        async with session.get(url, params=params, headers=headers or {}) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('data', []) 
            else:
                if response.status == 403:
                    raise Exception(f"Authentication failed: {response.status}")
                else:
                    raise Exception(f"API request failed: {response.status}")
    except Exception as e:
        print(f"Error fetching page: {e}")
        return []


def load_config(config_path: str):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)