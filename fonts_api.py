"""Async integration for the supplied Splus font API."""
import asyncio
import aiohttp

FONTS_API_URL = "https://l8pStudio.ir/apis-loop/api-fonts.php"
FONTS_API_TIMEOUT = 10

async def api_fonts(text: str, mode: str = None, restric: bool = False):
    if not text:
        return None
    payload = {"text": text}
    if mode:
        payload["mode"] = mode
    if restric:
        payload["restric"] = True
    try:
        timeout = aiohttp.ClientTimeout(total=FONTS_API_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(FONTS_API_URL, json=payload) as response:
                if response.status != 200:
                    return None
                data = await response.json(content_type=None)
                if not isinstance(data, dict) or not data.get("status"):
                    return None
                return data.get("data")
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
        return None
    except Exception:
        return None
