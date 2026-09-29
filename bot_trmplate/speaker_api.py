"""Speaker API integration with a shared HTTP session for high throughput."""
import asyncio
import aiohttp
SPEAKER_API_URL = "https://l8pStudio.ir/apis-loop/api-speaker.php"
SPEAKER_API_TIMEOUT = 7
SPEAKER_MAX_CONCURRENCY = 24
_session = None
_session_lock = asyncio.Lock()
_semaphore = asyncio.Semaphore(SPEAKER_MAX_CONCURRENCY)
async def _get_session():
    global _session
    if _session is None or _session.closed:
        async with _session_lock:
            if _session is None or _session.closed:
                connector = aiohttp.TCPConnector(limit=64, limit_per_host=24, ttl_dns_cache=300, enable_cleanup_closed=True)
                timeout = aiohttp.ClientTimeout(total=SPEAKER_API_TIMEOUT, connect=3, sock_connect=3, sock_read=5)
                _session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _session
async def api_speaker(text: str, mode: str = None, restric: bool = False):
    if not text: return None
    payload = {"text": text}
    if mode: payload["mode"] = mode
    if restric: payload["restric"] = True
    try:
        async with _semaphore:
            session = await _get_session()
            async with session.post(SPEAKER_API_URL, json=payload) as response:
                if response.status != 200: return None
                data = await response.json(content_type=None)
                if not isinstance(data, dict) or not data.get("status"): return None
                answer = data.get("data")
                return str(answer).strip() if answer is not None and str(answer).strip() else None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
        return None
    except Exception:
        return None
async def close_speaker_session():
    global _session
    if _session is not None and not _session.closed: await _session.close()
    _session = None
