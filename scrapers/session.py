import time
import random
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def make_session(retries: int = 3, backoff: float = 1.5) -> requests.Session:
    """Create a requests.Session with retry + backoff configured."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def jitter_sleep(min_s: float = 1.0, max_s: float = 3.0):
    """Sleep with random jitter to avoid rate-limiting."""
    t = random.uniform(min_s, max_s)
    time.sleep(t)


def safe_get(session: requests.Session, url: str, timeout: int = 15) -> requests.Response | None:
    """GET a URL, return response or None on error."""
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        logger.warning(f"GET failed [{url}]: {e}")
        return None
