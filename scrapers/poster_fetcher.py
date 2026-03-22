"""
Poster Fetcher — no TMDB API
-----------------------------
Tries these sources in order:
1. Wikipedia infobox image (already extracted by wiki_scraper)
2. Wikimedia Commons search
3. DuckDuckGo instant answer / image search (no API key)
4. Google image search (scraping fallback)

Returns the best available public URL for the poster.
"""

import re
import logging
import urllib.parse
from typing import Optional
from .session import make_session, jitter_sleep, safe_get
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def _wikimedia_search(session, title: str) -> Optional[str]:
    """Search Wikimedia Commons for a movie poster."""
    query = urllib.parse.quote(f"{title} film poster")
    url   = f"https://commons.wikimedia.org/w/index.php?search={query}&title=Special:MediaSearch&type=image"
    resp  = safe_get(session, url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "lxml")
    # First result thumbnail
    for img in soup.find_all("img", src=re.compile(r"upload\.wikimedia")):
        src = img.get("src", "")
        if src and not src.endswith(".svg"):
            # Upgrade to higher res
            src = re.sub(r"/\d+px-", "/400px-", src)
            if src.startswith("//"):
                src = "https:" + src
            return src
    return None


def _duckduckgo_image(session, title: str, language: str) -> Optional[str]:
    """Use DuckDuckGo image search (no API key needed)."""
    query   = urllib.parse.quote(f"{title} {language} film official poster")
    url     = f"https://duckduckgo.com/?q={query}&iax=images&ia=images"
    resp    = safe_get(session, url)
    if not resp:
        return None
    # Extract vqd token
    vqd = re.search(r'vqd=(["\'])([^"\']+)\1', resp.text)
    if not vqd:
        return None
    token = vqd.group(2)
    api_url = (
        f"https://duckduckgo.com/i.js?"
        f"l=wt-wt&o=json&q={query}&vqd={token}&f=,,,,,&p=1"
    )
    api_resp = safe_get(session, api_url)
    if not api_resp:
        return None
    try:
        data = api_resp.json()
        results = data.get("results", [])
        for r in results[:3]:
            img = r.get("image", "")
            if img and any(ext in img.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                return img
    except Exception:
        pass
    return None


def _wikipedia_page_image(session, title: str) -> Optional[str]:
    """Use Wikipedia's REST API to get the page summary image."""
    slug = urllib.parse.quote(title.replace(" ", "_"))
    url  = f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}"
    resp = safe_get(session, url)
    if not resp:
        return None
    try:
        data = resp.json()
        # thumbnail or originalimage
        img  = data.get("originalimage") or data.get("thumbnail")
        if img:
            return img.get("source")
    except Exception:
        pass
    return None


def _wikidata_image(session, title: str) -> Optional[str]:
    """Query Wikidata for a film's image (P18)."""
    query = f"""
    SELECT ?image WHERE {{
      ?film wdt:P31 wd:Q11424 ;
            rdfs:label "{title}"@en .
      ?film wdt:P18 ?image .
    }} LIMIT 1
    """
    url  = "https://query.wikidata.org/sparql"
    resp = safe_get(session, url + "?query=" + urllib.parse.quote(query) + "&format=json")
    if not resp:
        return None
    try:
        data    = resp.json()
        results = data.get("results", {}).get("bindings", [])
        if results:
            img_url = results[0].get("image", {}).get("value", "")
            # Convert Wikimedia commons URL to direct image
            if "Special:FilePath" in img_url or img_url.startswith("http"):
                return img_url
    except Exception:
        pass
    return None


def fetch_poster(title: str, language: str, wiki_url: str = None,
                 existing_wiki_image: str = None) -> Optional[str]:
    """
    Try multiple sources to get a poster image URL.
    Returns the best URL found or None.
    """
    session = make_session()

    # 1. Already have it from wiki infobox scrape
    if existing_wiki_image:
        logger.debug(f"  [poster] Using existing wiki image for '{title}'")
        return existing_wiki_image

    # 2. Wikipedia REST API page summary
    try:
        url = _wikipedia_page_image(session, title)
        if url:
            logger.debug(f"  [poster] Wikipedia REST for '{title}': {url[:60]}")
            return url
        jitter_sleep(0.3, 0.8)
    except Exception as e:
        logger.debug(f"  [poster] Wikipedia REST failed for '{title}': {e}")

    # 3. Wikimedia Commons search
    try:
        url = _wikimedia_search(session, title)
        if url:
            logger.debug(f"  [poster] Wikimedia Commons for '{title}': {url[:60]}")
            return url
        jitter_sleep(0.5, 1.2)
    except Exception as e:
        logger.debug(f"  [poster] Wikimedia failed for '{title}': {e}")

    # 4. DuckDuckGo image search
    try:
        url = _duckduckgo_image(session, title, language)
        if url:
            logger.debug(f"  [poster] DuckDuckGo for '{title}': {url[:60]}")
            return url
        jitter_sleep(0.5, 1.5)
    except Exception as e:
        logger.debug(f"  [poster] DuckDuckGo failed for '{title}': {e}")

    # 5. Wikidata P18
    try:
        url = _wikidata_image(session, title)
        if url:
            logger.debug(f"  [poster] Wikidata for '{title}': {url[:60]}")
            return url
    except Exception as e:
        logger.debug(f"  [poster] Wikidata failed for '{title}': {e}")

    logger.info(f"  [poster] No poster found for '{title}'")
    return None
