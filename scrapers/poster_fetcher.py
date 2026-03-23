"""
Poster Fetcher — multiple sources, no TMDB API needed.
Sources tried in order:
1. Wikipedia infobox image (from wiki_scraper)
2. Wikipedia REST API page summary
3. Wikimedia Commons search
4. Wikidata P18 property
5. Google Images scraping (for upcoming movies not on Wikipedia yet)
6. Bing Images scraping (fallback)
"""

import re
import logging
import urllib.parse
from typing import Optional
from .session import make_session, jitter_sleep, safe_get
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def _wikipedia_rest(session, title: str) -> Optional[str]:
    """Wikipedia REST API page summary image."""
    slug = urllib.parse.quote(title.replace(" ", "_"))
    url  = f"https://en.wikipedia.org/api/rest_v1/page/summary/{slug}"
    resp = safe_get(session, url)
    if not resp: return None
    try:
        data = resp.json()
        img  = data.get("originalimage") or data.get("thumbnail")
        if img:
            src = img.get("source", "")
            if src and not src.endswith(".svg"): return src
    except Exception: pass
    return None


def _wikimedia_commons(session, title: str, language: str) -> Optional[str]:
    """Search Wikimedia Commons for a movie poster."""
    query = urllib.parse.quote(f"{title} {language} film poster")
    url   = f"https://commons.wikimedia.org/w/index.php?search={query}&title=Special:MediaSearch&type=image"
    resp  = safe_get(session, url)
    if not resp: return None
    soup = BeautifulSoup(resp.text, "lxml")
    for img in soup.find_all("img", src=re.compile(r"upload\.wikimedia")):
        src = img.get("src", "")
        if src and not src.endswith(".svg"):
            src = re.sub(r"/\d+px-", "/400px-", src)
            if src.startswith("//"): src = "https:" + src
            return src
    return None


def _wikidata_image(session, title: str) -> Optional[str]:
    """Query Wikidata for film image (P18)."""
    query = f"""
    SELECT ?image WHERE {{
      ?film wdt:P31 wd:Q11424 ;
            rdfs:label "{title}"@en .
      ?film wdt:P18 ?image .
    }} LIMIT 1
    """
    url  = "https://query.wikidata.org/sparql?query=" + urllib.parse.quote(query) + "&format=json"
    resp = safe_get(session, url)
    if not resp: return None
    try:
        data    = resp.json()
        results = data.get("results", {}).get("bindings", [])
        if results:
            img_url = results[0].get("image", {}).get("value", "")
            if img_url and "Special:FilePath" in img_url:
                # Convert to direct URL
                fname   = img_url.split("Special:FilePath/")[-1]
                return f"https://commons.wikimedia.org/wiki/Special:FilePath/{fname}"
            if img_url.startswith("http"): return img_url
    except Exception: pass
    return None


def _google_images(session, title: str, language: str) -> Optional[str]:
    """
    Scrape Google Images for a movie poster.
    Used for upcoming movies not yet on Wikipedia.
    """
    query = urllib.parse.quote(f"{title} {language} movie official poster 2025 2026")
    url   = f"https://www.google.com/search?q={query}&tbm=isch&hl=en"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    try:
        resp = session.get(url, headers=headers, timeout=10)
        if resp.status_code != 200: return None
        # Google embeds image URLs in JSON blobs in the HTML
        # Look for direct image URLs in the response
        matches = re.findall(r'"(https://[^"]+\.(?:jpg|jpeg|png|webp))"', resp.text)
        for m in matches:
            # Skip tiny icons/logos
            if any(x in m for x in ["gstatic", "google.com", "favicon", "logo"]): continue
            if len(m) > 20: return m
    except Exception as e:
        logger.debug(f"  [google] Error: {e}")
    return None


def _bing_images(session, title: str, language: str) -> Optional[str]:
    """Bing Images scraping as final fallback."""
    query = urllib.parse.quote(f"{title} {language} film poster")
    url   = f"https://www.bing.com/images/search?q={query}&form=HDRSC2"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MovieStar/1.0)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = session.get(url, headers=headers, timeout=10)
        if resp.status_code != 200: return None
        soup = BeautifulSoup(resp.text, "lxml")
        # Bing puts image URLs in data-src or murl attributes
        for tag in soup.find_all(attrs={"murl": True}):
            murl = tag.get("murl", "")
            if murl and murl.startswith("http") and not "bing.com" in murl:
                return murl
        # Also check img tags
        for img in soup.find_all("img", src=re.compile(r"^https?://")):
            src = img.get("src", "")
            if src and "bing.com" not in src and not src.endswith(".svg"):
                return src
    except Exception as e:
        logger.debug(f"  [bing] Error: {e}")
    return None


def fetch_poster(title: str, language: str, wiki_url: str = None,
                 existing_wiki_image: str = None,
                 is_upcoming: bool = False) -> Optional[str]:
    """
    Try multiple sources to find a poster image URL.
    For upcoming movies, tries more sources including Google/Bing.
    """
    session = make_session()

    # 1. Already have it from wiki infobox
    if existing_wiki_image:
        logger.debug(f"  [poster] Using existing wiki image for '{title}'")
        return existing_wiki_image

    # 2. Wikipedia REST API
    try:
        url = _wikipedia_rest(session, title)
        if url:
            logger.debug(f"  [poster] Wikipedia REST: '{title}'")
            return url
        jitter_sleep(0.3, 0.8)
    except Exception as e:
        logger.debug(f"  [poster] Wikipedia REST failed: {e}")

    # 3. Wikimedia Commons
    try:
        url = _wikimedia_commons(session, title, language)
        if url:
            logger.debug(f"  [poster] Wikimedia Commons: '{title}'")
            return url
        jitter_sleep(0.5, 1.0)
    except Exception as e:
        logger.debug(f"  [poster] Wikimedia failed: {e}")

    # 4. Wikidata P18
    try:
        url = _wikidata_image(session, title)
        if url:
            logger.debug(f"  [poster] Wikidata: '{title}'")
            return url
        jitter_sleep(0.3, 0.8)
    except Exception as e:
        logger.debug(f"  [poster] Wikidata failed: {e}")

    # 5. Google Images (especially useful for upcoming/unreleased films)
    try:
        url = _google_images(session, title, language)
        if url:
            logger.info(f"  [poster] Google Images: '{title}'")
            return url
        jitter_sleep(0.5, 1.0)
    except Exception as e:
        logger.debug(f"  [poster] Google failed: {e}")

    # 6. Bing Images (last resort)
    try:
        url = _bing_images(session, title, language)
        if url:
            logger.info(f"  [poster] Bing Images: '{title}'")
            return url
    except Exception as e:
        logger.debug(f"  [poster] Bing failed: {e}")

    logger.info(f"  [poster] No poster found: '{title}'")
    return None
