"""
Wikipedia Movie Scraper
-----------------------
Scrapes the yearly Wikipedia pages for South Indian films:
  - "List of Malayalam films of YEAR"
  - "List of Tamil films of YEAR"
  - "List of Telugu films of YEAR"
  - "List of Kannada films of YEAR"
  - "List of Hindi films of YEAR"

Also scrapes each movie's individual Wikipedia page for:
  - Infobox poster image
  - Description (first paragraph)
  - Director / Cast / Genre from infobox
  - Release date
"""

import re
import logging
from typing import Optional
from bs4 import BeautifulSoup
from .session import make_session, jitter_sleep, safe_get

logger = logging.getLogger(__name__)

WIKI_BASE = "https://en.wikipedia.org"

# Wikipedia list page titles per language
WIKI_LIST_PAGES = {
    "Malayalam": [
        "List_of_Malayalam_films_of_{year}",
        "List_of_Malayalam_films_of_{year}_(A–M)",
        "List_of_Malayalam_films_of_{year}_(N–Z)",
    ],
    "Tamil": [
        "List_of_Tamil_films_of_{year}",
    ],
    "Telugu": [
        "List_of_Telugu_films_of_{year}",
    ],
    "Kannada": [
        "List_of_Kannada_films_of_{year}",
    ],
    "Hindi": [
        "List_of_Hindi_films_of_{year}",
    ],
}

YEARS = [2024, 2025, 2026]


def _clean_title(raw: str) -> str:
    """Remove disambiguation suffixes, strip whitespace."""
    title = raw.strip()
    title = re.sub(r"\s*\(film\)\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(\d{4} film\)\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\[\d+\]", "", title)           # remove [1] footnote markers
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _parse_date(raw: str) -> Optional[str]:
    """Try to parse a release date string into YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()
    # Try common formats
    for fmt in ("%d %B %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%B %Y"):
        try:
            from datetime import datetime
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Extract a year at least
    m = re.search(r"(20\d{2})", raw)
    if m:
        return m.group(1) + "-01-01"
    return None


def _extract_infobox_field(infobox, *labels) -> str:
    """Extract a field value from a Wikipedia infobox table."""
    for label in labels:
        for th in infobox.find_all("th"):
            if label.lower() in th.get_text(strip=True).lower():
                td = th.find_next_sibling("td")
                if td:
                    # Remove citation superscripts
                    for sup in td.find_all("sup"):
                        sup.decompose()
                    text = td.get_text(", ", strip=True)
                    text = re.sub(r"\[\d+\]", "", text).strip()
                    return text
    return ""


def _fetch_movie_details(session, wiki_url: str, title: str) -> dict:
    """
    Visit a movie's Wikipedia page and extract:
    poster_url, description, director, cast, genre, release_date
    """
    details = {
        "poster_url": None,
        "description": "",
        "director": "",
        "cast": "",
        "genre": "",
        "release_date": None,
        "wiki_url": wiki_url,
    }
    if not wiki_url:
        return details

    resp = safe_get(session, wiki_url)
    if not resp:
        return details

    soup = BeautifulSoup(resp.text, "lxml")

    # ── Poster image from infobox ────────────────────────────────────────
    infobox = soup.find("table", class_=re.compile(r"infobox"))
    if infobox:
        # Image: first <img> inside the infobox that is a poster (tall image)
        for img in infobox.find_all("img"):
            src = img.get("src", "")
            if src and not src.endswith(".svg"):
                full = "https:" + src if src.startswith("//") else src
                # Upgrade to higher resolution
                full = re.sub(r"/\d+px-", "/400px-", full)
                details["poster_url"] = full
                break

        details["director"]     = _extract_infobox_field(infobox, "Directed by", "Director")
        details["cast"]         = _extract_infobox_field(infobox, "Starring", "Cast")
        details["genre"]        = _extract_infobox_field(infobox, "Genre")
        raw_date                = _extract_infobox_field(infobox, "Release date", "Released")
        details["release_date"] = _parse_date(raw_date)

    # ── Description: first non-empty paragraph ───────────────────────────
    content_div = soup.find("div", id="mw-content-text")
    if content_div:
        for p in content_div.find_all("p", recursive=True):
            text = p.get_text(strip=True)
            if len(text) > 60 and not text.startswith("^"):
                text = re.sub(r"\[\d+\]", "", text).strip()
                details["description"] = text[:600]
                break

    return details


def scrape_language(language: str, years: list = None, fetch_details: bool = True) -> list[dict]:
    """
    Scrape all movies for a given language across specified years.
    Returns list of movie dicts ready for DB insertion.
    """
    if years is None:
        years = YEARS

    session = make_session()
    movies  = []
    seen    = set()   # (title.lower(), language.lower()) dedup

    templates = WIKI_LIST_PAGES.get(language, [f"List_of_{language}_films_of_{{year}}"])

    for year in years:
        for tmpl in templates:
            page_name = tmpl.format(year=year)
            url = f"{WIKI_BASE}/wiki/{page_name}"
            logger.info(f"📄 Scraping: {url}")

            resp = safe_get(session, url)
            if not resp:
                logger.warning(f"  ⚠️  Could not fetch {url}")
                continue

            jitter_sleep(0.8, 2.0)
            soup = BeautifulSoup(resp.text, "lxml")

            # ── Find all movie tables/lists on the page ───────────────────
            scraped_titles = []

            # Method 1: wikitable rows
            for table in soup.find_all("table", class_="wikitable"):
                for row in table.find_all("tr")[1:]:   # skip header
                    cells = row.find_all(["td", "th"])
                    for cell in cells:
                        link = cell.find("a", href=re.compile(r"^/wiki/"))
                        if link:
                            raw = link.get_text(strip=True)
                            href = link.get("href", "")
                            # Skip Wikipedia meta-links
                            if any(x in href for x in ["Help:", "Wikipedia:", "Category:", "File:"]):
                                continue
                            title = _clean_title(raw)
                            if len(title) > 2:
                                scraped_titles.append((title, f"{WIKI_BASE}{href}"))
                            break   # only first link per row = movie title

            # Method 2: bulleted lists (some years use this)
            if not scraped_titles:
                for li in soup.find_all("li"):
                    link = li.find("a", href=re.compile(r"^/wiki/"))
                    if link:
                        raw = link.get_text(strip=True)
                        href = link.get("href", "")
                        if any(x in href for x in ["Help:", "Wikipedia:", "Category:", "File:", "List_of"]):
                            continue
                        title = _clean_title(raw)
                        if len(title) > 2:
                            scraped_titles.append((title, f"{WIKI_BASE}{href}"))

            logger.info(f"  Found {len(scraped_titles)} titles on {page_name}")

            for title, wiki_url in scraped_titles:
                key = (title.lower(), language.lower())
                if key in seen:
                    continue
                seen.add(key)

                # Determine release type
                from datetime import datetime
                release_type = "released" if year <= datetime.now().year else "upcoming"

                movie = {
                    "title":        title,
                    "language":     language,
                    "release_type": release_type,
                    "wiki_url":     wiki_url,
                    "poster_url":   None,
                    "description":  "",
                    "director":     "",
                    "cast":         "",
                    "genre":        "",
                    "release_date": f"{year}-01-01",
                }

                if fetch_details and wiki_url:
                    try:
                        details = _fetch_movie_details(session, wiki_url, title)
                        movie.update({k: v for k, v in details.items() if v})
                        jitter_sleep(0.5, 1.5)
                    except Exception as e:
                        logger.warning(f"  Detail fetch failed for '{title}': {e}")

                movies.append(movie)

    logger.info(f"✅ {language}: {len(movies)} movies scraped")
    return movies


def scrape_all_languages(years: list = None, fetch_details: bool = True) -> list[dict]:
    """Scrape all configured languages."""
    all_movies = []
    for lang in WIKI_LIST_PAGES.keys():
        try:
            movies = scrape_language(lang, years=years, fetch_details=fetch_details)
            all_movies.extend(movies)
        except Exception as e:
            logger.error(f"Language scrape failed for {lang}: {e}")
    return all_movies
