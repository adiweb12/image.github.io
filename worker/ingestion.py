"""
Ingestion Worker
----------------
1. Scrape Wikipedia for each language
2. Validate & deduplicate
3. Fetch posters → upload to Cloudinary
4. Upsert into PostgreSQL
"""

import logging
import datetime
from typing import Optional
from rapidfuzz import fuzz

from db import SessionLocal, MovieDB
from scrapers.wiki_scraper import scrape_language, WIKI_LIST_PAGES
from scrapers.poster_fetcher import fetch_poster
from utils.cloudinary_utils import upload_poster_from_url

logger = logging.getLogger(__name__)

FUZZ_THRESHOLD = 85    # minimum fuzzy match score to accept a title
MAX_JOB_SECONDS = 600  # 10-minute global timeout per full sync


# ── Validation ──────────────────────────────────────────────────────────

RELEASED_CUTOFF_STR = "2025-12-01"  # only include movies from Dec 2025 onwards

def _is_valid(movie: dict) -> bool:
    """Return True if movie data passes basic validation."""
    import datetime
    title = (movie.get("title") or "").strip()
    lang  = (movie.get("language") or "").strip()
    if not title or len(title) < 2:
        return False
    if not lang:
        return False

    # Filter out non-movie Wikipedia pages
    skip_patterns = [
        "list of", "category:", "template:", "disambiguation",
        "filmography", "index of", "portal:", "wikipedia:",
    ]
    if any(p in title.lower() for p in skip_patterns):
        return False

    # Filter out actor/person pages by wiki URL
    wiki_url = (movie.get("wiki_url") or "").lower()
    person_url_patterns = [
        "_(actor)", "_(actress)", "_(director)", "_(singer)",
        "_(musician)", "_(politician)", "_(cricketer)", "_(footballer)",
        "_born_", "(born_",
    ]
    if any(p in wiki_url for p in person_url_patterns):
        return False

    # Date filter: only include upcoming OR released from Dec 2025 onwards
    release_date = movie.get("release_date")
    release_type = (movie.get("release_type") or "released").lower()

    if release_type == "upcoming":
        return True  # always include upcoming

    if release_date:
        try:
            rd = datetime.datetime.strptime(release_date[:10], "%Y-%m-%d").date()
            cutoff = datetime.date(2025, 12, 1)
            if rd < cutoff:
                return False  # too old — skip
        except (ValueError, TypeError):
            pass  # can't parse date — include it anyway

    return True


def _normalize(movie: dict) -> dict:
    """Normalize all fields in place."""
    movie["title"]    = movie.get("title", "").strip()
    movie["language"] = movie.get("language", "").strip().title()
    movie["director"] = (movie.get("director") or "").strip()[:300]
    movie["cast"]     = (movie.get("cast") or "").strip()[:1000]
    movie["genre"]    = (movie.get("genre") or "").strip()[:300]
    movie["description"] = (movie.get("description") or "").strip()[:1000]
    return movie


def _deduplicate(movies: list[dict]) -> list[dict]:
    """Remove duplicates using (title.lower(), language.lower()) key."""
    seen   = {}
    result = []
    for m in movies:
        key = (m["title"].lower(), m["language"].lower())
        if key not in seen:
            seen[key] = True
            result.append(m)
    return result


# ── Poster pipeline ─────────────────────────────────────────────────────

def _ensure_poster(movie: dict) -> Optional[str]:
    """
    Get a poster URL for a movie:
    1. Use wiki infobox image if found
    2. Otherwise try poster_fetcher sources
    Then upload to Cloudinary and return the Cloudinary URL.
    """
    existing = movie.get("poster_url") or movie.get("poster")
    wiki_url = movie.get("wiki_url")

    raw_url = fetch_poster(
        title=movie["title"],
        language=movie["language"],
        wiki_url=wiki_url,
        existing_wiki_image=existing,
    )
    if not raw_url:
        return None

    # Upload to Cloudinary (or return raw if Cloudinary not configured)
    cloudinary_url = upload_poster_from_url(raw_url, movie["title"])
    return cloudinary_url


# ── DB Upsert ────────────────────────────────────────────────────────────

def _upsert_movie(db, movie: dict):
    """Insert or update a movie record in the DB."""
    existing = (
        db.query(MovieDB)
        .filter_by(title=movie["title"], language=movie["language"])
        .first()
    )
    now = datetime.datetime.utcnow()

    if existing:
        # Update non-null fields only
        if movie.get("director"):
            existing.director = movie["director"]
        if movie.get("cast"):
            existing.cast = movie["cast"]
        if movie.get("genre"):
            existing.genre = movie["genre"]
        if movie.get("description"):
            existing.description = movie["description"]
        if movie.get("release_date"):
            existing.release_date = movie["release_date"]
        if movie.get("release_type"):
            existing.release_type = movie["release_type"]
        if movie.get("wiki_url"):
            existing.wiki_url = movie["wiki_url"]
        if movie.get("poster") and not existing.poster_synced:
            existing.poster = movie["poster"]
            existing.poster_synced = True
        existing.updated_at = now
    else:
        db.add(MovieDB(
            title=movie["title"],
            language=movie["language"],
            release_type=movie.get("release_type", "released"),
            release_date=movie.get("release_date"),
            poster=movie.get("poster"),
            description=movie.get("description", ""),
            director=movie.get("director", ""),
            cast=movie.get("cast", ""),
            genre=movie.get("genre", ""),
            wiki_url=movie.get("wiki_url"),
            poster_synced=bool(movie.get("poster")),
            created_at=now,
            updated_at=now,
        ))


# ── Main sync job ────────────────────────────────────────────────────────

def run_sync(languages: list[str] = None, years: list[int] = None,
             skip_posters: bool = False):
    """
    Full ingestion run.
    languages: subset to sync, or None = all
    years: which years to scrape, or None = default
    skip_posters: skip Cloudinary upload (faster for testing)
    """
    start  = datetime.datetime.utcnow()
    langs  = languages or list(WIKI_LIST_PAGES.keys())
    db     = SessionLocal()
    totals = {"scraped": 0, "inserted": 0, "updated": 0, "failed": 0}

    logger.info(f"🚀 Sync started — languages: {langs}")

    try:
        for lang in langs:
            elapsed = (datetime.datetime.utcnow() - start).total_seconds()
            if elapsed > MAX_JOB_SECONDS:
                logger.warning(f"⏱️  Global timeout reached after {elapsed:.0f}s, stopping")
                break

            logger.info(f"\n── Ingesting {lang} ──")
            try:
                raw_movies = scrape_language(lang, years=years, fetch_details=True)
            except Exception as e:
                logger.error(f"Scrape failed for {lang}: {e}")
                continue

            valid   = [_normalize(m) for m in raw_movies if _is_valid(m)]
            deduped = _deduplicate(valid)
            totals["scraped"] += len(deduped)
            logger.info(f"  {lang}: {len(raw_movies)} scraped → {len(deduped)} valid")

            for movie in deduped:
                try:
                    # Fetch & upload poster unless skipped
                    if not skip_posters:
                        poster_url = _ensure_poster(movie)
                        if poster_url:
                            movie["poster"] = poster_url

                    # Check if insert or update
                    existing = (
                        db.query(MovieDB)
                        .filter_by(title=movie["title"], language=movie["language"])
                        .first()
                    )
                    _upsert_movie(db, movie)
                    db.flush()

                    if existing:
                        totals["updated"] += 1
                    else:
                        totals["inserted"] += 1

                except Exception as e:
                    logger.warning(f"  ⚠️  Failed: '{movie.get('title')}' — {e}")
                    db.rollback()
                    totals["failed"] += 1

            db.commit()
            logger.info(f"  ✅ {lang} committed")

    except Exception as e:
        logger.error(f"Sync job error: {e}")
        db.rollback()
    finally:
        db.close()

    elapsed = (datetime.datetime.utcnow() - start).total_seconds()
    logger.info(
        f"\n🏁 Sync complete in {elapsed:.1f}s | "
        f"scraped={totals['scraped']} inserted={totals['inserted']} "
        f"updated={totals['updated']} failed={totals['failed']}"
    )
    return totals


def sync_posters_only():
    """
    Go through all movies without a Cloudinary poster and fetch+upload.
    Useful for running separately after the main sync.
    """
    db   = SessionLocal()
    done = 0
    try:
        movies = db.query(MovieDB).filter(
            (MovieDB.poster == None) | (MovieDB.poster_synced == False)
        ).all()
        logger.info(f"🖼️  Syncing posters for {len(movies)} movies")

        for movie in movies:
            try:
                raw_url = fetch_poster(
                    title=movie.title,
                    language=movie.language,
                    wiki_url=movie.wiki_url,
                )
                if raw_url:
                    cloud_url = upload_poster_from_url(raw_url, movie.title)
                    if cloud_url:
                        movie.poster       = cloud_url
                        movie.poster_synced = True
                        db.flush()
                        done += 1
            except Exception as e:
                logger.warning(f"  Poster sync failed for '{movie.title}': {e}")
                db.rollback()

        db.commit()
        logger.info(f"✅ Poster sync done: {done} posters uploaded")
    finally:
        db.close()
    return done
