import hmac
import logging
import time
import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Security, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text

from config import settings
from db import init_db, get_db, MovieDB
from api.schemas import MovieResponse, HealthResponse, SyncResponse
from worker.scheduler import start_scheduler, stop_scheduler, get_last_sync

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

API_KEY         = settings.SYNC_API_KEY
_api_key_header = APIKeyHeader(name="access_token", auto_error=False)


async def verify_api_key(token: str = Security(_api_key_header)):
    if not token or not hmac.compare_digest(token.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403, detail="Unauthorized")
    return token


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 movie_base starting up…")
    init_db()
    start_scheduler()
    yield
    stop_scheduler()
    logger.info("👋 movie_base shut down")


app = FastAPI(title="MovieBase API", version="1.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_methods=["GET","POST"],
    allow_headers=["access_token","Content-Type"])


@app.get("/health", response_model=HealthResponse, tags=["status"])
def health(db: Session = Depends(get_db)):
    db_status = "ok"
    count = 0
    try:
        db.execute(text("SELECT 1"))
        count = db.query(MovieDB).count()
    except Exception as e:
        db_status = f"error: {e}"
    return HealthResponse(status="ok" if db_status=="ok" else "degraded",
                          db=db_status, last_sync=get_last_sync(), version="1.0.0")


@app.get("/movies", response_model=List[MovieResponse], tags=["movies"])
def get_movies(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    language: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _=Depends(verify_api_key),
):
    q = db.query(MovieDB).order_by(MovieDB.updated_at.desc())
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return q.offset(skip).limit(limit).all()


@app.get("/movies/count", tags=["movies"])
def movie_count(language: Optional[str] = Query(None),
                db: Session = Depends(get_db),
                _=Depends(verify_api_key)):
    q = db.query(MovieDB)
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return {"count": q.count()}


# ── Cleanup: remove actor/person records ─────────────────────────────────
@app.post("/cleanup/old", tags=["admin"])
def cleanup_old_movies(db: Session = Depends(get_db), _=Depends(verify_api_key)):
    """Remove released movies older than Dec 1 2025."""
    from sqlalchemy import and_
    deleted = (
        db.query(MovieDB)
        .filter(
            MovieDB.release_type == "released",
            MovieDB.release_date != None,
            MovieDB.release_date < "2025-12-01",
        )
        .delete(synchronize_session=False)
    )
    db.commit()
    logger.info(f"✅ Removed {deleted} old movies (before Dec 2025)")
    return {"deleted": deleted}


@app.post("/cleanup/actors", tags=["admin"])
def cleanup_actors(db: Session = Depends(get_db), _=Depends(verify_api_key)):
    """
    Remove any non-film entries (actors, directors, etc.) from the DB.
    Detects person records by checking wiki_url for person indicators.
    """
    person_patterns = [
        "_(actor)", "_(actress)", "_(director)", "_(singer)",
        "_(musician)", "_(politician)", "_(cricketer)", "_(footballer)",
        "(born_",
    ]
    deleted = 0
    all_movies = db.query(MovieDB).all()
    for m in all_movies:
        url = (m.wiki_url or "").lower()
        if any(p in url for p in person_patterns):
            db.delete(m)
            deleted += 1
            logger.info(f"  🗑️  Removing person record: {m.title}")
    db.commit()
    logger.info(f"✅ Cleanup done: {deleted} non-film records removed")
    return {"deleted": deleted}


@app.post("/sync", response_model=SyncResponse, tags=["admin"])
async def trigger_sync(
    background_tasks: BackgroundTasks,
    languages: Optional[str] = Query(None),
    skip_posters: bool = Query(True),
    _=Depends(verify_api_key),
):
    from worker.ingestion import run_sync
    lang_list = [l.strip() for l in languages.split(",")] if languages else None
    background_tasks.add_task(run_sync, lang_list, None, skip_posters)
    return SyncResponse(status="started",
                        message=f"Scraping for {lang_list or 'all languages'}")


@app.post("/sync/now", tags=["admin"])
async def sync_now(
    language: str = Query("Malayalam"),
    max_movies: int = Query(500, ge=1, le=500),
    skip_posters: bool = Query(True),
    _=Depends(verify_api_key),
):
    """Fast synchronous scrape — titles only, no individual page visits."""
    from scrapers.wiki_scraper import scrape_language
    from worker.ingestion import _is_valid, _normalize, _deduplicate, _upsert_movie
    from db import SessionLocal

    start = time.time()
    logger.info(f"🔧 /sync/now: language={language}")

    try:
        raw = scrape_language(language, fetch_details=False)
    except Exception as e:
        logger.error(f"Scrape error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    valid   = [_normalize(m) for m in raw if _is_valid(m)]
    deduped = _deduplicate(valid)[:max_movies]

    db = SessionLocal()
    saved = 0
    errors = []
    sample = []
    try:
        for m in deduped:
            try:
                _upsert_movie(db, m)
                db.flush()
                saved += 1
                if len(sample) < 5:
                    sample.append({"title": m["title"], "wiki_url": m.get("wiki_url")})
            except Exception as e:
                db.rollback()
                errors.append(f"{m.get('title')}: {str(e)[:80]}")
        db.commit()
    finally:
        db.close()

    return {"language": language, "scraped_raw": len(raw),
            "valid": len(valid), "saved": saved,
            "errors": errors[:5], "elapsed_sec": round(time.time()-start, 1),
            "sample": sample}


@app.post("/sync/details", tags=["admin"])
async def sync_details_now(
    language: str = Query("Malayalam"),
    batch_size: int = Query(20, ge=1, le=30),
    _=Depends(verify_api_key),
):
    """Enrich existing movies with details from individual Wikipedia pages."""
    from scrapers.wiki_scraper import _fetch_movie_details, _is_film_page
    from scrapers.session import make_session, jitter_sleep
    from utils.cloudinary_utils import upload_poster_from_url
    from db import SessionLocal
    from bs4 import BeautifulSoup

    start = time.time()
    db    = SessionLocal()
    done  = 0
    errors = []

    try:
        movies = (
            db.query(MovieDB)
            .filter(MovieDB.language.ilike(f"%{language}%"))
            .filter(
                (MovieDB.description == None) | (MovieDB.description == "") |
                (MovieDB.director == None)    | (MovieDB.director == "")
            )
            .limit(batch_size)
            .all()
        )
        logger.info(f"🔍 Enriching {len(movies)} {language} movies…")
        session = make_session()

        for movie in movies:
            if not movie.wiki_url:
                continue
            try:
                details = _fetch_movie_details(session, movie.wiki_url, movie.title)
                # _fetch_movie_details returns empty dict for person pages (via _is_film_page)
                if details.get("director"):
                    movie.director    = details["director"]
                if details.get("description"):
                    movie.description = details["description"][:1000]
                if details.get("cast"):
                    movie.cast        = details["cast"]
                if details.get("genre"):
                    movie.genre       = details["genre"]
                if details.get("release_date"):
                    movie.release_date = details["release_date"]
                if details.get("poster_url") and not movie.poster:
                    cloud = upload_poster_from_url(details["poster_url"], movie.title)
                    movie.poster        = cloud or details["poster_url"]
                    movie.poster_synced = bool(cloud and "cloudinary" in cloud)
                db.flush()
                done += 1
                jitter_sleep(0.5, 1.2)
            except Exception as e:
                db.rollback()
                errors.append(f"{movie.title}: {str(e)[:100]}")

        db.commit()
    except Exception as e:
        db.rollback()
        errors.append(str(e))
    finally:
        db.close()

    return {"language": language, "enriched": done,
            "errors": errors[:5], "elapsed": round(time.time()-start, 1)}


@app.post("/sync/posters", response_model=SyncResponse, tags=["admin"])
async def sync_posters(background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    from worker.ingestion import sync_posters_only
    background_tasks.add_task(sync_posters_only)
    return SyncResponse(status="started", message="Poster sync started in background")
