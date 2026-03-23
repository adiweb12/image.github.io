import hmac
import logging
import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Security, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from config import settings
from db import init_db, get_db, MovieDB
from api.schemas import MovieResponse, HealthResponse, SyncResponse
from worker.scheduler import start_scheduler, stop_scheduler, get_last_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
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


app = FastAPI(
    title="MovieBase API",
    description="South Indian movie data ingestion & distribution",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_methods=["GET", "POST"],
    allow_headers=["access_token", "Content-Type"],
)


# ── Health ────────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["status"])
def health(db: Session = Depends(get_db)):
    db_status = "ok"
    movie_count = 0
    try:
        from sqlalchemy import text
        db.execute(text("SELECT 1"))
        movie_count = db.query(MovieDB).count()
    except Exception as e:
        db_status = f"error: {e}"

    last = get_last_sync()
    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        db=db_status,
        last_sync=last,
        version="1.0.0",
    )


# ── Movies ────────────────────────────────────────────────────────────────
@app.get("/movies", response_model=List[MovieResponse], tags=["movies"])
def get_movies(
    skip:     int            = Query(0, ge=0),
    limit:    int            = Query(20, ge=1, le=100),
    language: Optional[str] = Query(None),
    db:       Session        = Depends(get_db),
    _=Depends(verify_api_key),
):
    q = db.query(MovieDB).order_by(MovieDB.updated_at.desc())
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return q.offset(skip).limit(limit).all()


@app.get("/movies/count", tags=["movies"])
def movie_count(
    language: Optional[str] = Query(None),
    db:       Session        = Depends(get_db),
    _=Depends(verify_api_key),
):
    q = db.query(MovieDB)
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return {"count": q.count()}


# ── Sync (background) ─────────────────────────────────────────────────────
@app.post("/sync", response_model=SyncResponse, tags=["admin"])
async def trigger_sync(
    background_tasks: BackgroundTasks,
    languages:    Optional[str] = Query(None),
    skip_posters: bool          = Query(True),   # default True — faster
    _=Depends(verify_api_key),
):
    """Trigger background ingestion. Returns immediately."""
    from worker.ingestion import run_sync
    lang_list = [l.strip() for l in languages.split(",")] if languages else None
    background_tasks.add_task(run_sync, lang_list, None, skip_posters)
    return SyncResponse(
        status="started",
        message=f"Scraping started for: {lang_list or 'all languages'}. Check /health for progress.",
    )


# ── Sync (synchronous — waits for result, use for debugging) ─────────────
@app.post("/sync/now", tags=["admin"])
async def sync_now(
    language:     str  = Query("Malayalam", description="Single language to scrape"),
    max_movies:   int  = Query(100, ge=1, le=500, description="Max movies to scrape (for testing)"),
    skip_posters: bool = Query(True),
    _=Depends(verify_api_key),
):
    """
    Synchronous scrape — runs NOW and returns the result in the HTTP response.
    Use this to debug. Limited to one language at a time.
    """
    import time
    from scrapers.wiki_scraper import scrape_language
    from worker.ingestion import _is_valid, _normalize, _deduplicate, _upsert_movie
    from db import SessionLocal

    start = time.time()
    logger.info(f"🔧 /sync/now called for language={language}")

    try:
        raw = scrape_language(language, fetch_details=True)
    except Exception as e:
        logger.error(f"Scrape error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e), "movies": []})

    valid   = [_normalize(m) for m in raw if _is_valid(m)]
    deduped = _deduplicate(valid)[:max_movies]

    db      = SessionLocal()
    saved   = 0
    errors  = []
    sample  = []

    try:
        for m in deduped:
            try:
                _upsert_movie(db, m)
                db.flush()
                saved += 1
                if len(sample) < 5:
                    sample.append({"title": m["title"], "poster": m.get("poster_url"), "director": m.get("director")})
            except Exception as e:
                db.rollback()
                errors.append(f"{m.get('title')}: {e}")
        db.commit()
    finally:
        db.close()

    elapsed = round(time.time() - start, 1)
    return {
        "language":     language,
        "scraped_raw":  len(raw),
        "valid":        len(valid),
        "saved":        saved,
        "errors":       errors[:10],
        "elapsed_sec":  elapsed,
        "sample":       sample,
    }


@app.post("/sync/posters", response_model=SyncResponse, tags=["admin"])
async def sync_posters(
    background_tasks: BackgroundTasks,
    _=Depends(verify_api_key),
):
    from worker.ingestion import sync_posters_only
    background_tasks.add_task(sync_posters_only)
    return SyncResponse(status="started", message="Poster sync started")
