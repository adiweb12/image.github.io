import hmac
import logging
import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Security, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.orm import Session

from config import settings
from db import init_db, get_db, MovieDB
from api.schemas import MovieResponse, HealthResponse, SyncResponse
from worker.scheduler import start_scheduler, stop_scheduler, get_last_sync
from worker.ingestion import run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

API_KEY    = settings.SYNC_API_KEY
_api_key_header = APIKeyHeader(name="access_token", auto_error=False)


# ── Auth ─────────────────────────────────────────────────────────────────

async def verify_api_key(token: str = Security(_api_key_header)):
    if not token or not hmac.compare_digest(token.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403, detail="Unauthorized")
    return token


# ── Lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 movie_base starting up…")
    init_db()
    start_scheduler()
    yield
    stop_scheduler()
    logger.info("👋 movie_base shut down")


# ── App ───────────────────────────────────────────────────────────────────

app = FastAPI(
    title="MovieBase API",
    description="South Indian movie data ingestion & distribution service",
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


# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["status"])
def health(db: Session = Depends(get_db)):
    """Service health check — no auth required."""
    db_status = "ok"
    try:
        db.execute(__import__("sqlalchemy").text("SELECT 1"))
    except Exception as e:
        db_status = f"error: {e}"

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        db=db_status,
        last_sync=get_last_sync(),
    )


@app.get("/movies", response_model=List[MovieResponse], tags=["movies"])
def get_movies(
    skip:     int     = Query(0,  ge=0,  description="Offset"),
    limit:    int     = Query(20, ge=1, le=100, description="Page size"),
    language: Optional[str] = Query(None, description="Filter by language"),
    db:       Session = Depends(get_db),
    _=Depends(verify_api_key),
):
    """
    Return movies sorted by last updated (newest first).
    Requires: access_token header.
    """
    q = db.query(MovieDB).order_by(MovieDB.updated_at.desc())
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return q.offset(skip).limit(limit).all()


@app.get("/movies/count", tags=["movies"])
def movie_count(
    language: Optional[str] = Query(None),
    db:       Session = Depends(get_db),
    _=Depends(verify_api_key),
):
    """Total movie count, optionally filtered by language."""
    q = db.query(MovieDB)
    if language:
        q = q.filter(MovieDB.language.ilike(f"%{language}%"))
    return {"count": q.count()}


@app.post("/sync", response_model=SyncResponse, tags=["admin"])
async def trigger_sync(
    background_tasks: BackgroundTasks,
    languages: Optional[str] = Query(None, description="Comma-separated languages to sync"),
    skip_posters: bool        = Query(False),
    _=Depends(verify_api_key),
):
    """Trigger a background ingestion job immediately."""
    lang_list = [l.strip() for l in languages.split(",")] if languages else None
    background_tasks.add_task(run_sync, lang_list, None, skip_posters)
    return SyncResponse(
        status="started",
        message=f"Sync started in background for: {lang_list or 'all languages'}",
    )


@app.post("/sync/posters", response_model=SyncResponse, tags=["admin"])
async def sync_posters(
    background_tasks: BackgroundTasks,
    _=Depends(verify_api_key),
):
    """Upload missing posters to Cloudinary in the background."""
    from worker.ingestion import sync_posters_only
    background_tasks.add_task(sync_posters_only)
    return SyncResponse(status="started", message="Poster sync started in background")
