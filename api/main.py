import hmac
import logging
import time
import datetime
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Security, BackgroundTasks, File, UploadFile, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
from fastapi import Request
import io
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

import os as _os
_tmpl_dir = _os.path.join(_os.path.dirname(__file__), '..', 'templates')
templates = Jinja2Templates(directory=_tmpl_dir)
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
    """No-op: date filtering removed. Returns 0."""
    return {"deleted": 0, "note": "Date filter removed — all scraped movies are kept"}


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


# ── Stats endpoint (fast — single DB query) ─────────────────────────────
@app.get("/movies/stats", tags=["movies"])
async def movie_stats(db: Session = Depends(get_db), _=Depends(verify_api_key)):
    """All stats in one query — used by admin panel dashboard."""
    from sqlalchemy import func, case
    rows = db.query(
        func.count(MovieDB.id).label("total"),
        func.count(case((MovieDB.poster != None, 1))).label("with_poster"),
        func.count(case((MovieDB.poster.like("%cloudinary%"), 1))).label("cloudinary"),
        func.count(case((MovieDB.release_type == "upcoming", 1))).label("upcoming"),
        func.count(case((MovieDB.poster_synced == True, 1))).label("synced"),
    ).first()

    # Per-language breakdown
    lang_rows = db.execute(
        __import__("sqlalchemy").text("""
            SELECT language,
                   COUNT(*) as total,
                   COUNT(CASE WHEN poster IS NOT NULL THEN 1 END) as with_poster,
                   COUNT(CASE WHEN poster LIKE '%%cloudinary%%' THEN 1 END) as cloudinary
            FROM movies
            GROUP BY language
            ORDER BY total DESC
        """)
    ).fetchall()

    return {
        "total":      rows.total      if rows else 0,
        "with_poster":rows.with_poster if rows else 0,
        "cloudinary": rows.cloudinary  if rows else 0,
        "upcoming":   rows.upcoming    if rows else 0,
        "synced":     rows.synced      if rows else 0,
        "by_language": [
            {"language": r.language, "total": r.total,
             "with_poster": r.with_poster, "cloudinary": r.cloudinary}
            for r in lang_rows
        ],
    }


# ── SSE: live sync progress ───────────────────────────────────────────────
import asyncio
from fastapi.responses import StreamingResponse

_sync_log_queue: list = []   # simple in-memory log buffer

def _push_log(msg: str):
    """Push a log message to the SSE buffer (keep last 200)."""
    _sync_log_queue.append(msg)
    if len(_sync_log_queue) > 200:
        _sync_log_queue.pop(0)

@app.get("/admin/sync-stream", tags=["admin"])
async def sync_stream(request: Request, access_token: str = Query("")):
    """SSE stream — accepts key as query param since EventSource cannot set headers."""
    if not access_token or not __import__("hmac").compare_digest(access_token.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403, detail="Unauthorized")
    """Server-Sent Events stream for live sync progress."""
    async def event_gen():
        last = len(_sync_log_queue)
        # Send existing buffer first
        for msg in _sync_log_queue:
            yield f"data: {msg}\n\n"
        while True:
            if await request.is_disconnected():
                break
            if len(_sync_log_queue) > last:
                for msg in _sync_log_queue[last:]:
                    yield f"data: {msg}\n\n"
                last = len(_sync_log_queue)
            await asyncio.sleep(1)
    return StreamingResponse(event_gen(), media_type="text/event-stream",
                              headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ── Sync ALL languages at once ────────────────────────────────────────────
@app.post("/sync/all", tags=["admin"])
async def sync_all_languages(
    background_tasks: BackgroundTasks,
    skip_posters: bool = Query(False),
    _=Depends(verify_api_key),
):
    """Scrape + enrich + upload posters for ALL languages in background."""
    from worker.ingestion import run_sync
    background_tasks.add_task(_sync_all_with_log, skip_posters)
    return SyncResponse(status="started", message="Full sync started for all languages. Watch /admin/sync-stream for live progress.")


async def _sync_all_with_log(skip_posters: bool):
    import asyncio
    from scrapers.wiki_scraper import WIKI_LIST_PAGES
    from worker.ingestion import run_sync
    langs = list(WIKI_LIST_PAGES.keys())
    _push_log(f"🚀 Starting full sync for: {', '.join(langs)}")
    try:
        # Patch logger to also push to SSE
        import logging
        class SSEHandler(logging.Handler):
            def emit(self, record):
                msg = self.format(record)
                _push_log(msg)
        handler = SSEHandler()
        handler.setLevel(logging.INFO)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        await asyncio.get_event_loop().run_in_executor(
            None, lambda: run_sync(langs, None, skip_posters)
        )
        _push_log("✅ Full sync complete!")
        root_logger.removeHandler(handler)
    except Exception as e:
        _push_log(f"❌ Sync error: {e}")


# ── SSE: Live log streaming ──────────────────────────────────────────────
import asyncio
import queue as _queue
from fastapi.responses import StreamingResponse

_log_queue: _queue.Queue = _queue.Queue(maxsize=500)

class _QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            _log_queue.put_nowait(self.format(record))
        except _queue.Full:
            pass

# Attach queue handler to root logger
_qh = _QueueHandler()
_qh.setLevel(logging.INFO)
logging.getLogger().addHandler(_qh)


@app.get("/admin/log-stream", tags=["admin"])
async def log_stream(request: Request, key: str = ""):
    """Server-Sent Events — streams live log to admin panel."""
    import hmac
    if not key or not hmac.compare_digest(key.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403)

    async def generate():
        yield "data: connected\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                msg = _log_queue.get_nowait()
                # Classify log level for colour coding
                level = "ok" if "[INFO]" in msg else "warn" if "[WARNING]" in msg else "err" if "[ERROR]" in msg else "info"
                yield f"data: {level}|{msg}\n\n"
            except _queue.Empty:
                yield "data: ping\n\n"
                await asyncio.sleep(1)

    return StreamingResponse(generate(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


# ── Admin Panel HTML ─────────────────────────────────────────────────────
@app.get("/admin", response_class=HTMLResponse, tags=["admin"])
async def admin_panel(request: Request):
    """Admin panel UI — no auth at route level, UI handles key verification."""
    return templates.TemplateResponse("admin.html", {"request": request})


# ── Admin: Add movie manually ─────────────────────────────────────────────
@app.post("/admin/add-movie", tags=["admin"])
async def admin_add_movie(
    payload: dict,
    db: Session = Depends(get_db),
    _=Depends(verify_api_key),
):
    import datetime
    try:
        movie = MovieDB(
            title        = payload.get("title","").strip(),
            language     = payload.get("language","").strip(),
            release_type = payload.get("release_type","released"),
            release_date = payload.get("release_date"),
            poster       = payload.get("poster"),
            description  = payload.get("description",""),
            director     = payload.get("director",""),
            cast         = payload.get("cast",""),
            genre        = payload.get("genre",""),
            wiki_url     = payload.get("wiki_url"),
            poster_synced= bool(payload.get("poster","") and "cloudinary" in (payload.get("poster","") or "")),
            created_at   = datetime.datetime.utcnow(),
            updated_at   = datetime.datetime.utcnow(),
        )
        db.add(movie)
        db.commit()
        db.refresh(movie)
        return {"id": movie.id, "title": movie.title, "ok": True}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


# ── Admin: Upload image file → Cloudinary ─────────────────────────────────
@app.post("/admin/upload-image-file", tags=["admin"])
async def upload_image_file(
    file:     UploadFile = File(...),
    title:    str        = Form("movie"),
    movie_id: str        = Form(None),
    db:       Session    = Depends(get_db),
    _=Depends(verify_api_key),
):
    from utils.cloudinary_utils import upload_poster_from_url, _safe_id, _ensure_configured
    import cloudinary.uploader, io as _io

    image_bytes = await file.read()
    if not image_bytes:
        raise HTTPException(status_code=400, detail="Empty file")

    _ensure_configured()
    safe_id = _safe_id(title)
    try:
        result = cloudinary.uploader.upload(
            _io.BytesIO(image_bytes),
            public_id      = safe_id,
            overwrite      = True,
            resource_type  = "image",
            transformation = [
                {"width": 300, "height": 450, "crop": "fill", "gravity": "auto"},
                {"quality": "auto:good", "fetch_format": "auto"},
            ],
        )
        cloud_url = result.get("secure_url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # If movie_id provided, update the record
    if movie_id and cloud_url:
        try:
            movie = db.query(MovieDB).filter(MovieDB.id == int(movie_id)).first()
            if movie:
                movie.poster        = cloud_url
                movie.poster_synced = True
                db.commit()
        except Exception:
            db.rollback()

    return {"cloudinary_url": cloud_url, "ok": True}


# ── Admin: Upload image from URL → Cloudinary ──────────────────────────────
@app.post("/admin/upload-image", tags=["admin"])
async def upload_image_url(
    payload: dict,
    _=Depends(verify_api_key),
):
    from utils.cloudinary_utils import upload_poster_from_url
    url   = payload.get("url","").strip()
    title = payload.get("title","movie")
    if not url: raise HTTPException(status_code=400, detail="url required")
    cloud_url = upload_poster_from_url(url, title)
    return {"cloudinary_url": cloud_url, "ok": True}


# ── Admin: Remove image from a movie ──────────────────────────────────────
@app.post("/admin/remove-image/{movie_id}", tags=["admin"])
async def remove_image(
    movie_id: int,
    db: Session = Depends(get_db),
    _=Depends(verify_api_key),
):
    movie = db.query(MovieDB).filter(MovieDB.id == movie_id).first()
    if not movie: raise HTTPException(status_code=404, detail="Movie not found")
    movie.poster        = None
    movie.poster_synced = False
    db.commit()
    return {"ok": True, "movie": movie.title}


# ── Export DB (query param key for browser downloads) ─────────────────────
@app.get("/export", tags=["admin"])
async def export_db(key: str = Query(""), db: Session = Depends(get_db)):
    import hmac, json
    if not key or not hmac.compare_digest(key.encode(), API_KEY.encode()):
        raise HTTPException(status_code=403, detail="Unauthorized")
    movies = [m.__dict__.copy() for m in db.query(MovieDB).all()]
    for m in movies: m.pop('_sa_instance_state', None)
    payload = json.dumps({"exportedAt": str(datetime.datetime.utcnow()), "movies": movies}, default=str, indent=2)
    return Response(content=payload, media_type="application/json",
                    headers={"Content-Disposition": 'attachment; filename="moviebase-export.json"'})


@app.post("/sync/posters", response_model=SyncResponse, tags=["admin"])
async def sync_posters(background_tasks: BackgroundTasks, _=Depends(verify_api_key)):
    from worker.ingestion import sync_posters_only
    background_tasks.add_task(sync_posters_only)
    return SyncResponse(status="started", message="Poster sync started in background")
