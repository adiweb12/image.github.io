import os
import time
import hmac
import logging
import datetime
import random
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from rapidfuzz import fuzz, process
from dotenv import load_dotenv

from fastapi import FastAPI, Depends, HTTPException, Security, status, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse  # High-performance JSON
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint, Index, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel, ConfigDict
from apscheduler.schedulers.background import BackgroundScheduler

# --- SETUP ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("MovieStar_Ingestor")

API_KEY = os.getenv("SYNC_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "false").lower() == "true"

if not API_KEY or not DATABASE_URL:
    raise RuntimeError("Missing SYNC_API_KEY or DATABASE_URL environment variables.")

# --- DATABASE ---
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MovieDB(Base):
    __tablename__ = "movies_v4_final"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False, index=True)
    language = Column(String, nullable=False, index=True)
    release_type = Column(String, default="upcoming")
    release_date = Column(String, nullable=True)
    poster = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    director = Column(String, nullable=True)
    genre = Column(ARRAY(String), default=list)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('title', 'language', name='uq_title_language'),
        Index('idx_updated_at_desc', updated_at.desc()),
    )

# --- SCHEMAS ---
class MovieResponse(BaseModel):
    title: str
    language: str
    release_type: str
    release_date: Optional[str]
    poster: Optional[str]
    description: Optional[str]
    director: Optional[str]
    genre: List[str]
    model_config = ConfigDict(from_attributes=True)

# --- SCRAPER ---
class HardenedScraper:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"}

    def scrape_wiki(self, lang: str):
        url = f"https://en.wikipedia.org/wiki/List_of_{lang.capitalize()}_films_of_2026"
        try:
            time.sleep(random.uniform(1, 2))
            res = self.session.get(url, headers=self.headers, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')
            movies = []
            for table in soup.find_all('table', {'class': 'wikitable'}):
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 4:
                        idx = 1 if (cols[0].has_attr('rowspan') and len(cols) > 4) else 0
                        title = cols[idx].get_text(strip=True).split('(')[0].strip()
                        if not title or title.isdigit() or len(title) < 2: continue
                        movies.append({
                            "title": title,
                            "director": cols[-2].get_text(strip=True),
                            "cast": cols[-1].get_text(strip=True),
                            "language": lang.capitalize()
                        })
            return movies
        except Exception as e:
            logger.error(f"Scrape failed for {lang}: {e}")
            return []

# --- TASKS ---
LAST_SYNC = {"time": None, "result": "never"}

def sync_job_task():
    global LAST_SYNC
    db = SessionLocal()
    scraper = HardenedScraper()
    langs = ["malayalam", "telugu", "tamil", "kannada"]
    try:
        for lang in langs:
            movies = scraper.scrape_wiki(lang)
            for m in movies:
                try:
                    existing = db.query(MovieDB).filter_by(title=m['title'], language=m['language']).first()
                    if existing:
                        existing.director = m['director']
                        existing.description = f"Starring: {m['cast']}"
                    else:
                        db.add(MovieDB(title=m['title'], language=m['language'], 
                                       director=m['director'], description=f"Starring: {m['cast']}"))
                    db.flush()
                except Exception as e:
                    logger.error(f"Entry error {m['title']}: {e}")
                    db.rollback()
            db.commit()
        LAST_SYNC = {"time": datetime.datetime.utcnow().isoformat(), "result": "success"}
    finally:
        db.close()

# --- APP ---
app = FastAPI(title="MovieStar Ingestor V4.1", default_response_class=ORJSONResponse)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["*"], allow_headers=["*"])

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

async def verify_key(token: str = Security(APIKeyHeader(name="access_token", auto_error=False))):
    if not token or not hmac.compare_digest(token, API_KEY):
        raise HTTPException(status_code=403, detail="Unauthorized")
    return token

@app.on_event("startup")
def startup():
    if RUN_SCHEDULER:
        scheduler = BackgroundScheduler()
        scheduler.add_job(sync_job_task, 'interval', hours=6)
        scheduler.start()
        logger.info("Scheduler Active.")

@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(func.now())
    return {"status": "ok", "last_sync": LAST_SYNC}

@app.get("/movies", response_model=List[MovieResponse])
def get_movies(skip: int = 0, limit: int = Query(20, ge=1, le=100), db: Session = Depends(get_db), _=Depends(verify_key)):
    return db.query(MovieDB).order_by(MovieDB.updated_at.desc()).offset(skip).limit(limit).all()

@app.post("/sync", status_code=202)
def manual_sync(bt: BackgroundTasks, _=Depends(verify_key)):
    bt.add_task(sync_job_task)
    return {"message": "Sync queued"}
