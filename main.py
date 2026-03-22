import os
import time
import hmac
import logging
import datetime
import random
import re
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
from fastapi.responses import ORJSONResponse
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint, Index, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel, ConfigDict
from apscheduler.schedulers.background import BackgroundScheduler

# --- INIT ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("MovieStar_Ingestor")

# --- ENV & DB URL FIX ---
API_KEY = os.getenv("SYNC_API_KEY")
raw_url = os.getenv("DATABASE_URL", "")
if raw_url.startswith("postgres://"):
    DATABASE_URL = raw_url.replace("postgres://", "postgresql://", 1)
else:
    DATABASE_URL = raw_url

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "false").lower() == "true"
MAX_SYNC_RUNTIME = 900 # Increased to 15 mins for larger data

if not API_KEY or not DATABASE_URL:
    raise RuntimeError("Missing Environment Variables.")

# --- DATABASE ---
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class MovieDB(Base):
    __tablename__ = "movies_v5_final"
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False, index=True)
    language = Column(String, nullable=False, index=True)
    release_type = Column(String, default="released") # Changed default
    release_date = Column(String, nullable=True)
    poster = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    director = Column(String, nullable=True)
    genre = Column(ARRAY(String), default=list)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('title', 'language', name='uq_title_language'),
        Index('idx_updated_at_desc', updated_at.desc()),
    )

Base.metadata.create_all(bind=engine)

# --- SCHEMAS ---
class MovieResponse(BaseModel):
    title: str
    language: str
    release_type: str
    release_date: Optional[str]
    director: Optional[str]
    description: Optional[str]
    model_config = ConfigDict(from_attributes=True)

# --- ADVANCED SCRAPER ---
class HardenedScraper:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"}

    def scrape_year(self, lang: str, year: int) -> List[dict]:
        url = f"https://en.wikipedia.org/wiki/List_of_{lang.capitalize()}_films_of_{year}"
        try:
            time.sleep(random.uniform(1, 2))
            res = self.session.get(url, headers=self.headers, timeout=15)
            if res.status_code != 200: return []
            
            soup = BeautifulSoup(res.text, 'html.parser')
            movies = []
            
            for table in soup.find_all('table', {'class': 'wikitable'}):
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 4:
                        # Handle spanning rows for months/dates
                        idx = 0
                        if cols[0].has_attr('rowspan'): idx += 1
                        if len(cols) > idx and cols[idx].has_attr('rowspan'): idx += 1
                        
                        if idx >= len(cols): continue
                        
                        title = cols[idx].get_text(strip=True).split('(')[0].strip()
                        if not title or title.isdigit() or len(title) < 2: continue
                        
                        # Determine if Released or Upcoming based on year
                        current_year = datetime.datetime.now().year
                        rel_type = "released" if year < current_year else "upcoming"
                        
                        movies.append({
                            "title": title,
                            "director": cols[-2].get_text(strip=True),
                            "cast": cols[-1].get_text(strip=True),
                            "language": lang.capitalize(),
                            "release_type": rel_type,
                            "release_date": f"{year}-TBD"
                        })
            return movies
        except Exception as e:
            logger.error(f"Failed {lang} {year}: {e}")
            return []

# --- TASKS ---
LAST_SYNC = {"time": None, "result": "never"}

def sync_job_task():
    global LAST_SYNC
    start_time = time.time()
    db = SessionLocal()
    scraper = HardenedScraper()
    langs = ["malayalam", "telugu", "tamil", "kannada"]
    years = [2025, 2026] # Scrape both years for the 1/12/2025 requirement
    
    try:
        for year in years:
            for lang in langs:
                if time.time() - start_time > MAX_SYNC_RUNTIME: break
                
                movies = scraper.scrape_year(lang, year)
                for m in movies:
                    # Filter for Dec 2025 specifically if needed, 
                    # but usually 2025 releases are safe to include.
                    try:
                        existing = db.query(MovieDB).filter_by(title=m['title'], language=m['language']).first()
                        if existing:
                            existing.director = m['director']
                            existing.description = f"Starring: {m['cast']}"
                        else:
                            db.add(MovieDB(
                                title=m['title'],
                                language=m['language'],
                                director=m['director'],
                                description=f"Starring: {m['cast']}",
                                release_type=m['release_type'],
                                release_date=m['release_date']
                            ))
                        db.flush()
                    except:
                        db.rollback()
                db.commit()
        LAST_SYNC = {"time": datetime.datetime.utcnow().isoformat(), "result": "success"}
    finally:
        db.close()

# --- FASTAPI SETUP ---
app = FastAPI(title="MovieStar Ingestor V5", default_response_class=ORJSONResponse)
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
    return {"message": "Deep Sync started (2025-2026)"}
