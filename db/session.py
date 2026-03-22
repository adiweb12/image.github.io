from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .models import Base
from config import settings
import logging
import time

logger = logging.getLogger(__name__)


def _build_engine():
    """
    Build SQLAlchemy engine.
    - Automatically adds ?sslmode=require for Render/Railway/Supabase
      (any hosted PostgreSQL that requires SSL).
    - Falls back gracefully if connection args don't apply.
    """
    url = settings.DATABASE_URL

    # Render's PostgreSQL URL starts with postgres:// but SQLAlchemy needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    # Connect args — Render requires SSL
    connect_args = {}
    if "render.com" in url or "railway.app" in url or "supabase" in url or "neon.tech" in url:
        connect_args = {"sslmode": "require"}
    elif "localhost" in url or "127.0.0.1" in url:
        connect_args = {}   # no SSL for local dev
    else:
        # Unknown host — try SSL first, it's safer
        connect_args = {"sslmode": "require"}

    return create_engine(
        url,
        connect_args=connect_args,
        pool_pre_ping=True,     # test connections before use
        pool_size=3,
        max_overflow=5,
        pool_recycle=300,       # recycle connections every 5 min
        pool_timeout=30,
        echo=False,
    )


engine       = _build_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(retries: int = 5, delay: float = 3.0):
    """
    Create all tables. Retries on failure (useful during cold starts).
    """
    for attempt in range(1, retries + 1):
        try:
            # Verify connection works
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            Base.metadata.create_all(bind=engine)
            logger.info("✅ Database ready — tables created/verified")
            return
        except Exception as e:
            logger.warning(f"⚠️  DB init attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logger.error("❌ Could not connect to database after all retries")
                raise


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
