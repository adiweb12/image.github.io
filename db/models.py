import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime,
    UniqueConstraint, Index, ARRAY, Boolean
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MovieDB(Base):
    __tablename__ = "movies"

    id           = Column(Integer, primary_key=True, index=True)
    title        = Column(String(500), nullable=False, index=True)
    language     = Column(String(100), nullable=False, index=True)
    release_type = Column(String(50), default="released")   # released | upcoming | trending
    release_date = Column(String(50), nullable=True)
    poster       = Column(Text, nullable=True)               # Cloudinary URL
    description  = Column(Text, nullable=True)
    director     = Column(String(300), nullable=True)
    cast         = Column(Text, nullable=True)               # comma-separated
    genre        = Column(Text, nullable=True)               # comma-separated
    wiki_url     = Column(Text, nullable=True)
    poster_synced = Column(Boolean, default=False)
    created_at   = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.datetime.utcnow,
                          onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("title", "language", name="uq_title_language"),
        Index("idx_updated_at_desc", updated_at.desc()),
        Index("idx_lang_updated", "language", updated_at.desc()),
    )

    def __repr__(self):
        return f"<Movie {self.title} ({self.language})>"
