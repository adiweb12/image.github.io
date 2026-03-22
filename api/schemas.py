from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class MovieResponse(BaseModel):
    id:           int
    title:        str
    language:     str
    release_type: str = "released"
    release_date: Optional[str] = None
    poster:       Optional[str] = None    # Cloudinary URL
    description:  Optional[str] = None
    director:     Optional[str] = None
    cast:         Optional[str] = None
    genre:        Optional[str] = None
    wiki_url:     Optional[str] = None
    updated_at:   Optional[datetime] = None

    model_config = {"from_attributes": True}


class HealthResponse(BaseModel):
    status:    str
    db:        str
    last_sync: Optional[str] = None
    version:   str = "1.0.0"


class SyncResponse(BaseModel):
    status:    str
    message:   str


class SyncResult(BaseModel):
    scraped:  int = 0
    inserted: int = 0
    updated:  int = 0
    failed:   int = 0
