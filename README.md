# 🎬 MovieBase — Movie Data Ingestion Service

A production-ready Python microservice that:
- Scrapes South Indian film data from Wikipedia (no TMDB API)
- Downloads & uploads posters to Cloudinary
- Stores canonical data in PostgreSQL
- Exposes a secure FastAPI service for your main server to pull from

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env with your PostgreSQL URL, API key, Cloudinary creds

# 3. Run
uvicorn main:app --host 0.0.0.0 --port 8000
```

The service will:
- Create DB tables automatically on startup
- Start the 6-hour sync scheduler (if `RUN_SCHEDULER=true`)
- Expose the API at `http://localhost:8000`

---

## 📡 API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | None | Service + DB status |
| GET | `/movies` | ✅ | Paginated movie list with Cloudinary poster URLs |
| GET | `/movies/count` | ✅ | Total count, optional language filter |
| POST | `/sync` | ✅ | Trigger background scrape now |
| POST | `/sync/posters` | ✅ | Upload missing posters to Cloudinary |

**Authentication:** Pass your `SYNC_API_KEY` as the `access_token` header.

### Example Request
```bash
curl -H "access_token: your_key" \
     "http://localhost:8000/movies?limit=20&language=Malayalam"
```

### Example Response
```json
[
  {
    "id": 1,
    "title": "L2: Empuraan",
    "language": "Malayalam",
    "release_type": "released",
    "release_date": "2025-03-27",
    "poster": "https://res.cloudinary.com/your_cloud/image/upload/moviestar/l2_empuraan.jpg",
    "description": "The massive sequel to Lucifer...",
    "director": "Prithviraj Sukumaran",
    "cast": "Mohanlal, Prithviraj...",
    "genre": "Action, Drama",
    "updated_at": "2025-01-01T00:00:00"
  }
]
```

---

## ⚙️ Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `SYNC_API_KEY` | Secret key for API auth |
| `ALLOWED_ORIGINS` | Comma-separated CORS origins |
| `RUN_SCHEDULER` | `true` to enable 6-hour auto-sync |
| `CLOUDINARY_CLOUD_NAME` | Cloudinary cloud name |
| `CLOUDINARY_API_KEY` | Cloudinary API key |
| `CLOUDINARY_API_SECRET` | Cloudinary API secret |

---

## 📁 Structure

```
movie_base/
├── main.py              # Uvicorn entry point
├── config.py            # Settings from .env
├── requirements.txt
├── api/
│   ├── main.py          # FastAPI app + endpoints
│   └── schemas.py       # Pydantic response models
├── db/
│   ├── models.py        # SQLAlchemy ORM model
│   └── session.py       # DB engine + session
├── scrapers/
│   ├── session.py       # Hardened HTTP session (retry + backoff)
│   ├── wiki_scraper.py  # Wikipedia scraper (titles + infobox details)
│   └── poster_fetcher.py # Multi-source poster URL finder
├── worker/
│   ├── ingestion.py     # Validate → deduplicate → upsert → Cloudinary
│   └── scheduler.py     # APScheduler (every 6 hours)
└── utils/
    └── cloudinary_utils.py  # Cloudinary upload helpers
```

---

## 🌐 Deploy on Render

### PostgreSQL setup (IMPORTANT)

1. In Render dashboard → **New → PostgreSQL** → create a free DB
2. Copy the **"Internal Database URL"** (not external — internal is free and faster)
3. It looks like: `postgresql://user:pass@dpg-xxxxxxx.oregon-postgres.render.com/movie_base`
4. Paste it as `DATABASE_URL` in your Web Service's environment variables

> **Note:** If your URL starts with `postgres://` that is fine — the app converts it automatically.
> Render PostgreSQL requires SSL — this is handled automatically.

## 🌐 Deploy on Render (Web Service)

1. Create new **Web Service** → connect your repo
2. **Build Command:** `pip install -r requirements.txt`
3. **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
4. Add all env vars in Render dashboard
5. Set `RUN_SCHEDULER=true` on **only one** instance

---

## 🔌 How ms3 (Node.js) pulls from this

Every 3 hours, ms3 calls:
```
GET http://movie-base-url/movies?skip=0&limit=100
Headers: access_token: YOUR_KEY
```

It then upserts received movies into its own MongoDB — fully decoupled.
