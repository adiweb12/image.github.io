"""
Cloudinary utilities.
Downloads image to memory first, then uploads bytes to Cloudinary.
This avoids Wikimedia 429s that happen when Cloudinary fetches directly from Wikipedia.
"""

import io
import time
import logging
import requests
import cloudinary
import cloudinary.uploader
import cloudinary.api

from config import settings

logger      = logging.getLogger(__name__)
_configured = False

# Space out uploads — Cloudinary free tier allows ~500/hour
MIN_INTERVAL = 2.0   # seconds between uploads

_last_upload = 0.0


def _ensure_configured() -> bool:
    global _configured
    if not _configured and settings.cloudinary_configured:
        cloudinary.config(
            cloud_name  = settings.CLOUDINARY_CLOUD_NAME,
            api_key     = settings.CLOUDINARY_API_KEY,
            api_secret  = settings.CLOUDINARY_API_SECRET,
            secure      = True,
        )
        _configured = True
    return _configured


def _safe_id(title: str) -> str:
    return "moviestar/" + "".join(
        c if c.isalnum() or c in "-_" else "_"
        for c in title.lower()
    ).strip("_")[:80]


def _throttle():
    global _last_upload
    elapsed = time.time() - _last_upload
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_upload = time.time()


def _download_image(url: str) -> bytes | None:
    """Download image bytes from URL with browser-like headers."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MovieStar/1.0)",
        "Referer":    "https://en.wikipedia.org/",
    }
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.content
            if r.status_code == 429:
                wait = 3 * (attempt + 1)
                logger.warning(f"  [download] 429 for image, waiting {wait}s…")
                time.sleep(wait)
                continue
            logger.debug(f"  [download] HTTP {r.status_code} for {url}")
            return None
        except Exception as e:
            logger.debug(f"  [download] Error: {e}")
            if attempt < 2:
                time.sleep(2)
    return None


def upload_poster_from_url(image_url: str, title: str,
                            max_retries: int = 2) -> str | None:
    """
    Download image locally then upload bytes to Cloudinary.
    This avoids Cloudinary fetching from Wikimedia directly (which causes 429).
    Returns Cloudinary URL, original URL on failure, or None.
    """
    if not image_url:
        return None
    if not _ensure_configured():
        return image_url

    safe_id = _safe_id(title)

    # Check if already uploaded
    try:
        info = cloudinary.api.resource(safe_id)
        url  = info.get("secure_url")
        if url:
            logger.debug(f"  [cloudinary] Already exists: {title}")
            return url
    except Exception:
        pass

    # Download image bytes first (bypass Wikimedia rate limit)
    image_bytes = _download_image(image_url)
    if not image_bytes:
        logger.info(f"  [cloudinary] Could not download image for '{title}' — using original URL")
        return image_url

    # Upload bytes to Cloudinary
    for attempt in range(1, max_retries + 1):
        _throttle()
        try:
            result = cloudinary.uploader.upload(
                io.BytesIO(image_bytes),
                public_id     = safe_id,
                overwrite     = False,
                resource_type = "image",
                transformation= [
                    {"width": 300, "height": 450, "crop": "fill", "gravity": "auto"},
                    {"quality": "auto:good", "fetch_format": "auto"},
                ],
            )
            url = result.get("secure_url")
            if url:
                logger.info(f"☁️  Cloudinary OK: {title}")
                return url
        except cloudinary.exceptions.Error as e:
            err = str(e).lower()
            if "already exists" in err:
                try:
                    info = cloudinary.api.resource(safe_id)
                    return info.get("secure_url", image_url)
                except Exception:
                    return image_url
            elif "429" in err or "rate" in err:
                wait = 10 * attempt
                logger.warning(f"  [cloudinary] 429 for '{title}' — waiting {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"  [cloudinary] Error for '{title}': {e}")
                if attempt < max_retries:
                    time.sleep(2)
        except Exception as e:
            logger.warning(f"  [cloudinary] Unexpected: {e}")
            if attempt < max_retries:
                time.sleep(2)

    return image_url  # fallback to original
