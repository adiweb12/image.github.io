"""
Cloudinary utilities — with retry + rate limiting to avoid 429 errors.
Wikimedia aggressively rate-limits; we back off and retry.
"""

import time
import logging
import cloudinary
import cloudinary.uploader
import cloudinary.api

from config import settings

logger     = logging.getLogger(__name__)
_configured = False

# Rate limiting — max 1 Cloudinary upload per second
_last_upload_time = 0.0
MIN_UPLOAD_INTERVAL = 1.2  # seconds between uploads


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


def _rate_limit_wait():
    global _last_upload_time
    elapsed = time.time() - _last_upload_time
    if elapsed < MIN_UPLOAD_INTERVAL:
        time.sleep(MIN_UPLOAD_INTERVAL - elapsed)
    _last_upload_time = time.time()


def upload_poster_from_url(image_url: str, title: str,
                            max_retries: int = 3) -> str | None:
    """
    Upload an image from URL to Cloudinary.
    Returns Cloudinary secure URL, original URL on failure, or None.
    Handles 429 rate limiting with exponential backoff.
    """
    if not image_url:
        return None
    if not _ensure_configured():
        logger.debug("Cloudinary not configured — returning original URL")
        return image_url

    safe_id = _safe_id(title)

    # First check if already uploaded
    try:
        info = cloudinary.api.resource(safe_id)
        existing_url = info.get("secure_url")
        if existing_url:
            logger.debug(f"  [cloudinary] Already exists: {title}")
            return existing_url
    except Exception:
        pass  # doesn't exist yet — upload it

    for attempt in range(1, max_retries + 1):
        _rate_limit_wait()
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id     = safe_id,
                overwrite     = False,
                resource_type = "image",
                transformation= [
                    {"width": 300, "height": 450, "crop": "fill", "gravity": "auto"},
                    {"quality": "auto:good", "fetch_format": "auto"},
                ],
                timeout = 20,
            )
            url = result.get("secure_url")
            if url:
                logger.info(f"☁️  Cloudinary OK: {title}")
                return url
        except cloudinary.exceptions.Error as e:
            err = str(e).lower()
            if "already exists" in err:
                # Race condition — fetch it
                try:
                    info = cloudinary.api.resource(safe_id)
                    return info.get("secure_url", image_url)
                except Exception:
                    return image_url
            elif "429" in err or "too many" in err or "rate" in err:
                wait = 2 ** attempt  # 2s, 4s, 8s
                logger.warning(f"  [cloudinary] 429 rate limit for '{title}' — waiting {wait}s (attempt {attempt})")
                time.sleep(wait)
            elif "400" in err or "invalid" in err or "loading" in err:
                # Bad image URL — skip without retrying
                logger.debug(f"  [cloudinary] Bad image URL for '{title}': {e}")
                return image_url
            else:
                logger.warning(f"  [cloudinary] Error for '{title}' (attempt {attempt}): {e}")
                if attempt < max_retries:
                    time.sleep(1.5)
        except Exception as e:
            logger.warning(f"  [cloudinary] Unexpected error for '{title}': {e}")
            if attempt < max_retries:
                time.sleep(1)

    logger.info(f"  [cloudinary] Gave up on '{title}' — using original URL")
    return image_url  # fall back to original URL
