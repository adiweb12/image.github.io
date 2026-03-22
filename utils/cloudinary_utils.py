import cloudinary
import cloudinary.uploader
import logging
import requests
from config import settings

logger = logging.getLogger(__name__)

_configured = False


def _ensure_configured():
    global _configured
    if not _configured and settings.cloudinary_configured:
        cloudinary.config(
            cloud_name=settings.CLOUDINARY_CLOUD_NAME,
            api_key=settings.CLOUDINARY_API_KEY,
            api_secret=settings.CLOUDINARY_API_SECRET,
            secure=True,
        )
        _configured = True
    return _configured


def upload_poster_from_url(image_url: str, title: str) -> str | None:
    """
    Download image from URL, upload to Cloudinary.
    Returns the Cloudinary secure URL or None on failure.
    """
    if not image_url:
        return None
    if not _ensure_configured():
        logger.warning("Cloudinary not configured — returning original URL")
        return image_url

    # Create a safe public_id from the movie title
    safe_id = "moviestar/" + "".join(
        c if c.isalnum() or c in "-_" else "_"
        for c in title.lower()
    ).strip("_")[:80]

    try:
        # Upload directly from URL (Cloudinary fetches it)
        result = cloudinary.uploader.upload(
            image_url,
            public_id=safe_id,
            overwrite=False,       # don't re-upload if already exists
            resource_type="image",
            transformation=[
                {"width": 300, "height": 450, "crop": "fill", "gravity": "auto"},
                {"quality": "auto:good", "fetch_format": "auto"},
            ],
        )
        url = result.get("secure_url")
        logger.info(f"☁️  Cloudinary upload OK: {title} → {url}")
        return url
    except cloudinary.exceptions.Error as e:
        # If already exists, try to get the existing URL
        if "already exists" in str(e).lower():
            try:
                info = cloudinary.api.resource(safe_id)
                return info.get("secure_url")
            except Exception:
                pass
        logger.warning(f"Cloudinary upload failed for '{title}': {e}")
        return image_url   # fall back to original URL
    except Exception as e:
        logger.warning(f"Cloudinary unexpected error for '{title}': {e}")
        return image_url


def upload_poster_from_bytes(image_bytes: bytes, title: str) -> str | None:
    """Upload raw image bytes to Cloudinary."""
    if not image_bytes:
        return None
    if not _ensure_configured():
        return None

    safe_id = "moviestar/" + "".join(
        c if c.isalnum() or c in "-_" else "_"
        for c in title.lower()
    ).strip("_")[:80]

    try:
        import io
        result = cloudinary.uploader.upload(
            io.BytesIO(image_bytes),
            public_id=safe_id,
            overwrite=False,
            resource_type="image",
            transformation=[
                {"width": 300, "height": 450, "crop": "fill", "gravity": "auto"},
                {"quality": "auto:good", "fetch_format": "auto"},
            ],
        )
        return result.get("secure_url")
    except Exception as e:
        logger.warning(f"Cloudinary bytes upload failed for '{title}': {e}")
        return None
