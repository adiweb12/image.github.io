"""
movie_base — Entry Point
Run: uvicorn main:app --host 0.0.0.0 --port $PORT
"""
import sys
import os
import logging

# Ensure the project root is in the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from api.main import app   # noqa: E402 — re-export for uvicorn

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
