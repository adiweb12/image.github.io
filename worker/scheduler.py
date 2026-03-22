import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from config import settings
from worker.ingestion import run_sync

logger = logging.getLogger(__name__)

_scheduler = None
_last_sync  = None


def _job():
    global _last_sync
    import datetime
    logger.info("⏰ Scheduled sync starting…")
    try:
        totals = run_sync()
        _last_sync = datetime.datetime.utcnow().isoformat()
        logger.info(f"⏰ Scheduled sync done: {totals}")
    except Exception as e:
        logger.error(f"⏰ Scheduled sync failed: {e}")


def start_scheduler():
    global _scheduler
    if not settings.RUN_SCHEDULER:
        logger.info("⏸️  Scheduler disabled (RUN_SCHEDULER=false)")
        return

    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(
        _job,
        trigger=IntervalTrigger(hours=6),
        id="movie_sync",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
    )
    _scheduler.start()
    logger.info("✅ Scheduler started — syncing every 6 hours")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("🛑 Scheduler stopped")


def get_last_sync() -> str | None:
    return _last_sync
