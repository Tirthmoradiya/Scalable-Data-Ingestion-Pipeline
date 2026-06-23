"""
APScheduler-based pipeline scheduler.

Schedules are stored in a SQLAlchemy job store (persistent across restarts).
Supported trigger types: ``cron``, ``interval``, ``date`` (one-shot).

Usage
-----
    from pipeline.scheduler import PipelineScheduler
    scheduler = PipelineScheduler()
    scheduler.add_job(
        source="csv", file_path="/data/orders.csv",
        entity_type="orders", cron="0 * * * *",
    )
    scheduler.start()
"""

from __future__ import annotations

import uuid
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from pipeline.ingestion.csv_ingester import CSVIngester
from pipeline.ingestion.json_ingester import JSONIngester
from pipeline.runner import PipelineRunner
from pipeline.settings import get_settings
from pipeline.utils.logger import get_logger

log = get_logger(__name__)
settings = get_settings()


def _execute_pipeline_job(
    source: str,
    entity_type: str,
    file_path: str | None = None,
    url: str | None = None,
    db_url: str | None = None,
    **kwargs: Any,
) -> None:
    """Job function called by APScheduler (must be module-level for pickling)."""
    log.info("scheduled_job_started", source=source, entity_type=entity_type)
    runner = PipelineRunner(db_url=db_url)

    if source == "csv" and file_path:
        ingester = CSVIngester(file_path)
    elif source in ("json", "ndjson") and file_path:
        ingester = JSONIngester(file_path, ndjson=(source == "ndjson"))
    else:
        log.error("scheduled_job_unknown_source", source=source)
        return

    try:
        result = runner.run(ingester=ingester, entity_type=entity_type)
        log.info("scheduled_job_done", summary=result.summary())
    except Exception as exc:
        log.error("scheduled_job_failed", error=str(exc), exc_info=True)


class PipelineScheduler:
    """
    Wraps APScheduler to manage scheduled pipeline jobs.

    Jobs are persisted in the pipeline DB so they survive restarts.
    """

    def __init__(self, db_url: str | None = None) -> None:
        resolved_url = db_url or settings.db.url
        jobstores = {
            "default": SQLAlchemyJobStore(url=resolved_url),
        }
        executors = {
            "default": ThreadPoolExecutor(max_workers=settings.pipeline.max_workers),
        }
        self._scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            timezone="UTC",
        )

    def start(self) -> None:
        self._scheduler.start()
        log.info("scheduler_started")

    def shutdown(self, wait: bool = True) -> None:
        self._scheduler.shutdown(wait=wait)
        log.info("scheduler_stopped")

    def add_cron_job(
        self,
        source: str,
        entity_type: str,
        cron: str,
        file_path: str | None = None,
        url: str | None = None,
        job_id: str | None = None,
        db_url: str | None = None,
    ) -> str:
        """
        Add a cron-triggered pipeline job.

        Parameters
        ----------
        cron:
            Standard 5-field cron expression (e.g. ``"0 * * * *"``).
        """
        job_id = job_id or str(uuid.uuid4())
        trigger = CronTrigger.from_crontab(cron, timezone="UTC")
        self._scheduler.add_job(
            _execute_pipeline_job,
            trigger=trigger,
            id=job_id,
            name=f"pipeline-{source}-{entity_type}",
            kwargs={
                "source": source,
                "entity_type": entity_type,
                "file_path": file_path,
                "url": url,
                "db_url": db_url,
            },
            replace_existing=True,
        )
        log.info("job_scheduled", job_id=job_id, cron=cron, source=source)
        return job_id

    def add_interval_job(
        self,
        source: str,
        entity_type: str,
        minutes: int,
        file_path: str | None = None,
        url: str | None = None,
        job_id: str | None = None,
    ) -> str:
        """Add an interval-triggered pipeline job."""
        job_id = job_id or str(uuid.uuid4())
        trigger = IntervalTrigger(minutes=minutes, timezone="UTC")
        self._scheduler.add_job(
            _execute_pipeline_job,
            trigger=trigger,
            id=job_id,
            name=f"pipeline-{source}-{entity_type}",
            kwargs={"source": source, "entity_type": entity_type, "file_path": file_path},
            replace_existing=True,
        )
        log.info("interval_job_scheduled", job_id=job_id, minutes=minutes)
        return job_id

    def remove_job(self, job_id: str) -> None:
        self._scheduler.remove_job(job_id)
        log.info("job_removed", job_id=job_id)

    def list_jobs(self) -> list[dict]:
        jobs = self._scheduler.get_jobs()
        return [
            {
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time),
                "trigger": str(job.trigger),
            }
            for job in jobs
        ]
