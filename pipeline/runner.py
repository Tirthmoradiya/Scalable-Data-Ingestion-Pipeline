"""
PipelineRunner — orchestrates a complete pipeline run.

Features
--------
- Chunked ingestion (never loads full dataset into memory)
- Parallel chunk processing via ThreadPoolExecutor
- Per-chunk retry with exponential back-off
- Dead-letter file for exhausted records
- Prometheus metrics instrumentation
- Structured logging with run-scoped context
- Pipeline-run audit record in DB

Usage
-----
    from pipeline.runner import PipelineRunner
    from pipeline.ingestion.csv_ingester import CSVIngester

    runner = PipelineRunner(db_url="sqlite:///pipeline.db")
    result = runner.run(
        ingester=CSVIngester("data/sample_orders.csv"),
        entity_type="orders",
    )
    print(result.summary())
"""

from __future__ import annotations

import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pipeline.cleaning.cleaner import DataCleaner
from pipeline.ingestion.base_ingester import BaseIngester
from pipeline.loader.db_loader import DBLoader
from pipeline.models import Base
from pipeline.settings import Settings, get_settings
from pipeline.transformations.transformer import DataTransformer
from pipeline.utils.logger import bind_run_context, clear_run_context, get_logger
from pipeline.utils.metrics import PipelineMetrics
from pipeline.utils.retry import DeadLetterWriter
from pipeline.utils.telemetry import BATCH_DURATION, ROWS_FAILED, ROWS_INGESTED, RunTimer

log = get_logger(__name__)


@dataclass
class RunResult:
    run_id: str
    source: str
    rows_ingested: int = 0
    rows_failed: int = 0
    chunks_processed: int = 0
    dead_letter_path: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        duration = (
            (self.finished_at - self.started_at).total_seconds() if self.finished_at else None
        )
        return (
            f"RunResult(id={self.run_id[:8]} source={self.source!r} "
            f"ingested={self.rows_ingested} failed={self.rows_failed} "
            f"chunks={self.chunks_processed} "
            f"duration={duration:.2f}s)"
            if duration is not None
            else f"RunResult(id={self.run_id[:8]} source={self.source!r} [in progress])"
        )


class PipelineRunner:
    """
    Orchestrates a full pipeline run: ingest → clean → validate → load.

    Parameters
    ----------
    db_url:
        SQLAlchemy database URL. Falls back to ``settings.db.url``.
    settings:
        Settings instance (auto-loaded from environment if not supplied).
    """

    def __init__(
        self,
        db_url: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        resolved_url = db_url or self._settings.db.url
        is_sqlite = resolved_url.startswith("sqlite")
        engine_kwargs: dict[str, Any] = {"echo": self._settings.db.echo}
        if not is_sqlite:
            engine_kwargs.update(
                {
                    "pool_size": self._settings.db.pool_size,
                    "max_overflow": self._settings.db.max_overflow,
                    "pool_recycle": self._settings.db.pool_recycle,
                }
            )
        self._engine = create_engine(resolved_url, **engine_kwargs)
        Base.metadata.create_all(self._engine)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(
        self,
        ingester: BaseIngester,
        entity_type: str = "generic",
        chunk_size: int | None = None,
        max_workers: int | None = None,
    ) -> RunResult:
        """
        Execute a full pipeline run for the given ingester.

        Parameters
        ----------
        ingester:
            Any ``BaseIngester`` subclass.
        entity_type:
            Label for metrics (e.g. ``"orders"``, ``"products"``).
        chunk_size:
            Rows per processing chunk (default: from settings).
        max_workers:
            Thread-pool workers (default: from settings).
        """
        run_id = str(uuid.uuid4())
        source = str(getattr(ingester, "file_path", None) or getattr(ingester, "url", entity_type))
        chunk_size = chunk_size or self._settings.pipeline.chunk_size
        max_workers = max_workers or self._settings.pipeline.max_workers

        bind_run_context(run_id=run_id, source=source)
        log.info(
            "run_started", entity_type=entity_type, chunk_size=chunk_size, max_workers=max_workers
        )

        result = RunResult(run_id=run_id, source=source)
        metrics = PipelineMetrics(source=source)

        with DeadLetterWriter(
            run_id=run_id,
            dead_letter_dir=self._settings.pipeline.dead_letter_dir,
        ) as dlq:
            result.dead_letter_path = str(dlq.path)

            with RunTimer(source=source):
                try:
                    self._process_all_chunks(
                        ingester=ingester,
                        entity_type=entity_type,
                        chunk_size=chunk_size,
                        max_workers=max_workers,
                        metrics=metrics,
                        result=result,
                        dlq=dlq,
                    )
                except Exception as exc:
                    log.error("run_failed", error=str(exc), exc_info=True)
                    result.errors.append(str(exc))
                    raise
                finally:
                    result.finished_at = datetime.now(tz=UTC)
                    metrics.finish()
                    result.rows_ingested = metrics.rows_ingested
                    result.rows_failed = metrics.rows_failed
                    self._save_run(metrics)
                    clear_run_context()

        log.info("run_complete", summary=result.summary())
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _process_all_chunks(
        self,
        ingester: BaseIngester,
        entity_type: str,
        chunk_size: int,
        max_workers: int,
        metrics: PipelineMetrics,
        result: RunResult,
        dlq: DeadLetterWriter,
    ) -> None:
        """Fan-out chunks to a ThreadPoolExecutor."""
        futures: list[Future[Any]] = []

        # Determine if ingester supports streaming chunks
        if hasattr(ingester, "ingest_chunks"):
            chunk_generator = ingester.ingest_chunks(chunk_size=chunk_size)
        else:
            # Fallback: load all, wrap in single chunk
            chunk_generator = iter([ingester.ingest()])

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for chunk_index, chunk in enumerate(chunk_generator):
                future = pool.submit(
                    self._process_chunk,
                    chunk=chunk,
                    chunk_index=chunk_index,
                    entity_type=entity_type,
                    metrics=metrics,
                    dlq=dlq,
                )
                futures.append(future)

            for future in as_completed(futures):
                ingested, failed = future.result()
                result.chunks_processed += 1
                ROWS_INGESTED.labels(source=result.source, entity=entity_type).inc(ingested)
                ROWS_FAILED.labels(source=result.source, reason_category="validation").inc(failed)

    def _process_chunk(
        self,
        chunk: list[dict[str, Any]],
        chunk_index: int,
        entity_type: str,
        metrics: PipelineMetrics,
        dlq: DeadLetterWriter,
    ) -> tuple[int, int]:
        """Process a single chunk in a worker thread. Returns (ingested, failed)."""
        with BATCH_DURATION.labels(source=entity_type).time(), Session(self._engine) as session:
            chunk_metrics = PipelineMetrics(source=f"chunk-{chunk_index}")
            transformer = DataTransformer(session, chunk_metrics)
            loader = DBLoader(session, batch_size=self._settings.pipeline.batch_size)

            cleaned = DataCleaner.clean_records(chunk)

            objects: list[Any] = []
            # Route by entity type
            if entity_type == "customers":
                objects = transformer.transform_customers(cleaned)
                loader.load_customers(objects)
            elif entity_type == "products":
                cat_map = loader.get_category_map()
                objects = transformer.transform_products(cleaned, cat_map)
                loader.load_products(objects)
            elif entity_type == "orders":
                customer_map = loader.get_customer_map()
                objects = transformer.transform_orders(cleaned, customer_map)
                loader.load_orders(objects)
            elif entity_type == "categories":
                objects = transformer.transform_categories(cleaned)
                loader.load_categories(objects)
            else:
                log.warning("unknown_entity_type", entity_type=entity_type)

            self._flush_with_retry(session)

            # Write chunk failures to dead-letter
            for err in chunk_metrics.errors:
                dlq.write(record={}, reason=err)

            # Merge chunk metrics into global metrics
            metrics.rows_ingested += chunk_metrics.rows_ingested
            metrics.rows_failed += chunk_metrics.rows_failed
            metrics.errors.extend(chunk_metrics.errors)

            log.info(
                "chunk_done",
                chunk_index=chunk_index,
                ingested=chunk_metrics.rows_ingested,
                failed=chunk_metrics.rows_failed,
            )
            return chunk_metrics.rows_ingested, chunk_metrics.rows_failed

    def _flush_with_retry(self, session: Session) -> None:
        import time

        attempts = self._settings.pipeline.retry_max_attempts
        backoff = self._settings.pipeline.retry_backoff_factor
        last_exc: Exception | None = None
        for attempt in range(attempts):
            try:
                session.commit()
                return
            except Exception as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    time.sleep(backoff * (2**attempt))
                    log.warning("flush_retry", attempt=attempt + 1, error=str(exc))
        raise last_exc

    def _save_run(self, metrics: PipelineMetrics) -> None:
        try:
            with Session(self._engine) as session:
                loader = DBLoader(session)
                loader.save_pipeline_run(metrics)
        except Exception as exc:
            log.error("failed_to_save_run", error=str(exc))
