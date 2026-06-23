"""Pipeline run metrics tracker — timezone-aware."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class PipelineMetrics:
    source: str
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    rows_ingested: int = 0
    rows_failed: int = 0
    errors: list[str] = field(default_factory=list)

    def record_success(self, count: int = 1) -> None:
        self.rows_ingested += count

    def record_failure(self, reason: str) -> None:
        self.rows_failed += 1
        self.errors.append(reason)

    def finish(self) -> None:
        self.finished_at = datetime.now(tz=UTC)

    @property
    def error_log(self) -> str | None:
        return "\n".join(self.errors) if self.errors else None

    def summary(self) -> str:
        duration = (
            (self.finished_at - self.started_at).total_seconds() if self.finished_at else None
        )
        if duration is not None:
            return (
                f"source={self.source!r} ingested={self.rows_ingested} "
                f"failed={self.rows_failed} duration={duration:.2f}s"
            )
        return f"source={self.source!r} ingested={self.rows_ingested} failed={self.rows_failed}"
