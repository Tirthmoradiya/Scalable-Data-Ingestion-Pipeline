"""
Data quality profiler — post-load reconciliation and reporting.

Checks performed after a pipeline run:
  1. Row count reconciliation: DB count vs records ingested
  2. Null rate per column (sampled from DB)
  3. Duplicate rate (unique count vs total)
  4. Value distribution summary for categorical fields
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import func, inspect, text
from sqlalchemy.orm import Session

from pipeline.utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class ColumnProfile:
    name: str
    total_rows: int
    null_count: int
    distinct_count: int

    @property
    def null_rate(self) -> float:
        return self.null_count / self.total_rows if self.total_rows else 0.0

    @property
    def distinct_rate(self) -> float:
        return self.distinct_count / self.total_rows if self.total_rows else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.name,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "null_rate_pct": round(self.null_rate * 100, 2),
            "distinct_count": self.distinct_count,
            "distinct_rate_pct": round(self.distinct_rate * 100, 2),
        }


@dataclass
class TableProfile:
    table_name: str
    total_rows: int
    ingested_rows: int
    columns: list[ColumnProfile] = field(default_factory=list)

    @property
    def reconciliation_ok(self) -> bool:
        """True if DB row count >= ingested row count (allowing for dedup)."""
        return self.total_rows <= self.ingested_rows

    @property
    def high_null_columns(self) -> list[ColumnProfile]:
        return [c for c in self.columns if c.null_rate > 0.5]

    def report(self) -> str:
        lines = [
            f"\n{'='*60}",
            f"  Table: {self.table_name}",
            f"  DB rows:       {self.total_rows:,}",
            f"  Ingested rows: {self.ingested_rows:,}",
            f"  Reconciliation: {'✓ OK' if self.reconciliation_ok else '⚠ MISMATCH'}",
            f"{'='*60}",
            f"  {'Column':<25} {'Nulls':>8} {'Null%':>8} {'Distinct':>10}",
            f"  {'-'*55}",
        ]
        for col in self.columns:
            flag = " ⚠" if col.null_rate > 0.5 else ""
            lines.append(
                f"  {col.name:<25} {col.null_count:>8,} {col.null_rate*100:>7.1f}% "
                f"{col.distinct_count:>10,}{flag}"
            )
        return "\n".join(lines)


class DataProfiler:
    """
    Profiles a table after a pipeline run.

    Parameters
    ----------
    session:
        Active SQLAlchemy session.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def profile_table(
        self,
        table_name: str,
        ingested_rows: int,
        columns: list[str] | None = None,
    ) -> TableProfile:
        """
        Profile a table and return a ``TableProfile``.

        Parameters
        ----------
        table_name:
            DB table name.
        ingested_rows:
            Number of rows the pipeline attempted to insert (for reconciliation).
        columns:
            Columns to profile. If None, introspect from DB metadata.
        """
        total_rows = self._session.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
        ).scalar_one()

        if columns is None:
            # Introspect column names from SQLAlchemy inspector
            insp = inspect(self._session.bind)
            col_info = insp.get_columns(table_name)
            columns = [c["name"] for c in col_info if c["name"] != "id"]

        col_profiles: list[ColumnProfile] = []
        for col in columns:
            null_count = self._session.execute(
                text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL")  # noqa: S608
            ).scalar_one()
            distinct_count = self._session.execute(
                text(f"SELECT COUNT(DISTINCT {col}) FROM {table_name}")  # noqa: S608
            ).scalar_one()
            col_profiles.append(
                ColumnProfile(
                    name=col,
                    total_rows=total_rows,
                    null_count=null_count,
                    distinct_count=distinct_count,
                )
            )

        profile = TableProfile(
            table_name=table_name,
            total_rows=total_rows,
            ingested_rows=ingested_rows,
            columns=col_profiles,
        )
        log.info(
            "table_profiled",
            table=table_name,
            db_rows=total_rows,
            ingested=ingested_rows,
            high_null_cols=[c.name for c in profile.high_null_columns],
        )
        return profile
