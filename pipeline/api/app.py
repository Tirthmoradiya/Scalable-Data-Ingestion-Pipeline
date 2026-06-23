"""
FastAPI status & monitoring API.

Endpoints
---------
GET  /health        — liveness + DB connectivity check
GET  /runs          — list recent pipeline runs (paginated)
GET  /runs/{id}     — detail for a specific run incl. error log
GET  /metrics       — Prometheus text-format metrics
GET  /docs          — Auto-generated Swagger UI (FastAPI default)
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from pipeline.models import PipelineRun
from pipeline.settings import get_settings

settings = get_settings()

app = FastAPI(
    title="Data Pipeline Status API",
    description="Monitor pipeline runs, health, and metrics.",
    version=settings.version,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api.cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------
_engine = create_engine(settings.db.url, pool_pre_ping=True)


def get_session():
    with Session(_engine) as session:
        yield session


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str
    environment: str
    version: str
    db_reachable: bool
    timestamp: datetime


class RunSummary(BaseModel):
    id: int
    source: str
    rows_ingested: int
    rows_failed: int
    started_at: datetime
    finished_at: datetime | None
    duration_seconds: float | None

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_duration(cls, run: PipelineRun) -> RunSummary:
        duration = None
        if run.finished_at and run.started_at:
            duration = (run.finished_at - run.started_at).total_seconds()
        return cls(
            id=run.id,
            source=run.source,
            rows_ingested=run.rows_ingested,
            rows_failed=run.rows_failed,
            started_at=run.started_at,
            finished_at=run.finished_at,
            duration_seconds=duration,
        )


class RunDetail(RunSummary):
    error_log: str | None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["monitoring"])
def health_check(session: Session = Depends(get_session)) -> HealthResponse:
    """Liveness probe — checks DB connectivity."""
    db_ok = False
    try:
        session.execute(select(PipelineRun).limit(1))
        db_ok = True
    except Exception:
        pass

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        environment=settings.environment.value,
        version=settings.version,
        db_reachable=db_ok,
        timestamp=datetime.now(UTC),
    )


@app.get("/runs", response_model=list[RunSummary], tags=["pipeline"])
def list_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[RunSummary]:
    """List recent pipeline runs, newest first."""
    runs = session.scalars(
        select(PipelineRun).order_by(PipelineRun.started_at.desc()).offset(offset).limit(limit)
    ).all()
    return [RunSummary.from_orm_with_duration(r) for r in runs]


@app.get("/runs/{run_id}", response_model=RunDetail, tags=["pipeline"])
def get_run(run_id: int, session: Session = Depends(get_session)) -> RunDetail:
    """Get full detail for a specific pipeline run including its error log."""
    run = session.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    duration = None
    if run.finished_at and run.started_at:
        duration = (run.finished_at - run.started_at).total_seconds()
    return RunDetail(
        id=run.id,
        source=run.source,
        rows_ingested=run.rows_ingested,
        rows_failed=run.rows_failed,
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_seconds=duration,
        error_log=run.error_log,
    )


@app.get("/metrics", response_class=PlainTextResponse, tags=["monitoring"])
def prometheus_metrics() -> str:
    """Prometheus text-format metrics endpoint."""
    return generate_latest().decode("utf-8")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def start_api() -> None:
    import uvicorn

    uvicorn.run(
        "pipeline.api.app:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.reload,
    )
