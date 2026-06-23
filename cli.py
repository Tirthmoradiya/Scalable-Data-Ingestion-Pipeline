"""
Production CLI for the Scalable Data Ingestion Pipeline.
Built with Typer + Rich for a premium terminal experience.

Commands
--------
  ingest   — Run the pipeline for a source file or API
  migrate  — Run Alembic DB migrations
  status   — Show recent pipeline runs from the DB
  schedule — Manage scheduled pipeline jobs
  api      — Start the FastAPI monitoring server
  profile  — Run a data quality profile on a DB table
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from pipeline.settings import get_settings
from pipeline.utils.logger import configure_logging

app = typer.Typer(
    name="pipeline",
    help="[bold cyan]Scalable Data Ingestion Pipeline[/bold cyan] — production CLI",
    rich_markup_mode="rich",
    no_args_is_help=True,
)
schedule_app = typer.Typer(help="Manage scheduled pipeline jobs")
app.add_typer(schedule_app, name="schedule")

console = Console()
settings = get_settings()


class SourceType(StrEnum):
    csv = "csv"
    json = "json"
    ndjson = "ndjson"
    api = "api"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _print_banner() -> None:
    console.print(
        Panel.fit(
            "[bold cyan]⚡ Data Ingestion Pipeline[/bold cyan]  [dim]v1.0.0[/dim]",
            border_style="cyan",
            padding=(0, 2),
        )
    )


def _get_db_url(db_url: str | None) -> str:
    if db_url == "sqlite" or (db_url is None and settings.is_development):
        return "sqlite:///pipeline.db"
    return db_url or settings.db.url


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------
@app.command()
def ingest(
    source: SourceType = typer.Option(..., "--source", "-s", help="Ingestion source type"),
    file: Path | None = typer.Option(None, "--file", "-f", help="Path to input file"),
    url: str | None = typer.Option(None, "--url", "-u", help="REST API URL"),
    entity_type: str = typer.Option(
        "generic", "--entity", "-e", help="Entity type (customers/products/orders/categories)"
    ),
    db_url: str | None = typer.Option(None, "--db-url", help="DB URL (use 'sqlite' for local)"),
    chunk_size: int = typer.Option(1000, "--chunk-size", help="Rows per processing chunk"),
    max_workers: int = typer.Option(4, "--workers", help="Parallel worker threads"),
    profile: bool = typer.Option(
        False, "--profile", help="Run data quality profile after ingestion"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """[bold green]Ingest[/bold green] data from a file or API into the database."""
    _print_banner()
    configure_logging(
        log_level="DEBUG" if verbose else settings.obs.log_level,
        log_format=settings.obs.log_format,
    )

    from pipeline.ingestion.csv_ingester import CSVIngester
    from pipeline.ingestion.json_ingester import JSONIngester
    from pipeline.runner import PipelineRunner

    # Validate inputs
    if source in (SourceType.csv, SourceType.json, SourceType.ndjson) and file is None:
        console.print(f"[red]✗[/red] --file is required for source=[bold]{source.value}[/bold]")
        raise typer.Exit(1)
    if source == SourceType.api and url is None:
        console.print("[red]✗[/red] --url is required for source=[bold]api[/bold]")
        raise typer.Exit(1)

    resolved_db = _get_db_url(db_url)
    console.print(
        f"[dim]DB:[/dim] {resolved_db.split('@')[-1] if '@' in resolved_db else resolved_db}"
    )
    console.print(f"[dim]Source:[/dim] {file or url}  [dim]Entity:[/dim] {entity_type}")
    console.print()

    # Build ingester
    if source == SourceType.csv:
        ingester = CSVIngester(file)
    elif source in (SourceType.json, SourceType.ndjson):
        ingester = JSONIngester(file, ndjson=(source == SourceType.ndjson))
    else:
        from pipeline.ingestion.api_ingester import APIIngester

        ingester = APIIngester(url)

    runner = PipelineRunner(db_url=resolved_db)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(f"[cyan]Ingesting {entity_type}…", total=None)
        result = runner.run(
            ingester=ingester,
            entity_type=entity_type,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )
        progress.update(task, completed=True, total=1)

    # Summary table
    table = Table(title="Run Summary", box=box.ROUNDED, border_style="cyan")
    table.add_column("Field", style="bold")
    table.add_column("Value")
    table.add_row("Run ID", result.run_id[:8])
    table.add_row("Source", result.source)
    table.add_row("Rows Ingested", f"[green]{result.rows_ingested:,}[/green]")
    table.add_row(
        "Rows Failed",
        f"[red]{result.rows_failed:,}[/red]" if result.rows_failed else "[green]0[/green]",
    )
    table.add_row("Chunks Processed", str(result.chunks_processed))
    if result.finished_at:
        dur = (result.finished_at - result.started_at).total_seconds()
        table.add_row("Duration", f"{dur:.2f}s")
    if result.rows_failed:
        table.add_row("Dead-Letter File", result.dead_letter_path or "—")
    console.print(table)

    # Data quality profile
    if profile:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from pipeline.quality.profiler import DataProfiler

        eng = create_engine(resolved_db)
        entity_table_map = {
            "customers": ("customers", ["name", "email", "phone"]),
            "products": ("products", ["sku", "name", "price"]),
            "orders": ("orders", ["status", "total_amount", "ordered_at"]),
            "categories": ("categories", ["name"]),
        }
        if entity_type in entity_table_map:
            tbl, cols = entity_table_map[entity_type]
            with Session(eng) as sess:
                profiler = DataProfiler(sess)
                tp = profiler.profile_table(tbl, result.rows_ingested, cols)
                console.print(tp.report())

    if result.rows_failed > 0:
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
@app.command()
def status(
    limit: int = typer.Option(10, "--limit", "-n", help="Number of recent runs to show"),
    db_url: str | None = typer.Option(None, "--db-url"),
) -> None:
    """[bold blue]Show[/bold blue] recent pipeline runs."""
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session

    from pipeline.models import PipelineRun

    resolved_db = _get_db_url(db_url)
    eng = create_engine(resolved_db)

    with Session(eng) as sess:
        runs = sess.scalars(
            select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(limit)
        ).all()

    if not runs:
        console.print("[yellow]No pipeline runs found.[/yellow]")
        return

    table = Table(title=f"Last {limit} Pipeline Runs", box=box.ROUNDED, border_style="blue")
    table.add_column("ID", justify="right")
    table.add_column("Source", max_width=40)
    table.add_column("Ingested", justify="right", style="green")
    table.add_column("Failed", justify="right")
    table.add_column("Duration")
    table.add_column("Started At")

    for run in runs:
        dur = "—"
        if run.finished_at and run.started_at:
            dur = f"{(run.finished_at - run.started_at).total_seconds():.1f}s"
        failed_style = "red" if run.rows_failed else "green"
        table.add_row(
            str(run.id),
            run.source,
            f"{run.rows_ingested:,}",
            Text(str(run.rows_failed), style=failed_style),
            dur,
            str(run.started_at)[:19],
        )

    console.print(table)


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------
@app.command()
def migrate(
    db_url: str | None = typer.Option(None, "--db-url"),
    revision: str = typer.Option("head", "--revision", help="Alembic revision target"),
) -> None:
    """[bold yellow]Run[/bold yellow] Alembic DB migrations."""
    try:
        from alembic import command
        from alembic.config import Config as AlembicConfig

        cfg = AlembicConfig("alembic.ini")
        if db_url:
            cfg.set_main_option("sqlalchemy.url", _get_db_url(db_url))
        with console.status("[bold yellow]Applying migrations…"):
            command.upgrade(cfg, revision)
        console.print(f"[green]✓[/green] Migrations applied to [bold]{revision}[/bold]")
    except Exception as exc:
        console.print(f"[red]✗ Migration failed:[/red] {exc}")
        raise typer.Exit(1) from exc


# ---------------------------------------------------------------------------
# api
# ---------------------------------------------------------------------------
@app.command()
def api(
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(8000, "--port"),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    """[bold magenta]Start[/bold magenta] the FastAPI monitoring server."""
    import uvicorn

    console.print(f"[magenta]Starting API server on http://{host}:{port}[/magenta]")
    uvicorn.run("pipeline.api.app:app", host=host, port=port, reload=reload)


# ---------------------------------------------------------------------------
# schedule subcommands
# ---------------------------------------------------------------------------
@schedule_app.command("add")
def schedule_add(
    source: SourceType = typer.Option(..., "--source", "-s"),
    file: Path | None = typer.Option(None, "--file", "-f"),
    entity_type: str = typer.Option("generic", "--entity", "-e"),
    cron: str | None = typer.Option(None, "--cron", help="Cron expression e.g. '0 * * * *'"),
    interval_minutes: int | None = typer.Option(None, "--every", help="Interval in minutes"),
    db_url: str | None = typer.Option(None, "--db-url"),
) -> None:
    """[bold]Add[/bold] a scheduled pipeline job."""
    from pipeline.scheduler import PipelineScheduler

    if not cron and not interval_minutes:
        console.print("[red]✗[/red] Provide either --cron or --every")
        raise typer.Exit(1)

    sched = PipelineScheduler(db_url=_get_db_url(db_url))
    sched.start()

    if cron:
        job_id = sched.add_cron_job(
            source=source.value,
            entity_type=entity_type,
            cron=cron,
            file_path=str(file) if file else None,
        )
        console.print(f"[green]✓[/green] Cron job added: [bold]{job_id[:8]}[/bold] ({cron})")
    else:
        job_id = sched.add_interval_job(
            source=source.value,
            entity_type=entity_type,
            minutes=interval_minutes,
            file_path=str(file) if file else None,
        )
        console.print(
            f"[green]✓[/green] Interval job added: [bold]{job_id[:8]}[/bold] "
            f"(every {interval_minutes}m)"
        )


@schedule_app.command("list")
def schedule_list(db_url: str | None = typer.Option(None, "--db-url")) -> None:
    """[bold]List[/bold] all scheduled jobs."""
    from pipeline.scheduler import PipelineScheduler

    sched = PipelineScheduler(db_url=_get_db_url(db_url))
    sched.start()
    jobs = sched.list_jobs()
    sched.shutdown(wait=False)

    if not jobs:
        console.print("[yellow]No scheduled jobs found.[/yellow]")
        return

    table = Table(title="Scheduled Jobs", box=box.ROUNDED, border_style="magenta")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Next Run")
    table.add_column("Trigger")
    for job in jobs:
        table.add_row(job["id"][:8], job["name"], job["next_run"], job["trigger"])
    console.print(table)


# ---------------------------------------------------------------------------
# profile
# ---------------------------------------------------------------------------
@app.command()
def profile_table(
    table: str = typer.Argument(..., help="DB table name to profile"),
    ingested: int = typer.Option(0, "--ingested", help="Ingested row count for reconciliation"),
    db_url: str | None = typer.Option(None, "--db-url"),
) -> None:
    """[bold]Profile[/bold] a table's data quality."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from pipeline.quality.profiler import DataProfiler

    eng = create_engine(_get_db_url(db_url))
    with SASession(eng) as sess:
        profiler = DataProfiler(sess)
        tp = profiler.profile_table(table, ingested)
        console.print(tp.report())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app()
