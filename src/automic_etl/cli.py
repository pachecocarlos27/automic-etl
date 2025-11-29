"""Command-line interface for Automic ETL."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from automic_etl import __version__

app = typer.Typer(
    name="automic",
    help="AI-Augmented ETL Tool for Lakehouse Architecture",
    add_completion=False,
)

console = Console()


# ============================================================================
# Initialization Commands
# ============================================================================

@app.command()
def init(
    config_path: str = typer.Option(
        "config/settings.yaml",
        "--config", "-c",
        help="Path to configuration file",
    ),
    provider: str = typer.Option(
        "aws",
        "--provider", "-p",
        help="Cloud provider (aws, gcp, azure)",
    ),
) -> None:
    """Initialize a new lakehouse."""
    from automic_etl.core.config import Settings, get_settings
    from automic_etl.medallion import Lakehouse

    console.print(Panel.fit(
        f"[bold blue]Automic ETL v{__version__}[/bold blue]\n"
        "Initializing Lakehouse...",
        title="Initialization",
    ))

    try:
        settings = get_settings(config_path if Path(config_path).exists() else None)
        lakehouse = Lakehouse(settings)
        lakehouse.initialize()

        console.print("[green]✓[/green] Lakehouse initialized successfully!")
        console.print(f"  Provider: {settings.storage.provider.value}")
        console.print(f"  Catalog: {settings.iceberg.catalog.name}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@app.command()
def version() -> None:
    """Show version information."""
    console.print(f"Automic ETL version {__version__}")


# ============================================================================
# Ingestion Commands
# ============================================================================

ingest_app = typer.Typer(help="Data ingestion commands")
app.add_typer(ingest_app, name="ingest")


@ingest_app.command("file")
def ingest_file(
    path: str = typer.Argument(..., help="File path to ingest"),
    table: str = typer.Option(..., "--table", "-t", help="Target table name"),
    source: str = typer.Option("file", "--source", "-s", help="Source identifier"),
    format: str = typer.Option(None, "--format", "-f", help="File format (csv, json, parquet)"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Ingest a file into the bronze layer."""
    import polars as pl
    from automic_etl.core.config import get_settings
    from automic_etl.medallion import Lakehouse

    console.print(f"[bold]Ingesting file:[/bold] {path}")

    # Determine format
    if format is None:
        format = Path(path).suffix.lstrip(".")

    try:
        settings = get_settings(config)
        lakehouse = Lakehouse(settings)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Reading file...", total=None)

            # Read file
            if format == "csv":
                df = pl.read_csv(path)
            elif format in ["json", "jsonl"]:
                df = pl.read_json(path) if format == "json" else pl.read_ndjson(path)
            elif format == "parquet":
                df = pl.read_parquet(path)
            else:
                console.print(f"[red]Unsupported format:[/red] {format}")
                raise typer.Exit(1)

            progress.update(task, description="Ingesting to bronze layer...")

            rows = lakehouse.ingest(
                table_name=table,
                data=df,
                source=source,
                source_file=Path(path).name,
            )

        console.print(f"[green]✓[/green] Ingested {rows} rows to bronze.{table}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@ingest_app.command("database")
def ingest_database(
    connector: str = typer.Argument(..., help="Connector type (postgresql, mysql, mongodb)"),
    table: str = typer.Option(..., "--table", "-t", help="Source table name"),
    target: str = typer.Option(None, "--target", help="Target table name (defaults to source)"),
    host: str = typer.Option("localhost", "--host", "-h", help="Database host"),
    port: int = typer.Option(None, "--port", "-p", help="Database port"),
    database: str = typer.Option(..., "--database", "-d", help="Database name"),
    user: str = typer.Option(None, "--user", "-u", help="Username"),
    password: str = typer.Option(None, "--password", help="Password"),
    query: str = typer.Option(None, "--query", "-q", help="Custom SQL query"),
    incremental: bool = typer.Option(False, "--incremental", "-i", help="Use incremental extraction"),
    watermark_column: str = typer.Option("updated_at", "--watermark", "-w", help="Watermark column"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Ingest data from a database."""
    from automic_etl.core.config import get_settings
    from automic_etl.connectors import get_connector
    from automic_etl.extraction import BatchExtractor, IncrementalExtractor
    from automic_etl.medallion import Lakehouse

    target = target or table

    console.print(f"[bold]Connecting to {connector}:[/bold] {host}:{port or 'default'}/{database}")

    try:
        settings = get_settings(config)

        # Build connector config
        conn_kwargs = {
            "host": host,
            "database": database,
        }
        if port:
            conn_kwargs["port"] = port
        if user:
            conn_kwargs["user"] = user
        if password:
            conn_kwargs["password"] = password

        conn = get_connector(connector, **conn_kwargs)

        with conn:
            lakehouse = Lakehouse(settings)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Extracting data...", total=None)

                if incremental:
                    extractor = IncrementalExtractor(settings)
                    rows = extractor.extract_to_lakehouse(
                        connector=conn,
                        source_name=f"{connector}_{database}_{table}",
                        table_name=target,
                        watermark_column=watermark_column,
                        source_table=table,
                    )
                else:
                    extractor = BatchExtractor(settings)
                    rows = extractor.extract_to_lakehouse(
                        connector=conn,
                        table_name=target,
                        source=f"{connector}_{database}",
                        query=query,
                    )

        console.print(f"[green]✓[/green] Ingested {rows} rows to bronze.{target}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


# ============================================================================
# Processing Commands
# ============================================================================

process_app = typer.Typer(help="Data processing commands")
app.add_typer(process_app, name="process")


@process_app.command("bronze-to-silver")
def process_to_silver(
    table: str = typer.Argument(..., help="Bronze table to process"),
    target: str = typer.Option(None, "--target", "-t", help="Target silver table"),
    dedup: str = typer.Option(None, "--dedup", "-d", help="Columns for deduplication (comma-separated)"),
    incremental: bool = typer.Option(True, "--incremental/--full", help="Incremental processing"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Process data from bronze to silver layer."""
    from automic_etl.core.config import get_settings
    from automic_etl.medallion import Lakehouse

    target = target or table
    dedup_columns = dedup.split(",") if dedup else None

    console.print(f"[bold]Processing:[/bold] bronze.{table} -> silver.{target}")

    try:
        settings = get_settings(config)
        lakehouse = Lakehouse(settings)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Processing to silver...", total=None)

            rows = lakehouse.process_to_silver(
                bronze_table=table,
                silver_table=target,
                dedup_columns=dedup_columns,
                incremental=incremental,
            )

        console.print(f"[green]✓[/green] Processed {rows} rows to silver.{target}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@process_app.command("silver-to-gold")
def process_to_gold(
    table: str = typer.Argument(..., help="Silver table to aggregate"),
    target: str = typer.Option(..., "--target", "-t", help="Target gold table"),
    group_by: str = typer.Option(..., "--group-by", "-g", help="Columns to group by (comma-separated)"),
    metrics: str = typer.Option(..., "--metrics", "-m", help="Metrics as 'name:column:agg' (comma-separated)"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Aggregate data from silver to gold layer."""
    from automic_etl.core.config import get_settings
    from automic_etl.medallion import Lakehouse
    from automic_etl.medallion.gold import AggregationType

    console.print(f"[bold]Aggregating:[/bold] silver.{table} -> gold.{target}")

    try:
        settings = get_settings(config)
        lakehouse = Lakehouse(settings)

        # Parse group by
        group_cols = [c.strip() for c in group_by.split(",")]

        # Parse metrics (format: name:column:agg)
        aggregations = {}
        for metric in metrics.split(","):
            parts = metric.strip().split(":")
            if len(parts) == 3:
                name, col, agg = parts
                agg_type = AggregationType(agg.lower())
                aggregations[name] = [(col, agg_type)]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Aggregating to gold...", total=None)

            rows = lakehouse.aggregate_to_gold(
                silver_table=table,
                gold_table=target,
                group_by=group_cols,
                aggregations=aggregations,
            )

        console.print(f"[green]✓[/green] Aggregated {rows} rows to gold.{target}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


# ============================================================================
# Query Commands
# ============================================================================

@app.command()
def query(
    sql: str = typer.Argument(None, help="SQL query to execute"),
    table: str = typer.Option(None, "--table", "-t", help="Table to query"),
    layer: str = typer.Option("silver", "--layer", "-l", help="Layer to query (bronze, silver, gold)"),
    limit: int = typer.Option(10, "--limit", "-n", help="Row limit"),
    output: str = typer.Option("table", "--output", "-o", help="Output format (table, json, csv)"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Query data from the lakehouse."""
    from automic_etl.core.config import get_settings
    from automic_etl.medallion import Lakehouse

    try:
        settings = get_settings(config)
        lakehouse = Lakehouse(settings)

        if sql:
            df = lakehouse.sql(sql)
        elif table:
            df = lakehouse.query(table=table, layer=layer, limit=limit)
        else:
            console.print("[red]Error:[/red] Provide either SQL or --table")
            raise typer.Exit(1)

        if df.is_empty():
            console.print("No results found.")
            return

        # Output results
        if output == "table":
            rich_table = Table(show_header=True, header_style="bold")
            for col in df.columns:
                rich_table.add_column(col)
            for row in df.head(limit).iter_rows():
                rich_table.add_row(*[str(v) for v in row])
            console.print(rich_table)
        elif output == "json":
            console.print(df.head(limit).to_dicts())
        elif output == "csv":
            console.print(df.head(limit).write_csv())

        console.print(f"\n[dim]Showing {min(len(df), limit)} of {len(df)} rows[/dim]")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


# ============================================================================
# LLM Commands
# ============================================================================

llm_app = typer.Typer(help="LLM augmentation commands")
app.add_typer(llm_app, name="llm")


@llm_app.command("extract-entities")
def extract_entities(
    text_file: str = typer.Argument(..., help="Text file to process"),
    entity_types: str = typer.Option(None, "--types", "-t", help="Entity types (comma-separated)"),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Extract entities from text using LLM."""
    from automic_etl.core.config import get_settings
    from automic_etl.llm import EntityExtractor

    console.print(f"[bold]Extracting entities from:[/bold] {text_file}")

    try:
        settings = get_settings(config)
        extractor = EntityExtractor(settings)

        text = Path(text_file).read_text()
        types = entity_types.split(",") if entity_types else None

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Extracting entities...", total=None)
            entities = extractor.extract(text, types)

        # Display results
        table = Table(show_header=True, header_style="bold")
        table.add_column("Type")
        table.add_column("Value")
        table.add_column("Confidence")

        for entity in entities:
            table.add_row(
                entity.entity_type,
                entity.value[:50],
                f"{entity.confidence:.2f}",
            )

        console.print(table)
        console.print(f"\n[green]✓[/green] Extracted {len(entities)} entities")

        if output:
            df = extractor.extract_to_dataframe(text, types)
            if output.endswith(".json"):
                df.write_json(output)
            else:
                df.write_csv(output)
            console.print(f"Results saved to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@llm_app.command("infer-schema")
def infer_schema(
    file_path: str = typer.Argument(..., help="Data file to analyze"),
    output: str = typer.Option(None, "--output", "-o", help="Output schema file"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Infer schema from data using LLM."""
    import polars as pl
    from automic_etl.core.config import get_settings
    from automic_etl.llm import SchemaGenerator

    console.print(f"[bold]Inferring schema from:[/bold] {file_path}")

    try:
        settings = get_settings(config)
        generator = SchemaGenerator(settings)

        # Read sample data
        ext = Path(file_path).suffix.lower()
        if ext == ".csv":
            df = pl.read_csv(file_path)
        elif ext == ".json":
            df = pl.read_json(file_path)
        elif ext == ".parquet":
            df = pl.read_parquet(file_path)
        else:
            console.print(f"[red]Unsupported format:[/red] {ext}")
            raise typer.Exit(1)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing with LLM...", total=None)
            schema = generator.infer_schema(df)

        # Display results
        console.print("\n[bold]Inferred Schema:[/bold]")
        console.print(json.dumps(schema, indent=2))

        if output:
            with open(output, "w") as f:
                json.dump(schema, f, indent=2)
            console.print(f"\nSchema saved to {output}")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


@llm_app.command("nl-query")
def natural_language_query(
    question: str = typer.Argument(..., help="Natural language question"),
    tables: str = typer.Option(None, "--tables", "-t", help="Tables to query (comma-separated)"),
    execute: bool = typer.Option(False, "--execute", "-x", help="Execute the generated query"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Convert natural language to SQL query."""
    from automic_etl.core.config import get_settings
    from automic_etl.llm import QueryBuilder
    from automic_etl.medallion import Lakehouse

    console.print(f"[bold]Question:[/bold] {question}")

    try:
        settings = get_settings(config)
        query_builder = QueryBuilder(settings)
        lakehouse = Lakehouse(settings)

        # Register table schemas
        table_list = tables.split(",") if tables else []

        if not table_list:
            # Get all tables from lakehouse
            all_tables = lakehouse.list_tables()
            for layer, layer_tables in all_tables.items():
                for t in layer_tables:
                    df = lakehouse.query(t, layer=layer, limit=1)
                    query_builder.register_dataframe(f"{layer}.{t}", df)
        else:
            for t in table_list:
                parts = t.split(".")
                layer = parts[0] if len(parts) > 1 else "silver"
                table = parts[-1]
                df = lakehouse.query(table, layer=layer, limit=1)
                query_builder.register_dataframe(t, df)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Generating SQL...", total=None)
            result = query_builder.build_query(question)

        console.print("\n[bold]Generated SQL:[/bold]")
        console.print(Panel(result.sql, title="SQL"))
        console.print(f"\n[dim]Explanation: {result.explanation}[/dim]")

        if result.warnings:
            console.print(f"[yellow]Warnings:[/yellow] {', '.join(result.warnings)}")

        if execute:
            console.print("\n[bold]Executing query...[/bold]")
            df = lakehouse.sql(result.sql)
            table = Table(show_header=True)
            for col in df.columns[:10]:
                table.add_column(col)
            for row in df.head(10).iter_rows():
                table.add_row(*[str(v)[:30] for v in row[:10]])
            console.print(table)

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


# ============================================================================
# UI Commands
# ============================================================================

@app.command()
def ui(
    port: int = typer.Option(8501, "--port", "-p", help="Port to run the UI on"),
    host: str = typer.Option("localhost", "--host", "-h", help="Host to bind to"),
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Launch the Streamlit web UI."""
    import subprocess
    import sys
    from pathlib import Path

    console.print(Panel.fit(
        f"[bold blue]Automic ETL v{__version__}[/bold blue]\n"
        "Launching Web UI...",
        title="Web Interface",
    ))

    # Get the path to the app.py file
    app_path = Path(__file__).parent / "ui" / "app.py"

    if not app_path.exists():
        console.print(f"[red]Error:[/red] UI module not found at {app_path}")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Starting Streamlit server at http://{host}:{port}")
    console.print("[dim]Press Ctrl+C to stop the server[/dim]\n")

    try:
        subprocess.run(
            [
                sys.executable, "-m", "streamlit", "run",
                str(app_path),
                "--server.port", str(port),
                "--server.address", host,
                "--theme.primaryColor", "#1f77b4",
                "--theme.backgroundColor", "#ffffff",
                "--theme.secondaryBackgroundColor", "#f0f2f6",
            ],
            check=True,
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped[/yellow]")
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error starting UI:[/red] {e}")
        raise typer.Exit(1)


# ============================================================================
# Status Commands
# ============================================================================

@app.command()
def status(
    config: str = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Show lakehouse status."""
    from automic_etl.core.config import get_settings
    from automic_etl.medallion import Lakehouse

    try:
        settings = get_settings(config)
        lakehouse = Lakehouse(settings)

        console.print(Panel.fit(
            f"[bold blue]Automic ETL v{__version__}[/bold blue]\n"
            f"Provider: {settings.storage.provider.value}\n"
            f"Catalog: {settings.iceberg.catalog.name}",
            title="Lakehouse Status",
        ))

        tables = lakehouse.list_tables()

        for layer, layer_tables in tables.items():
            console.print(f"\n[bold]{layer.upper()} Layer[/bold] ({len(layer_tables)} tables)")
            if layer_tables:
                for t in layer_tables[:10]:
                    console.print(f"  • {t}")
                if len(layer_tables) > 10:
                    console.print(f"  ... and {len(layer_tables) - 10} more")

    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(1)


def main() -> None:
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
