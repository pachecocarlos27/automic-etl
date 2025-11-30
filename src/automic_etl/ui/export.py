"""Data export functionality for Automic ETL UI."""

from __future__ import annotations

from typing import Any, Literal
from datetime import datetime
from io import BytesIO, StringIO
import json
import streamlit as st


ExportFormat = Literal["csv", "json", "parquet", "excel", "sql"]


def export_dataframe(
    data: Any,  # pandas DataFrame or list of dicts
    filename: str,
    format: ExportFormat = "csv",
    label: str = "Download",
    include_timestamp: bool = True,
):
    """
    Export data to downloadable file.

    Args:
        data: DataFrame or list of dicts to export
        filename: Base filename (without extension)
        format: Export format
        label: Button label
        include_timestamp: Whether to include timestamp in filename
    """
    import pandas as pd

    # Convert to DataFrame if needed
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data

    if df.empty:
        st.warning("No data to export")
        return

    # Add timestamp to filename if requested
    if include_timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename}_{timestamp}"

    # Generate file based on format
    if format == "csv":
        file_data = df.to_csv(index=False)
        mime_type = "text/csv"
        extension = "csv"

    elif format == "json":
        file_data = df.to_json(orient="records", indent=2)
        mime_type = "application/json"
        extension = "json"

    elif format == "parquet":
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        file_data = buffer.getvalue()
        mime_type = "application/octet-stream"
        extension = "parquet"

    elif format == "excel":
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine="openpyxl")
        file_data = buffer.getvalue()
        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        extension = "xlsx"

    elif format == "sql":
        # Generate SQL INSERT statements
        table_name = filename.replace("-", "_").replace(" ", "_")
        sql_lines = [f"-- Generated SQL for {table_name}"]
        sql_lines.append(f"-- Exported at {datetime.now().isoformat()}")
        sql_lines.append("")

        # Create table statement
        columns_def = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype == "int64":
                sql_type = "INTEGER"
            elif dtype == "float64":
                sql_type = "DECIMAL(18,6)"
            elif dtype == "bool":
                sql_type = "BOOLEAN"
            elif "datetime" in str(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "VARCHAR(255)"
            columns_def.append(f"    {col} {sql_type}")

        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {table_name} (")
        sql_lines.append(",\n".join(columns_def))
        sql_lines.append(");")
        sql_lines.append("")

        # Insert statements
        for _, row in df.iterrows():
            values = []
            for val in row:
                if pd.isna(val):
                    values.append("NULL")
                elif isinstance(val, str):
                    values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, bool):
                    values.append("TRUE" if val else "FALSE")
                else:
                    values.append(f"'{str(val)}'")

            sql_lines.append(f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(values)});")

        file_data = "\n".join(sql_lines)
        mime_type = "text/plain"
        extension = "sql"

    else:
        st.error(f"Unsupported format: {format}")
        return

    # Create download button
    st.download_button(
        label=f"{label} ({extension.upper()})",
        data=file_data,
        file_name=f"{filename}.{extension}",
        mime=mime_type,
        use_container_width=True,
    )


def export_dialog(
    data: Any,
    default_filename: str = "export",
    available_formats: list[ExportFormat] | None = None,
):
    """
    Show export dialog with format selection.

    Args:
        data: Data to export
        default_filename: Default filename
        available_formats: List of available formats
    """
    if available_formats is None:
        available_formats = ["csv", "json", "excel", "parquet", "sql"]

    format_labels = {
        "csv": "CSV (Comma Separated Values)",
        "json": "JSON (JavaScript Object Notation)",
        "excel": "Excel (.xlsx)",
        "parquet": "Parquet (Columnar)",
        "sql": "SQL (INSERT statements)",
    }

    with st.expander("Export Options", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            filename = st.text_input(
                "Filename",
                value=default_filename,
                help="Filename without extension"
            )

        with col2:
            format_choice = st.selectbox(
                "Format",
                options=available_formats,
                format_func=lambda x: format_labels.get(x, x.upper()),
            )

        include_timestamp = st.checkbox("Include timestamp in filename", value=True)

        export_dataframe(
            data=data,
            filename=filename,
            format=format_choice,
            include_timestamp=include_timestamp,
        )


def export_chart(
    fig: Any,
    filename: str = "chart",
    formats: list[str] | None = None,
):
    """
    Export chart to image file.

    Args:
        fig: Plotly figure or matplotlib figure
        filename: Base filename
        formats: Available formats (png, svg, pdf, html)
    """
    if formats is None:
        formats = ["png", "svg", "html"]

    col1, col2 = st.columns([2, 1])

    with col1:
        format_choice = st.selectbox(
            "Chart format",
            options=formats,
            key=f"chart_format_{filename}"
        )

    with col2:
        # Try to export based on figure type
        try:
            if hasattr(fig, "to_image"):  # Plotly
                if format_choice in ["png", "svg", "pdf"]:
                    img_bytes = fig.to_image(format=format_choice)
                    mime = f"image/{format_choice}" if format_choice != "pdf" else "application/pdf"
                    st.download_button(
                        label=f"Download {format_choice.upper()}",
                        data=img_bytes,
                        file_name=f"{filename}.{format_choice}",
                        mime=mime,
                    )
                elif format_choice == "html":
                    html_str = fig.to_html()
                    st.download_button(
                        label="Download HTML",
                        data=html_str,
                        file_name=f"{filename}.html",
                        mime="text/html",
                    )
            elif hasattr(fig, "savefig"):  # Matplotlib
                buffer = BytesIO()
                fig.savefig(buffer, format=format_choice, bbox_inches="tight", dpi=150)
                buffer.seek(0)
                mime = f"image/{format_choice}" if format_choice != "pdf" else "application/pdf"
                st.download_button(
                    label=f"Download {format_choice.upper()}",
                    data=buffer,
                    file_name=f"{filename}.{format_choice}",
                    mime=mime,
                )
        except Exception as e:
            st.error(f"Failed to export chart: {e}")


def export_report(
    title: str,
    sections: list[dict[str, Any]],
    filename: str = "report",
    format: Literal["html", "markdown", "pdf"] = "html",
):
    """
    Export a report with multiple sections.

    Args:
        title: Report title
        sections: List of section dicts with 'title', 'content', and optional 'data'
        filename: Base filename
        format: Report format
    """
    timestamp = datetime.now()

    if format == "html":
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
            color: #333;
        }}
        h1 {{
            color: #1f77b4;
            border-bottom: 2px solid #1f77b4;
            padding-bottom: 0.5rem;
        }}
        h2 {{
            color: #444;
            margin-top: 2rem;
        }}
        .meta {{
            color: #666;
            font-size: 0.875rem;
            margin-bottom: 2rem;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 0.75rem;
            text-align: left;
        }}
        th {{
            background: #f5f5f5;
            font-weight: 600;
        }}
        tr:nth-child(even) {{
            background: #fafafa;
        }}
        .section {{
            margin-bottom: 2rem;
        }}
        pre {{
            background: #f5f5f5;
            padding: 1rem;
            border-radius: 4px;
            overflow-x: auto;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <div class="meta">
        Generated: {timestamp.strftime("%Y-%m-%d %H:%M:%S")} | Automic ETL
    </div>
"""
        for section in sections:
            html_content += f'<div class="section">\n'
            html_content += f'<h2>{section.get("title", "Section")}</h2>\n'

            if "content" in section:
                html_content += f'<p>{section["content"]}</p>\n'

            if "data" in section:
                import pandas as pd
                df = pd.DataFrame(section["data"]) if isinstance(section["data"], list) else section["data"]
                html_content += df.to_html(index=False, classes="data-table")

            if "code" in section:
                html_content += f'<pre>{section["code"]}</pre>\n'

            html_content += '</div>\n'

        html_content += """
</body>
</html>
"""
        st.download_button(
            label="Download HTML Report",
            data=html_content,
            file_name=f"{filename}.html",
            mime="text/html",
            use_container_width=True,
        )

    elif format == "markdown":
        md_content = f"# {title}\n\n"
        md_content += f"*Generated: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}*\n\n---\n\n"

        for section in sections:
            md_content += f"## {section.get('title', 'Section')}\n\n"

            if "content" in section:
                md_content += f"{section['content']}\n\n"

            if "data" in section:
                import pandas as pd
                df = pd.DataFrame(section["data"]) if isinstance(section["data"], list) else section["data"]
                md_content += df.to_markdown(index=False) + "\n\n"

            if "code" in section:
                md_content += f"```\n{section['code']}\n```\n\n"

        st.download_button(
            label="Download Markdown Report",
            data=md_content,
            file_name=f"{filename}.md",
            mime="text/markdown",
            use_container_width=True,
        )


def bulk_export_button(
    items: list[dict[str, Any]],
    filename_prefix: str = "bulk_export",
):
    """
    Button for bulk exporting multiple items.

    Args:
        items: List of items with 'name' and 'data' keys
        filename_prefix: Prefix for the zip filename
    """
    import zipfile

    if not items:
        st.warning("No items to export")
        return

    buffer = BytesIO()

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for item in items:
            name = item.get("name", "data")
            data = item.get("data")

            if data is None:
                continue

            # Convert to CSV
            import pandas as pd
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data

            csv_data = df.to_csv(index=False)
            zf.writestr(f"{name}.csv", csv_data)

    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label=f"Download All ({len(items)} files)",
        data=buffer,
        file_name=f"{filename_prefix}_{timestamp}.zip",
        mime="application/zip",
        use_container_width=True,
    )


def copy_to_clipboard_button(text: str, label: str = "Copy to clipboard"):
    """
    Button to copy text to clipboard.

    Args:
        text: Text to copy
        label: Button label
    """
    import html as html_lib

    escaped_text = html_lib.escape(text).replace("\n", "\\n").replace("'", "\\'")

    button_id = f"copy_btn_{hash(text) % 10000}"

    st.markdown(f"""
    <button id="{button_id}" onclick="
        navigator.clipboard.writeText('{escaped_text}');
        this.textContent = 'Copied!';
        setTimeout(() => this.textContent = '{label}', 2000);
    " style="
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.5rem 1rem;
        cursor: pointer;
        width: 100%;
        font-size: 0.875rem;
    ">
        {label}
    </button>
    """, unsafe_allow_html=True)
