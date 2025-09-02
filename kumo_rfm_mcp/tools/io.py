import asyncio
import os.path as osp
from pathlib import Path
from typing import Annotated

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from kumo_rfm_mcp import TableSource, TableSourcePreview


async def find_table_files(
    path: Annotated[Path, "Local root directory to scan"],
    recursive: Annotated[
        bool,
        Field(
            default=False,
            description=("Whether to scan subdirectories recursively. Use "
                         "with caution in large directories such as home "
                         "folders or system directories."),
        ),
    ],
) -> list[TableSource]:
    """Finds all table-like files (e.g., CSV, Parquet) in a directory.

    This tool is for local directories only. It cannot search, e.g., in S3
    buckets.
    """
    path = path.expanduser()

    if not path.exists() or not path.is_dir():
        raise ToolError(f"Directory '{path}' does not exist")

    def _find_table_files() -> list[TableSource]:
        pattern = "**/*" if recursive else "*"
        suffixes = {'.csv', '.parquet'}
        files = [f for f in path.glob(pattern) if f.suffix.lower() in suffixes]
        return [
            TableSource(path=f, bytes=f.stat().st_size) for f in sorted(files)
        ]

    return await asyncio.to_thread(_find_table_files)


async def inspect_table_files(
    paths: Annotated[
        list[str],
        ("File paths to inspect. Can be a mix of local file paths, S3 URIs "
         "(s3://...), or HTTP/HTTPS URLs."),
    ],
    num_rows: Annotated[
        int,
        Field(
            default=20,
            ge=1,
            le=1000,
            description="Number of rows to read per file",
        ),
    ],
) -> dict[str, TableSourcePreview]:
    """Inspect the first rows of table-like files.

    Each row in a file is represented as a dictionary mapping column
    names to their corresponding values.
    """
    def read_file(path: str) -> TableSourcePreview:
        path = osp.expanduser(path)
        suffix = path.rsplit('.', maxsplit=1)[-1].lower()

        if suffix not in {'csv', 'parquet'}:
            raise ToolError(f"'{path}' is not a valid CSV or Parquet file")

        try:
            if suffix == 'csv':
                df = pd.read_csv(path, nrows=num_rows)
            else:
                assert suffix == 'parquet'
                # TODO Read first row groups via `pyarrow` instead.
                df = pd.read_parquet(path).head(num_rows)
        except Exception as e:
            raise ToolError(f"Could not read file '{path}': {e}") from e

        df = df.astype(object).where(df.notna(), None)
        return TableSourcePreview(rows=df.to_dict(orient='records'))

    tasks = [asyncio.to_thread(read_file, path) for path in paths]
    previews = await asyncio.gather(*tasks)
    return {path: preview for path, preview in zip(paths, previews)}


def register_io_tools(mcp: FastMCP) -> None:
    """Register all I/O tools to the MCP server."""
    mcp.tool(annotations=dict(
        title="🔍 Searching for tabular files…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(find_table_files)

    mcp.tool(annotations=dict(
        title="🧐 Analyzing table structure…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(inspect_table_files)
