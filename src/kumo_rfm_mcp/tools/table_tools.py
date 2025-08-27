from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from kumo_rfm_mcp import TableSource


def discover_table_files(
    path: Annotated[Path, "Root directory to scan"],
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
    """Discover all table-like files (e.g., CSV, Parquet) in a directory."""
    path = path.expanduser()

    if not path.exists() or not path.is_dir():
        raise ToolError(f"Directory '{path}' does not exist")

    pattern = "**/*" if recursive else "*"
    suffixes = {'.csv', '.parquet'}
    files = [f for f in path.glob(pattern) if f.suffix.lower() in suffixes]

    return [
        TableSource(path=str(f), bytes=f.stat().st_size) for f in sorted(files)
    ]


def inspect_table_file(
    path: Annotated[Path, "File path to inspect"],
    num_rows: Annotated[
        int,
        Field(
            default=20,
            ge=1,
            le=1000,
            description="Number of rows to read",
        ),
    ],
) -> list[dict[str, Any]]:
    """Inspect the first rows of a table-like file.

    Each row in the file is represented as a dictionary mapping column
    names to their corresponding values.
    """
    if path.suffix.lower() == '.csv':
        try:
            df = pd.read_csv(path, nrows=num_rows)
        except Exception as e:
            raise ToolError(f"Could not read file '{path}': {e}") from e
    elif path.suffix.lower() == '.parquet':
        try:
            # TODO Read first row groups via `pyarrow` instead.
            df = pd.read_parquet(path).head(num_rows)
        except Exception as e:
            raise ToolError(f"Could not read file '{path}': {e}") from e
    else:
        raise ToolError(f"'{path}' is not a valid CSV or Parquet file")

    return df.to_dict(orient='records')


def register_table_tools(mcp: FastMCP) -> None:
    """Register all table management tools to the MCP server."""
    mcp.tool()(discover_table_files)
    mcp.tool()(inspect_table_file)
