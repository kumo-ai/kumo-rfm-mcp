from pathlib import Path
from typing import Any

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from kumo_rfm_mcp import TableSource


def discover_table_files(
    root_dir: str,
    recursive: bool = False,
) -> list[TableSource]:
    """Discover all table-like files (e.g., CSV, Parquet) in a directory.

    Args:
        root_dir: Root directory to scan.
        recursive: Whether to scan subdirectories recursively. Use with caution
            in large directories such as home folders or system directories.

    Returns:
        List of table source objects for each discovered file.

    Raises:
        ToolError: If the directory does not exist.
    """
    path = Path(root_dir).expanduser()

    if not path.exists() or not path.is_dir():
        raise ToolError(f"Directory '{root_dir}' does not exist")

    pattern = "**/*" if recursive else "*"
    suffixes = {'.csv', '.parquet'}
    files = [f for f in path.glob(pattern) if f.suffix.lower() in suffixes]

    return [
        TableSource(path=str(f), bytes=f.stat().st_size) for f in sorted(files)
    ]


def inspect_table_file(
    path: str,
    num_rows: int = 20,
) -> list[dict[str, Any]]:
    """Inspect the first rows of a table-like file.

    Each row in the file is represented as a dictionary mapping column
    names to their corresponding values.

    Args:
        path: File path to inspect.
        num_rows: Number of rows to read.

    Returns:
        Each dictionary corresponds to one row in the table.

    Raises:
        ToolError: If more than 1,000 rows are requested.
        ToolError: If the file cannot be read.
        ToolError: If the file is not a CSV/Parquet file.
    """
    if num_rows > 1000:
        raise ToolError("Cannot return more than 1,000 rows")

    if path.lower().endswith('.csv'):
        try:
            df = pd.read_csv(path, nrows=num_rows)
        except Exception as e:
            raise ToolError(f"Could not read file '{path}': {e}") from e
    elif path.lower().endswith('.parquet'):
        try:
            # TODO Read first row groups via `pyarrow` instead.
            df = pd.read_parquet(path).head(num_rows)
        except Exception as e:
            raise ToolError(f"Could not read file '{path}': {e}") from e
    else:
        raise ToolError(f"File '{path}' is not a valid CSV or Parquet "
                        f"file")

    return df.to_dict(orient='records')


def register_table_tools(mcp: FastMCP) -> None:
    """Register all table management tools to the MCP server."""
    mcp.tool()(discover_table_files)
    mcp.tool()(inspect_table_file)
