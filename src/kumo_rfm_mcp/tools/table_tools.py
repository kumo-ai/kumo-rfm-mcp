import glob
import logging
from pathlib import Path
from typing import Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm

from kumo_rfm_mcp import SessionManager, TableSource

logger = logging.getLogger('kumo-rfm-mcp.table_tools')


def register_table_tools(mcp: FastMCP):
    """Register all table management tools with the MCP server."""
    @mcp.tool()
    def add_table(path: str, name: str) -> dict[str, Any]:
        """
        Tables are the core entities in the Kumo graph. They are the tables
        that will be used to generate predictions. Tables can be added to the
        graph using this tool, each table only needs to be added once.

        The tables need to be linked to each other using either ``infer_links``
        or ``add_link`` tools before finalizing the graph.

        This tool loads a ``*.csv`` or ``*.parquet`` file path and adds it to
        the Kumo graph.

        Args:
            path: File path to the data source (e.g., ``'data/users.csv'``)
            name: The name of the table in the graph (e.g., ``'users'``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Table with name 'users' added successfully"
            }
        """
        logger.info(f"Adding table '{name}' from path '{path}'")

        try:
            logger.info(f"Loading data from '{path}'")
            df = load_dataframe(path)
            logger.info(
                f"Loaded {len(df)} rows and {len(df.columns)} columns from "
                f"'{path}'")
        except Exception as e:
            logger.error(f"Failed to load data from '{path}': {e}")
            return dict(
                success=False,
                message=(f"Could not load data source from '{path}'. {e}"),
            )

        try:
            session = SessionManager.get_default_session()
            logger.info(f"Creating LocalTable '{name}' with {len(df)} rows")
            table = rfm.LocalTable(df, name).infer_metadata(verbose=False)
            session.graph.add_table(table)
            # Store source path in session catalog for metadata inspection
            if hasattr(session, 'catalog'):
                from kumo_rfm_mcp.data_models import TableSource
                session.catalog[name] = TableSource(path=path)
            logger.info(f"Successfully added table '{name}' to graph")
            return dict(
                success=True,
                message=f"Table with name '{name}' added successfully",
            )
        except Exception as e:
            logger.error(f"Failed to register table '{name}': {e}")
            return dict(
                success=False,
                message=f"Failed to register table with name '{name}'. {e}",
            )

    @mcp.tool()
    def remove_table(name: str) -> dict[str, Any]:
        """
        Tables are the core entities in the Kumo graph. They are the tables
        that will be used to generate predictions. Tables can be removed from
        the graph using this tool, each table only needs to be removed once.

        This tool removes an existing table from the Kumo graph.

        Args:
            name: The name of the table to remove (e.g., ``'users'``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Table with name 'users' removed successfully"
            }
        """
        try:
            logger.info(f"Removing table '{name}' from graph")
            session = SessionManager.get_default_session()
            session.graph.remove_table(name)
            # Remove from session catalog if present
            if hasattr(session, 'catalog') and name in session.catalog:
                try:
                    del session.catalog[name]
                except Exception:
                    pass
            logger.info(f"Successfully removed table '{name}' from graph")
            return dict(
                success=True,
                message=f"Table with name '{name}' removed successfully",
            )
        except Exception as e:
            logger.error(f"Failed to remove table '{name}': {e}")
            return dict(
                success=False,
                message=f"Failed to remove table with name '{name}'. {e}",
            )

    @mcp.tool(tags=['source'])
    def discover_table_files(
        root_dir: str,
        recursive: bool = False,
    ) -> list[TableSource]:
        """Discover all table-like files (e.g., CSV, Parquet) in a directory.

        Args:
            root_dir: Root directory to scan.
            recursive: Whether to scan subdirectories recursively.

        Returns:
            List of table source objects for each discovered file.

        Raises:
            ToolError: If the directory does not exist.
        """
        path = Path(root_dir)

        if not path.exists() or not path.is_dir():
            raise ToolError(f"Directory '{root_dir}' does not exist")

        pattern = "**/*" if recursive else "*"
        suffixes = {'.csv', '.parquet'}
        files = [f for f in path.glob(pattern) if f.suffix.lower() in suffixes]

        return [
            TableSource(path=str(f), bytes=f.stat().st_size)
            for f in sorted(files)
        ]

    @mcp.tool(tags=['source'])
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
                raise ToolError(f"Could not read file {path}: {e}") from e
        elif path.lower().endswith('.parquet'):
            try:
                # TODO Read first row groups via `pyarrow` instead.
                df = pd.read_parquet(path).head(num_rows)
            except Exception as e:
                raise ToolError(f"Could not read file '{path}': {e}") from e
        else:
            raise ToolError(f"File '{path}' is not a valid CSV/Parquet file")

        return df.to_dict(orient='records')
