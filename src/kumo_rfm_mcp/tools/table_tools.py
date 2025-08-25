import glob
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from fastmcp import FastMCP, ToolError
from kumoai.experimental import rfm

from kumo_rfm_mcp import SessionManager, TableSource

logger = logging.getLogger('kumo-rfm-mcp.table_tools')


def register_table_tools(mcp: FastMCP):
    """Register all table management tools with the MCP server."""
    @mcp.tool()
    def add_table(path: str, name: str) -> Dict[str, Any]:
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
    def remove_table(name: str) -> Dict[str, Any]:
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

    @mcp.tool()
    def inspect_table(name: str, num_rows: int = 20) -> Dict[str, Any]:
        """
        Tables are core entities in the Kumo graph. They are the tables that
        will be used to generate predictions. This tool inspects a table in
        the Kumo graph. Use it to get information about the table, such as the
        number of rows, columns, primary key, and time column.

        Primary key, time column, and column names are particularly important
        for defining the predictive queries.

        Args:
            name: The name of the table in the graph (e.g., ``'users'``)
            num_rows: The number of rows to inspect.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
            {
                "success": true,
                "message": "Table with name 'users' inspected successfully",
                "data": {
                    "name": "users",
                    "num_rows": 18431,
                    "num_columns": 3,
                    "primary_key": "user_id",
                    "time_column": "dob",
                    "rows": [
                        {"user_id": 1, "dob": "1990-06-02",
                        "gender": "male"},
                        {"user_id": 2, "dob": "1989-08-25",
                        "gender": "female"},
                        {"user_id": 3, "dob": "1987-01-17",
                        "gender": "male"}
                        ...
                    ]
                }
            }
        """
        try:
            logger.info(f"Inspecting table '{name}' (showing {num_rows} rows)")
            session = SessionManager.get_default_session()
            table = session.graph[name]

            table_info = dict(
                name=name,
                num_rows=len(table._data),
                num_columns=len(table._data.columns),
                primary_key=table._primary_key,
                time_column=table._time_column,
                rows=table._data.iloc[:num_rows].to_dict(orient='records'),
            )
            logger.info(f"Table '{name}' has {table_info['num_rows']} rows, "
                        f"{table_info['num_columns']} columns")

            return dict(
                success=True,
                message=f"Table with name '{name}' inspected successfully",
                data=table_info,
            )
        except Exception as e:
            logger.error(f"Failed to inspect table '{name}': {e}")
            return dict(
                success=False,
                message=f"Failed to inspect table with name '{name}'. {e}",
            )

    @mcp.tool()
    def list_tables() -> Dict[str, Any]:
        """Lists all tables in the Kumo graph. Use this tool to check if the
        graph contains all the tables that you want to use to generate
        predictions.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
            {
                "success": true,
                "message": "Listed 2 tables successfully",
                "data": {
                    "users": {
                        "num_rows": 18431,
                        "columns": ["user_id", "dob", "gender"],
                        "primary_key": "user_id",
                        "time_column": "dob",
                    },
                    "items": {
                        "num_rows": 3654,
                        "columns": ["item_id", "category", "description"],
                        "primary_key": "item_id",
                        "time_column": None,
                    },
                }
            }
        """
        try:
            session = SessionManager.get_default_session()
            data = {
                table.name:
                dict(
                    num_rows=len(table._data),
                    columns=list(table._data.columns),
                    primary_key=table._primary_key,
                    time_column=table._time_column,
                )
                for table in session.graph.tables.values()
            }
            return dict(
                success=True,
                message=f"Listed {len(data)} tables successfully",
                data=data,
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to list tables. {e}",
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
