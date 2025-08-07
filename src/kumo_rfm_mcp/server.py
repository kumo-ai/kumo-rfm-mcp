#!/usr/bin/env python3
import logging
from typing import Any, Dict

import pandas as pd
from fastmcp import FastMCP
from kumoai.experimental import rfm

import kumo_rfm_mcp
from kumo_rfm_mcp import SessionManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kumo-rfm-mcp')

mcp = FastMCP(
    name='KumoRFM',
    version=kumo_rfm_mcp.__version__,
)


@mcp.tool()
def add_table(path: str, name: str) -> Dict[str, Any]:
    """Loads a ``*.csv`` or ``*.parquet`` file path and adds it to the
    Kumo graph.

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
    if not path.endswith('.csv') and not path.endswith('.parquet'):
        return dict(
            success=False,
            message=(f"Can not read file from path '{path}'. Only '*.csv' or "
                     f"'*.parquet' files are supported"),
        )

    try:
        if path.endswith('.csv'):
            df = pd.read_csv(path)
        else:
            df = pd.read_parquet(path)
    except Exception as e:
        return dict(
            success=False,
            message=(f"Could not load data source from '{path}'. {e}"),
        )

    try:
        session = SessionManager.get_default_session()
        table = rfm.LocalTable(df, name).infer_metadata(verbose=False)
        session.graph.add_table(table)
        return dict(
            success=True,
            message=f"Table with name '{name}' added successfully",
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to register table with name '{name}'. {e}",
        )


@mcp.tool()
def inspect_table(name: str, num_rows: int = 5) -> Dict[str, Any]:
    """Inspects a table in the Kumo graph.

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
                    {"user_id": 1, "dob": "1990-06-02", "gender": "male"},
                    {"user_id": 2, "dob": "1989-08-25", "gender": "female"},
                    {"user_id": 3, "dob": "1987-01-17", "gender": "male"}
                ]
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        table = session.graph[name]
        return dict(
            success=True,
            message=f"Table with name '{name}' inspected successfully",
            data=dict(
                name=name,
                num_rows=len(table._data),
                num_columns=len(table._data.columns),
                primary_key=table._primary_key,
                time_column=table._time_column,
                rows=table._data.iloc[:num_rows].to_dict(orient='records'),
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to inspect table with name '{name}'. {e}",
        )


@mcp.tool()
def list_tables() -> Dict[str, Any]:
    """Lists all tables in the Kumo graph.

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


if __name__ == '__main__':
    logger.info("Starting KumoRFM MCP Server...")
    mcp.run(transport='stdio')
