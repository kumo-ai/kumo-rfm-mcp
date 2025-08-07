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

### Tools ###

# Table Management
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
def remove_table(name: str) -> Dict[str, Any]:
    """Removes an existing table from the Kumo graph.

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
        session = SessionManager.get_default_session()
        session.graph.remove_table(name)
        return dict(
            success=True,
            message=f"Table with name '{name}' removed successfully",
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to remove table with name '{name}'. {e}",
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

# Graph Management
@mcp.tool()
async def kumo_inspect_graph() -> dict:
    """
    View the complete graph structure including all tables and their relationships.
    
    This operation provides a comprehensive view of the current graph state,
    including all tables, their schemas, and the links between them.
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Graph structure retrieved successfully",
            "data": {
                "tables": {
                    "users": {
                        "num_rows": 18431,
                        "columns": ["user_id", "dob", "gender"],
                        "primary_key": "user_id",
                        "time_column": "dob"
                    }
                },
                "links": [
                    {
                        "source": "users",
                        "target": "orders",
                        "source_key": "user_id",
                        "target_key": "customer_id"
                    }
                ]
            }
        }
    """
    # TODO: Implement kumo_inspect_graph
    return dict(
        success=False,
        message="kumo_inspect_graph is not yet implemented",
    )

@mcp.tool()
async def kumo_add_table_link(
    source_table: str,
    target_table: str,
    source_key: str,
    target_key: str
) -> dict:
    """
    Create a link (edge) between two tables in the graph.
    
    Args:
        source_table: Name of the source table
        target_table: Name of the target table
        source_key: Column name in source table for the relationship
        target_key: Column name in target table for the relationship
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Link created successfully between users and orders",
            "data": {
                "source_table": "users",
                "target_table": "orders",
                "source_key": "user_id", 
                "target_key": "customer_id"
            }
        }
    """
    # TODO: Implement kumo_add_table_link
    return dict(
        success=False,
        message="kumo_add_table_link is not yet implemented",
    )

@mcp.tool()
async def kumo_remove_table_link(
    source_table: str,
    target_table: str
) -> dict:
    """
    Remove a link (edge) between two tables in the graph.
    
    Args:
        source_table: Name of the source table
        target_table: Name of the target table
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Link removed successfully between users and orders",
            "data": {
                "source_table": "users",
                "target_table": "orders"
            }
        }
    """
    # TODO: Implement kumo_remove_table_link
    return dict(
        success=False,
        message="kumo_remove_table_link is not yet implemented",
    )

@mcp.tool()
async def kumo_finalize_graph() -> dict:
    """
    Finalize the graph and create a KumoRFM model instance.
    
    This operation creates a KumoRFM model from the current graph state,
    making it available for inference operations.
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Graph finalized and KumoRFM model created successfully",
            "data": {
                "model_id": "rfm_model_20241210",
                "num_tables": 3,
                "num_links": 2
            }
        }
    """
    # TODO: Implement kumo_finalize_graph
    return dict(
        success=False,
        message="kumo_finalize_graph is not yet implemented",
    )

@mcp.tool()
async def kumo_infer_links() -> dict:
    """
    Automatically infer potential links between tables in the graph.
    
    This operation analyzes the schema and data of existing tables to
    suggest possible relationships that could be added as links.
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Found 3 potential links",
            "data": {
                "suggested_links": [
                    {
                        "source": "users",
                        "target": "orders", 
                        "source_key": "user_id",
                        "target_key": "customer_id",
                        "confidence": 0.95
                    }
                ]
            }
        }
    """
    # TODO: Implement kumo_infer_links
    return dict(
        success=False,
        message="kumo_infer_links is not yet implemented",
    )






if __name__ == '__main__':
    logger.info("Starting KumoRFM MCP Server...")
    mcp.run(transport='stdio')
