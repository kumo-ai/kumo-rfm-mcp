#!/usr/bin/env python3
import logging
import sys
from typing import Any, Dict

import pandas as pd
from fastmcp import FastMCP
from kumoai.experimental import rfm

import kumo_rfm_mcp
from kumo_rfm_mcp import SessionManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr  # Ensure logs go to stderr so they're visible
)
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

@mcp.tool()
async def infer_links() -> dict:
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
            "message": "Link inference completed",
            "data": {
                "num_links": 1,
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        graph = session.graph
        
        # Run automatic link inference
        graph.infer_links(verbose=False)
        
        return dict(
            success=True,
            message="Link inference completed",
            data=dict(
                num_links=len(graph.edges),
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to infer links. {e}",
        )


# Graph Management
@mcp.tool()
async def inspect_graph() -> dict:
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
                    },
                    "orders": {
                        "num_rows": 998998,
                        "columns": ["order_id", "user_id", "order_date"],
                        "primary_key": "order_id",
                        "time_column": "order_date"
                    }
                },
                "links": [
                    {
                        "source": "orders",
                        "foreign_key": "user_id", 
                        "destination": "users",
                    }
                ]
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        graph = session.graph
        
        # Get tables info
        tables = {
            table.name: dict(
                num_rows=len(table._data),
                columns=list(table._data.columns),
                primary_key=table._primary_key,
                time_column=table._time_column,
            )
            for table in graph.tables.values()
        }
        
        # Get edges info
        links = []
        for edge in graph.edges:
            links.append({
                "source": edge.src_table,
                "foreign_key": edge.fkey,
                "destination": edge.dst_table,
            })
        
        return dict(
            success=True,
            message="Graph structure retrieved successfully",
            data=dict(
                tables=tables,
                links=links,
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to inspect graph. {e}",
        )

@mcp.tool()
async def link_tables(
    source_table: str,
    foreign_key: str,
    target_table: str,
) -> dict:
    """
    Create a link (edge) between two tables in the graph.
    
    Args:
        source_table: Name of the source table
        foreign_key: Column to link with the primary key 
                     of the destination table, names should match
        (e.g., ``'user_id'``)
        target_table: Name of the destination table
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Successfully linked users and orders by user_id",
            "data": {
                "source_table": "users",
                "foreign_key": "user_id", 
                "target_table": "orders",
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        graph = session.graph
        
        # LocalGrpah.link() already performs validations with appropriate error
        # messages
        graph.link(src_table=source_table, fkey=foreign_key, dst_table=target_table)
        
        return dict(
            success=True,
            message=(f"Successfully linked {source_table} and {target_table} "
                     f"by {foreign_key}"),
            data=dict(
                source_table=source_table,
                foreign_key=foreign_key,
                target_table=target_table,
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=(f"Failed to link {source_table} and {target_table} "
                     f"by {foreign_key}. {e}"),
        )

@mcp.tool()
async def unlink_tables(
    source_table: str,
    foreign_key: str,
    target_table: str,
) -> dict:
    """
    Remove a link (edge) between two tables in the graph.
    
    Args:
        source_table: Name of the source table
        foreign_key: Column to link with the primary key 
                     of the destination table, names should match
        (e.g., ``'user_id'``)
        target_table: Name of the destination table
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Successfully unlinked users and orders by user_id",
            "data": {
                "source_table": "users",
                "foreign_key": "user_id", 
                "target_table": "orders",
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        graph = session.graph
        
        # LocalGraph.unlink() already performs validations with appropriate error
        # messages
        graph.unlink(
            src_table=source_table, 
            fkey=foreign_key, 
            dst_table=target_table,
        )
        
        return dict(
            success=True,
            message=(f"Successfully unlinked {source_table} and {target_table}"
                     f" by {foreign_key}"),
            data=dict(
                source_table=source_table,
                foreign_key=foreign_key,
                target_table=target_table,
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=(f"Failed to unlink {source_table} and {target_table} "
                     f"by {foreign_key}. {e}"),
        )

@mcp.tool()
async def finalize_graph() -> dict:
    """
    Finalize the graph and create a KumoRFM model instance.
    
    This operation creates a KumoRFM model from the current graph state,
    making it available for inference operations (e.g., ``'predict'``, and 
    ``'evaluate'``).
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Successfully spun up KumoRFM model!",
        }
    """
    try:
        logger.info("Starting graph finalization...")
        session = SessionManager.get_default_session()
        graph = session.graph
        
        logger.info(f"Graph has {len(graph.tables)} tables and {len(graph.edges)} edges")
        
        # Validate the graph before finalizing
        logger.info("Validating graph...")
        graph.validate()
        logger.info("Graph validation completed successfully")
        
    except Exception as e:
        logger.error(f"Graph validation failed: {e}")
        return dict(
            success=False,
            message=f"Encountered error while validating graph. {e}",
        )

    try:
        # Create the KumoRFM model from the graph
        logger.info("Creating KumoRFM model...")
        model = rfm.KumoRFM(graph, verbose=False)
        logger.info("KumoRFM model created successfully")
        
        session.model = model
        logger.info("Model stored in session")
        
        return dict(
            success=True,
            message="Successfully spun up KumoRFM model!",
        )
    except Exception as e:
        logger.error(f"Failed to create KumoRFM model: {e}")
        return dict(
            success=False,
            message=f"Failed to spin up KumoRFM model. {e}",
        )

# Inference
@mcp.tool()
async def validate_query(query_string: str) -> dict:
    """
    Validate a predictive query string against the current graph structure.
    
    This operation checks if the query syntax is correct and compatible with
    the current graph schema without executing the prediction.
    
    Args:
        query_string: The predictive query to validate (e.g., "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1")
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Query validated successfully",
            "data": {
                "query": "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1",
                "is_valid": true
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        
        if session.model is None:
            return dict(
                success=False,
                message="No KumoRFM model available. Please call finalize_graph first.",
            )
        
        # Use the KumoRFM model's internal _parse_query method to validate
        model = session.model
        parsed_query = model._parse_query(query_string)
        
        return dict(
            success=True,
            message="Query validated successfully",
            data=dict(
                query=query_string,
                is_valid=True,
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Query validation failed: {e}",
            data=dict(
                query=query_string,
                is_valid=False,
            ),
        )

@mcp.tool()
async def predict(query_string: str) -> dict:
    """
    Execute a predictive query and return model predictions.
    
    This operation runs the specified predictive query against the KumoRFM model
    and returns the predictions as tabular data.
    
    Args:
        query_string: The predictive query to execute (e.g., "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1")
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Prediction completed successfully",
            "data": {
                "query": "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1",
                "predictions": [
                    {"user_id": 1, "COUNT(orders.*, 0, 30, days) > 0": 0.85}
                ]
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        
        if session.model is None:
            return dict(
                success=False,
                message="No KumoRFM model available. Please call finalize_graph first.",
            )
        
        logger.info(f"Running prediction for query: {query_string}")
        model = session.model
        
        # Execute the prediction with verbose=False to reduce output
        result_df = model.predict(query_string, verbose=False)
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        predictions = result_df.to_dict(orient='records')
        
        logger.info(f"Prediction completed, returned {len(predictions)} results")
        
        return dict(
            success=True,
            message="Prediction completed successfully",
            data=dict(
                query=query_string,
                predictions=predictions,
            ),
        )
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        return dict(
            success=False,
            message=f"Prediction failed: {e}",
        )

@mcp.tool()
async def evaluate(query_string: str) -> dict:
    """
    Evaluate a predictive query and return performance metrics.
    
    This operation runs the specified predictive query in evaluation mode,
    comparing predictions against known ground truth labels and returning
    performance metrics.
    
    Args:
        query_string: The predictive query to evaluate (e.g., "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1")
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Evaluation completed successfully",
            "data": {
                "query": "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1",
                "metrics": [
                    {"metric": "auroc", "value": 0.87},
                    {"metric": "auprc", "value": 0.82}
                ]
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        
        if session.model is None:
            return dict(
                success=False,
                message="No KumoRFM model available. Please call finalize_graph first.",
            )
        
        logger.info(f"Running evaluation for query: {query_string}")
        model = session.model
        
        # Execute the evaluation with verbose=False to reduce output
        result_df = model.evaluate(query_string, verbose=False)
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        metrics = result_df.to_dict(orient='records')
        
        logger.info(f"Evaluation completed, returned {len(metrics)} metrics")
        
        return dict(
            success=True,
            message="Evaluation completed successfully",
            data=dict(
                query=query_string,
                metrics=metrics,
            ),
        )
    except Exception as e:
        logger.error(f"Evaluation failed: {e}")
        return dict(
            success=False,
            message=f"Evaluation failed: {e}",
        )



# Session Management
@mcp.tool()
async def get_session_status() -> dict:
    """
    Get the current session status including tables, graph state, and KumoRFM model status.
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Session status retrieved successfully",
            "data": {
                "initialized": true,
                "table_names": ["users", "orders", "items"],
                "num_links": 2,
                "is_rfm_model_ready": true
            }
        }
    """
    try:
        session = SessionManager.get_default_session()
        
        table_names = list(session.graph.tables.keys())
        num_links = len(session.graph.edges)
        is_rfm_model_ready = session.model is not None
        
        return dict(
            success=True,
            message="Session status retrieved successfully",
            data=dict(
                initialized=session.initialized,
                table_names=table_names,
                num_links=num_links,
                is_rfm_model_ready=is_rfm_model_ready,
            ),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to get session status. {e}",
        )

@mcp.tool()
async def clear_session() -> dict:
    """
    Clear the current session by removing all tables, links, and the KumoRFM model.
    
    This operation resets the session to its initial state, allowing you to start fresh
    with new data and graph configuration.
    
    Returns:
        Dictionary containing:
        - success (bool): ``True`` if operation succeeded
        - message (str): Human-readable status message
        - data (dict, optional): Additional information on success
    
    Examples:
        {
            "success": true,
            "message": "Session cleared successfully",
        }
    """
    try:
        session = SessionManager.get_default_session()
        
        # Clear the graph by creating a new empty one
        session.graph = rfm.LocalGraph(tables=[])
        
        # Clear the KumoRFM model
        session.model = None
        
        return dict(
            success=True,
            message=("Session cleared successfully, "
                     "ready to start with fresh data!"),
        )
    except Exception as e:
        return dict(
            success=False,
            message=f"Failed to clear session. {e}",
        )




if __name__ == '__main__':
    logger.info("Starting KumoRFM MCP Server...")
    mcp.run(transport='stdio')
