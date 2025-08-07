#!/usr/bin/env python3
"""
Simple KumoRFM MCP Server.
"""

import logging
import pandas as pd
from kumoai.experimental.rfm import LocalTable

from fastmcp import FastMCP
from fastmcp.client.transport import StdioTransport
from kumo_rfm_mcp.session import SessionState

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kumo-rfm-mcp")

# Initialize FastMCP
mcp = FastMCP(
    name="KumoRFM",
    version="0.1.0",
    description="Machine learning on enterprise data with KumoRFM"
)

# Initialize server state
SessionState.initialize()  


@mcp.tool(
    name="create_table",
    description="Register local .csv/.parquet file as a table in the Kumo graph",
)
def kumo_create_table(file_path: str, table_name: str) -> Tuple[bool, str]:
    """Transform local data file into a LocalTable"""
    session = SessionState.get_session()
    
    if not session.initialized:
        return False, "Error: Server is not initialized."
    
    try:
        df = pd.read_csv(file_path) or pd.read_parquet(file_path)

        # Create a LocalTable from the pandas DataFrame
        table = LocalTable(df, name=table_name).infer_metadata()

        # Add the table to the graph
        session.graph.add_table(table)
        
        return True, f"Table {table_name} created successfully."
    except Exception as e:
        logger.error(f"Table {table_name} creation failed: {e}")
        return False, f"Table {table_name} creation failed: {str(e)}"


@mcp.tool(
    name="remove_table",
    description="Remove a table from the Kumo graph",
)
def kumo_remove_table(table_name: str) -> Tuple[bool, str]:
    """Remove a table from the Kumo graph"""
    session = SessionState.get_session()
    
    if not session.initialized:
        return False, "Error: Server is not initialized."
    
    try:
        session.graph.remove_table(table_name)
        return True, f"Table {table_name} removed successfully."
    except Exception as e:
        logger.error(f"Table {table_name} removal failed: {e}")
        return False, f"Table {table_name} removal failed: {str(e)}"


@mcp.tool(
    name="inspect_table",
    description="Inspect a table in the Kumo graph",
)
def kumo_inspect_table(table_name: str, row_limit: int = 3) -> Tuple[bool, str]:
    """Inspect a table in the Kumo graph"""
    session = SessionState.get_session()

    if not session.initialized:
        return False, "Error: Server is not initialized."
    
    try:
        table = session.graph.get_table(table_name)

        # sample
        df = table.sample(row_limit)

        # Return the sampled data
        return True, json.dumps(df.to_dict(orient="records"), indent=2)
    except Exception as e:
        logger.error(f"Table {table_name} inspection failed: {e}")
        return False, f"Table {table_name} inspection failed: {str(e)}"

@mcp.tool(
    name="list_tables",
    description="List all tables in the Kumo graph",
)
def kumo_list_tables() -> Tuple[bool, str]:
    """List all tables in the Kumo graph"""
    session = SessionState.get_session()

    if not session.initialized:
        return False, "Error: Server is not initialized."
    
    try:
        tables = session.graph.tables
        return True, json.dumps(tables, indent=2)
    except Exception as e:
        logger.error(f"Table listing failed: {e}")
        return False, f"Table listing failed: {str(e)}"


def main():
    """Main entry point for the MCP server"""
    logger.info("Starting KumoRFM MCP Server...")
    mcp.run(transport=StdioTransport)

if __name__ == "__main__":
    main()