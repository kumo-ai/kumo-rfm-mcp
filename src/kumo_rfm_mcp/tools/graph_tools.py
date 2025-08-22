import logging
from typing import Any, Dict

from fastmcp import FastMCP

from kumo_rfm_mcp import SessionManager
from kumo_rfm_mcp.data_models import LinkMetadata, TableMetadata
from kumo_rfm_mcp.utils import extract_link_metadata, extract_table_metadata

logger = logging.getLogger('kumo-rfm-mcp.graph_tools')


def _generate_mermaid_diagram(tables: list[TableMetadata],
                              links: list[LinkMetadata]) -> str:
    """Generate a Mermaid entity relationship diagram from table
    and link metadata.

    Each table shows columns with their semantic types and special metadata:
    - PK: Primary key column
    - time_col: Time column for temporal data

    Args:
        tables: List of table metadata objects
        links: List of link metadata objects

    Returns:
        Mermaid diagram as a string
    """
    lines = ["erDiagram"]

    # Add table definitions with detailed column information
    for table in tables:
        # Get column names from stypes
        columns = list(table.stypes.keys()) if table.stypes else []

        # Create table definition
        lines.append(f"    {table.name} {{")

        if not columns:
            # Handle empty tables
            lines.append("        string no_columns \"(empty table)\"")
        else:
            # Add each column with its stype and metadata
            for column in columns:
                column_type = table.stypes.get(column, "unknown")

                # Determine metadata for this column
                metadata_parts = []
                if column == table.primary_key:
                    metadata_parts.append("PK")
                if column == table.time_column:
                    metadata_parts.append("time_col")

                # Format the column entry: column_name stype "metadata"
                if metadata_parts:
                    metadata_str = f" \"{', '.join(metadata_parts)}\""
                    lines.append(
                        f"        {column} {column_type}{metadata_str}")
                else:
                    lines.append(f"        {column} {column_type}")

        lines.append("    }")

    # Add relationships/foreign keys
    for link in links:
        # Use ||--o{ to represent one-to-many relationship (FK relationship)
        relationship = (
            f"    {link.destination_table} ||--o{{ {link.source_table} : "
            f"\"{link.foreign_key}\"")
        lines.append(relationship)

    return "\n".join(lines)


def register_graph_tools(mcp: FastMCP):
    """Register all graph management tools with the MCP server."""

    @mcp.tool()
    async def suggest_links() -> Dict[str, Any]:
        """
        This tool suggests links between tables based on the current metadata
        state.
        """
        try:
            raise NotImplementedError("Link inference is not yet implemented")
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to suggest links. {e}",
            )

    @mcp.tool()
    async def visualize_graph() -> Dict[str, Any]:
        """Visualizes the graph as a mermaid entity relationship diagram.

        This tool generates a Mermaid diagram showing:
        - Tables with each column's name, semantic type (stype), and metadata
        - Primary key columns marked with "PK"
        - Time columns marked with "time_col"
        - Foreign key relationships between tables

        Returns:
            Dictionary containing:
            - success (bool): True if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Contains the mermaid diagram string

        Examples:
            {
                "success": true,
                "message": "Graph visualization generated successfully",
                "data": {
                    "mermaid_diagram": "erDiagram\\n
                    users {...}\\n    orders {...}\\n    ..."
                }
            }
        """
        try:
            session = SessionManager.get_default_session()

            # Extract table and link metadata using utility functions
            tables = extract_table_metadata(session)
            links = extract_link_metadata(session)

            # Generate Mermaid diagram
            mermaid_diagram = _generate_mermaid_diagram(tables, links)

            logger.info(f"Generated Mermaid diagram with {len(tables)} tables "
                        f"and {len(links)} links")

            return dict(success=True,
                        message="Graph visualization generated successfully",
                        data={
                            "mermaid_diagram": mermaid_diagram,
                            "num_tables": len(tables),
                            "num_links": len(links)
                        })
        except Exception as e:
            logger.error(f"Failed to visualize graph: {e}")
            return dict(
                success=False,
                message=f"Failed to visualize graph. {e}",
            )
