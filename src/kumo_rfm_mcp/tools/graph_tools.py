import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.experimental.rfm.utils import to_dtype
from kumoapi.typing import Dtype, Stype

from kumo_rfm_mcp import (
    GraphMetadata,
    LinkMetadata,
    SessionManager,
    TableMetadata,
    UpdateGraphMetadata,
)

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


def get_graph_metadata() -> GraphMetadata:
    session = SessionManager.get_default_session()

    tables: list[TableMetadata] = []
    for table in session.graph.tables.values():
        dtypes: dict[str, Dtype] = {}
        stypes: dict[str, Stype | None] = {}
        for column in table._data.columns:
            if column in table:
                dtypes[column] = table[column].dtype
                stypes[column] = table[column].stype
            else:
                dtypes[column] = to_dtype(table._data[column])
                stypes[column] = None
        tables.append(
            TableMetadata(
                path=table._path,  # type: ignore
                name=table.name,
                num_rows=len(table._data),
                dtypes=dtypes,
                stypes=stypes,
                primary_key=table._primary_key,
                time_column=table._time_column,
            ))

    links: list[LinkMetadata] = []
    for edge in session.graph.edges:
        links.append(
            LinkMetadata(
                source_table=edge.src_table,
                foreign_key=edge.fkey,
                destination_table=edge.dst_table,
            ))

    return GraphMetadata(tables, links)


def register_graph_tools(mcp: FastMCP):
    """Register all graph management tools with the MCP server."""
    @mcp.tool(tags=['graph'])
    def inspect_graph_metadata() -> GraphMetadata:
        """Inspect the current graph metadata.

        Confirming that the metadata is set up correctly is crucial for the RFM
        model to work properly. In particular,

        * primary keys and time columns need to be correctly specified for each
          table in case they exist
        * columns need to point to a valid semantic type that describe their
          semantic meaning, or ``None`` if they have been discarded
        * links need to point to valid foreign key-primary key relationships

        Returns:
            The graph metadata.
        """
        return get_graph_metadata()

    @mcp.tool()
    def update_graph_metadata(update: UpdateGraphMetadata) -> GraphMetadata:
        """Partially update the current graph metadata.

        Setting up the metadata is crucial for the RFM model to work properly.
        This tool allows you to update the metadata for the current graph.
        In particular,

        * primary keys and time columns need to be correctly specified for each
          table in case they exist
        * columns need to point to a valid semantic type that describe their
          semantic meaning, or ``None`` if they should be discarded
        * links need to point to valid foreign key-primary key relationships

        Omitted fields will be untouched.

        Args:
            update: The metadata updates to perform.

        Returns:
            The graph metadata.

        Raises:
            ToolError: If table or column names do not exist.
            ToolError: If semantic types are invalid for a column's data type.
        """
        # Only keep specified keys:
        update_dict = update.model_dump(exclude_unset=True)

        session = SessionManager.get_default_session()
        session._model = None  # Need to reset the model if graph changes.
        graph = session.graph

        tables_to_update = update_dict.get('tables_to_update', {})
        for table_name, table_update in tables_to_update.items():
            try:
                stypes = table_update.get('stypes', {})
                for column_name, stype in stypes.items():
                    if column_name not in graph[table_name]:
                        graph[table_name].add_column(column_name)
                    if stype is None:
                        del graph[table_name][column_name]
                    else:
                        graph[table_name][column_name].stype = stype
                if 'primary_key' in table_update:
                    graph[table_name].primary_key = table_update['primary_key']
                if 'time_column' in table_update:
                    graph[table_name].time_column = table_update['time_column']
            except Exception as e:
                raise ToolError(str(e)) from e

        for link in update_dict.get('links_to_remove', []):
            try:
                graph.unlink(
                    link['source_table'],
                    link['foreign_key'],
                    link['destination_table'],
                )
            except Exception as e:
                raise ToolError(str(e)) from e

        for link in update_dict.get('links_to_add', []):
            try:
                graph.link(
                    link['source_table'],
                    link['foreign_key'],
                    link['destination_table'],
                )
            except Exception as e:
                raise ToolError(str(e)) from e

        return get_graph_metadata()

    @mcp.tool(tags=['graph'])
    async def suggest_links() -> dict[str, Any]:
        """
        This tool suggests links between tables based on the current metadata
        state.
        """
        raise NotImplementedError("Link inference is not yet implemented")

    @mcp.tool(tags=['graph'])
    async def visualize_graph() -> dict[str, Any]:
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

            return dict(
                success=True,
                message="Graph visualization generated successfully", data={
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
