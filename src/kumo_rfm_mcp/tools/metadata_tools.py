import logging
from typing import Any, Dict

from fastmcp import FastMCP

from kumo_rfm_mcp import SessionManager
from kumo_rfm_mcp.data_models import GraphMetadata, LinkMetadata, TableMetadata

logger = logging.getLogger('kumo-rfm-mcp.metadata_tools')


def register_metadata_tools(mcp: FastMCP):
    """Register all metadata management tools with the MCP server."""

    @mcp.tool()
    def inspect_metadata() -> Dict[str, Any]:
        """Inspect the current metadata from the session.

        Returns:
            Dict[str, Any]: The current metadata containing tables and links
            information.
        """
        try:
            session = SessionManager.get_default_session()

            # Extract table metadata
            tables: list[TableMetadata] = []
            for table in session.graph.tables.values():
                # Collect column stypes from the table metadata
                stypes: dict[str, str] = {}
                for column in table.columns:
                    stypes[column.name] = str(column.stype)

                # Pull path from the session catalog if available
                path = ''
                if hasattr(session,
                           'catalog') and table.name in session.catalog:
                    try:
                        path = session.catalog[table.name].path
                    except Exception:
                        path = ''

                tables.append(
                    TableMetadata(
                        name=table.name,
                        path=path,
                        primary_key=table._primary_key
                        if table._primary_key else "",
                        time_column=table._time_column
                        if table._time_column else None,
                        stypes=stypes,
                    ))

            # Extract link metadata
            links: list[LinkMetadata] = []
            for edge in session.graph.edges:
                links.append(
                    LinkMetadata(
                        source_table=edge.src_table,
                        foreign_key=edge.fkey,
                        destination_table=edge.dst_table,
                    ))

            metadata = GraphMetadata(tables=tables, links=links)

            return {
                "success": True,
                "message": "Metadata retrieved successfully",
                "data": metadata.model_dump(),
            }

        except Exception as e:
            logger.error(f"Failed to inspect metadata: {e}")
            return {
                "success": False,
                "message": f"Failed to inspect metadata: {e}"
            }
