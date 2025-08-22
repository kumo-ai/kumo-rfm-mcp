import logging
from typing import Any, Dict

import kumoai.experimental.rfm as rfm
from fastmcp import FastMCP

from kumo_rfm_mcp import SessionManager
from kumo_rfm_mcp.data_models import (GraphMetadata, LinkMetadata,
                                      TableMetadata, TableSource)
from kumo_rfm_mcp.utils import load_dataframe

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

    @mcp.tool()
    def update_metadata(metadata: GraphMetadata) -> Dict[str, Any]:
        """Update the metadata for the current session.

        Args:
            metadata: The new metadata to set for the session.

        Returns:
            Dict[str, Any]: The updated metadata that was set.
        """
        try:
            session = SessionManager.get_default_session()

            # Replace mode: clear current graph/model/catalog
            # TODO(@BlazStojanovic): add a merge mode
            session.graph = rfm.LocalGraph(tables=[])
            session.model = None
            if hasattr(session, 'catalog'):
                session.catalog.clear()

            tables_added = 0
            links_added = 0

            # Build tables
            for tbl in metadata.tables:
                # Load DataFrame from source path (validates supported formats)
                try:
                    df = load_dataframe(tbl.path)
                except Exception as e:
                    return dict(
                        success=False,
                        message=(f"Failed to load table '{tbl.name}' from "
                                 f"'{tbl.path}'. {e}"),
                    )

                try:
                    table = rfm.LocalTable(
                        df=df, name=tbl.name).infer_metadata(verbose=False)

                    # Validate specified columns exist
                    specified_columns = set(tbl.stypes.keys())
                    missing = [
                        c for c in specified_columns
                        if c not in table._data.columns
                    ]
                    if missing:
                        return dict(
                            success=False,
                            message=(
                                f"Table '{tbl.name}': specified stypes for "
                                f"missing columns {missing}. Available "
                                f"columns: {list(table._data.columns)}"),
                        )

                    # Keep only specified columns
                    for col in list(table._data.columns):
                        if col not in specified_columns and col in table:
                            table.remove_column(col)

                    for col_name, stype in tbl.stypes.items():
                        table[col_name].stype = stype

                    # Set primary key and time column
                    if tbl.primary_key:
                        table.primary_key = tbl.primary_key
                    if tbl.time_column:
                        table.time_column = tbl.time_column

                    session.graph.add_table(table)
                    tables_added += 1

                    # Update catalog
                    if hasattr(session, 'catalog'):
                        session.catalog[table.name] = TableSource(
                            path=tbl.path)
                except Exception as e:
                    return dict(
                        success=False,
                        message=f"Failed to register table '{tbl.name}'. {e}",
                    )

            # Build links (validates like link_tables)
            try:
                for link in metadata.links:
                    session.graph.link(link.source_table, link.foreign_key,
                                       link.destination_table)
                    links_added += 1
            except Exception as e:
                return dict(
                    success=False,
                    message=f"Failed to create links. {e}",
                )

            return dict(
                success=True,
                message="Metadata applied (replace mode)",
                data=dict(tables_added=tables_added, links_added=links_added),
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to update metadata. {e}",
            )
