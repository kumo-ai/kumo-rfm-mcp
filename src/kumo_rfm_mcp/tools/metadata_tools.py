import logging
from typing import Any, Dict

import kumoai.experimental.rfm as rfm
from fastmcp import FastMCP

from kumo_rfm_mcp import SessionManager
from kumo_rfm_mcp.data_models import GraphMetadata, TableSource
from kumo_rfm_mcp.utils import (extract_link_metadata, extract_table_metadata,
                                load_dataframe)

logger = logging.getLogger('kumo-rfm-mcp.metadata_tools')


def register_metadata_tools(mcp: FastMCP):
    """Register all metadata management tools with the MCP server."""

    @mcp.tool()
    def inspect_metadata() -> Dict[str, Any]:
        """Inspect the current metadata from the session.

        Setting up metadata is crucial for the RFM model to work correctly.
        This tool allows you to inspect the current metadata and update it.

        Returns:
            Dict[str, Any]: The current metadata containing tables and links
            information.
        """
        try:
            session = SessionManager.get_default_session()

            # Extract table and link metadata using utility functions
            tables = extract_table_metadata(session)
            links = extract_link_metadata(session)

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

        Setting up metadata is crucial for the RFM model to work correctly.
        This tool allows you to update the metadata for the current session.

        Note: This tool will replace the current metadata with the new one.

        <KUMO_RFM_METADATA>
        The metadata is a JSON object that contains the following fields:
        - tables: A list of table metadata objects.
        - links: A list of link metadata objects.

        Together these objects form a graph which is used to build the RFM
        model.

        </KUMO_RFM_METADATA>

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
