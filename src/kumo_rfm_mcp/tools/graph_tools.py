"""Graph management tools for KumoRFM MCP server."""

import logging
from typing import Any, Dict

from fastmcp import FastMCP

from kumo_rfm_mcp import SessionManager

logger = logging.getLogger('kumo-rfm-mcp')


def register_graph_tools(mcp: FastMCP):
    """Register all graph management tools with the MCP server."""

    @mcp.tool()
    async def infer_links() -> Dict[str, Any]:
        """The graph is a collection of tables and links between them, it is
        the core data structure powering the KumoRFM model. The graph needs to
        be finalized before the KumoRFM model can start generating predictions.

        This tool automatically infers potential links between tables in the
        graph. It matches columns with the same name in different tables and
        adds them as links.

        The inferred links can be inspected using the ``inspect_graph`` tool.
        This tool only works if no links have been added to the graph yet. To
        add links manually, use the ``link_tables`` tool to link two tables via
        a foreign key column.

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
                    "inferred_links": [
                        {
                            "source_table": "orders",
                            "foreign_key": "user_id",
                            "destination_table": "users"
                        },
                        {
                            "source_table": "orders",
                            "foreign_key": "item_id",
                            "destination_table": "items"
                        }
                    ]
                }
            }
        """
        try:
            session = SessionManager.get_default_session()
            edges = set(session.graph.edges)
            session.graph.infer_links(verbose=False)
            edges = set(session.graph.edges) - edges

            return dict(
                success=True,
                message="Link inference completed",
                data=dict(inferred_links=[
                    dict(
                        source_table=edge.src_table,
                        foreign_key=edge.fkey,
                        destination_table=edge.dst_table,
                    ) for edge in edges
                ]),
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to infer links. {e}",
            )

    @mcp.tool()
    async def inspect_graph() -> Dict[str, Any]:
        """Obtains the complete graph structure including all tables and their
        relationships. This operation provides a comprehensive view of the
        current graph state, including all tables, their schemas, and the links
        between them. Use this tool to check if the graph contains all the
        tables and edges that you will use to generate predictions. The graph
        needs to be finalized before the KumoRFM model can start generating
        predictions.

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
                            "source_table": "orders",
                            "foreign_key": "user_id",
                            "destination_table": "users",
                        }
                    ]
                }
            }
        """
        try:
            session = SessionManager.get_default_session()

            tables = {
                table.name:
                dict(
                    num_rows=len(table._data),
                    columns=list(table._data.columns),
                    primary_key=table._primary_key,
                    time_column=table._time_column,
                )
                for table in session.graph.tables.values()
            }

            links = [
                dict(
                    source_table=edge.src_table,
                    foreign_key=edge.fkey,
                    destination_table=edge.dst_table,
                ) for edge in session.graph.edges
            ]

            return dict(
                success=True,
                message="Graph structure retrieved successfully",
                data=dict(tables=tables, links=links),
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
        destination_table: str,
    ) -> Dict[str, Any]:
        """Creates a link (edge) between two tables in the graph. This tool
        allows you to manually link two tables via a foreign key column. To
        see the list of links in the graph, use ``inspect_graph`` tool.

        Args:
            source_table: Name of the source table (e.g., ``'orders'``)
            foreign_key: Column name in the source table that acts as a foreign
                key to link to the primary key of the destination table
                (e.g. ``'user_id'``)
            destination_table: Name of the destination table with a primary key
                (e.g. ``'users'``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Successfully linked 'orders' and 'users'
                            by 'user_id'",
            }
        """
        try:
            session = SessionManager.get_default_session()
            session.graph.link(source_table, foreign_key, destination_table)

            return dict(
                success=True,
                message=(f"Successfully linked '{source_table}' and "
                         f"'{destination_table}' by '{foreign_key}'"),
            )
        except Exception as e:
            return dict(
                success=False,
                message=(f"Failed to link '{source_table}' and "
                         f"'{destination_table}' by '{foreign_key}'. {e}"),
            )

    @mcp.tool()
    async def unlink_tables(
        source_table: str,
        foreign_key: str,
        destination_table: str,
    ) -> Dict[str, Any]:
        """Removes a link (edge) between two tables in the graph. This tool
        allows you to manually unlink two tables via a foreign key column. To
        see the list of links in the graph, use ``inspect_graph`` tool.

        Args:
            source_table: Name of the source table (e.g., ``'orders'``)
            foreign_key: Column name in the source table that acts as a foreign
                key to link to the primary key of the destination table
                (e.g. ``'user_id'``)
            destination_table: Name of the destination table with a primary key
                (e.g. ``'users'``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Successfully unlinked 'orders' and 'users'
                            by 'user_id'",
            }
        """
        try:
            session = SessionManager.get_default_session()
            session.graph.unlink(source_table, foreign_key, destination_table)

            return dict(
                success=True,
                message=(f"Successfully unlinked '{source_table}' and "
                         f"'{destination_table}' by '{foreign_key}'"),
            )
        except Exception as e:
            return dict(
                success=False,
                message=(f"Failed to unlink '{source_table}' and "
                         f"'{destination_table}' by '{foreign_key}'. {e}"),
            )
