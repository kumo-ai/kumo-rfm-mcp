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

    @mcp.tool()
    async def infer_links() -> dict[str, Any]:
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
            logger.info("Starting link inference")
            session = SessionManager.get_default_session()
            edges = set(session.graph.edges)
            logger.info(f"Graph currently has {len(edges)} existing links")

            session.graph.infer_links(verbose=False)
            new_edges = set(session.graph.edges) - edges
            logger.info(f"Inferred {len(new_edges)} new links")

            inferred_links = [
                dict(
                    source_table=edge.src_table,
                    foreign_key=edge.fkey,
                    destination_table=edge.dst_table,
                ) for edge in new_edges
            ]

            for link in inferred_links:
                logger.info(
                    f"Inferred link: {link['source_table']}."
                    f"{link['foreign_key']} -> {link['destination_table']}")

            return dict(
                success=True,
                message="Link inference completed",
                data=dict(inferred_links=inferred_links),
            )
        except Exception as e:
            logger.error(f"Failed to infer links: {e}")
            return dict(
                success=False,
                message=f"Failed to infer links. {e}",
            )

    @mcp.tool()
    async def inspect_graph() -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
    ) -> dict[str, Any]:
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
