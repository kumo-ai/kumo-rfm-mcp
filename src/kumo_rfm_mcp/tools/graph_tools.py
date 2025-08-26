import logging
from collections import defaultdict
from typing import Any

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm
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
    @mcp.tool()
    def inspect_graph_metadata() -> GraphMetadata:
        """Inspect the current graph metadata.

        Confirming that the metadata is set up correctly is crucial for the RFM
        model to work properly. In particular,

        * primary keys and time columns need to be correctly specified for each
          table in case they exist;
        * columns need to point to a valid semantic type that describe their
          semantic meaning, or ``None`` if they have been discarded;
        * links need to point to valid foreign key-primary key relationships.

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
          table in case they exist;
        * columns need to point to a valid semantic type that describe their
          semantic meaning, or ``None`` if they should be discarded;
        * links need to point to valid foreign key-primary key relationships.

        Omitted fields will be untouched.

        Args:
            update: The metadata updates to perform.

        Returns:
            The graph metadata.

        Raises:
            ToolError: If table or column names do not exist.
            ToolError: If semantic types are invalid for a column's data type.
            ToolError: If specified links are invalid.
        """

        session = SessionManager.get_default_session()
        session._model = None  # Need to reset the model if graph changes.
        graph = session.graph

        for table in update.tables_to_add:
            if table.path.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(table.path)
                except Exception as e:
                    raise ToolError(
                        f"Could not read file '{table.path}': {e}") from e
            elif table.path.lower().endswith('.parquet'):
                try:
                    df = pd.read_parquet(table.path)
                except Exception as e:
                    raise ToolError(
                        f"Could not read file '{table.path}': {e}") from e
            else:
                raise ToolError(f"File '{table.path}' is not a valid CSV or "
                                f"Parquet file")

            try:
                local_table = rfm.LocalTable(
                    df,
                    table.name,
                    primary_key=table.primary_key,
                    time_column=table.time_column,
                )
                local_table._path = table.path
                graph.add_table(local_table)
            except Exception as e:
                raise ToolError(str(e)) from e

        # Only keep specified keys:
        update_dict = update.model_dump(exclude_unset=True)
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

        for link in update.links_to_remove:
            try:
                graph.unlink(
                    link.source_table,
                    link.foreign_key,
                    link.destination_table,
                )
            except Exception as e:
                raise ToolError(str(e)) from e

        for link in update.links_to_add:
            try:
                graph.link(
                    link.source_table,
                    link.foreign_key,
                    link.destination_table,
                )
            except Exception as e:
                raise ToolError(str(e)) from e

        for table_name in update.tables_to_remove:
            try:
                del graph[table_name]
            except Exception as e:
                raise ToolError(str(e)) from e

        return get_graph_metadata()

    @mcp.tool()
    async def suggest_links() -> dict[str, Any]:
        """
        This tool suggests links between tables based on the current metadata
        state.
        """
        raise NotImplementedError("Link inference is not yet implemented")

    @mcp.tool()
    async def show_graph(show_columns: bool = True) -> str:
        """Visualize the graph as a mermaid entity relationship diagram.

        Args:
            show_columns: Whether tho show all columns in a table. If
                ``False``, will only show the primary key, foreign key(s), and
                time column of each table.
        """
        session = SessionManager.get_default_session()

        fkey_dict = defaultdict(list)
        for edge in session.graph.edges:
            fkey_dict[edge.src_table].append(edge.fkey)

        lines = ["erDiagram"]

        for table in session.graph.tables.values():
            feat_columns = []
            for column in table.columns:
                if (column.name != table._primary_key
                        and column.name not in fkey_dict[table.name]
                        and column.name != table._time_column):
                    feat_columns.append(column)

            lines.append(f"{' ' * 4}{table.name} {{")
            if pkey := table.primary_key:
                lines.append(f"{' ' * 8}{pkey.stype} {pkey.name} PK")
            for fkey_name in fkey_dict[table.name]:
                fkey = table[fkey_name]
                lines.append(f"{' ' * 8}{fkey.stype} {fkey.name} FK")
            if time_col := table.time_column:
                lines.append(f"{' ' * 8}{time_col.stype} {time_col.name}")
            if show_columns:
                for col in feat_columns:
                    lines.append(f"{' ' * 8}{col.stype} {col.name}")
            lines.append(f"{' ' * 4}}}")

        if len(session.graph.edges) > 0:
            lines.append("")

        for edge in session.graph.edges:
            lines.append(f"{' ' * 4}{edge.dst_table} o|--o{{ {edge.src_table} "
                         f": {edge.fkey}")

        return '\n'.join(lines)
