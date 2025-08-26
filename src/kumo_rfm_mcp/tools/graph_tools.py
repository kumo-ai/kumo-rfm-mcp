from collections import defaultdict

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
    UpdatedGraphMetadata,
    UpdateGraphMetadata,
)


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
                path=table._path,
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

    return GraphMetadata(tables=tables, links=links)


def update_graph_metadata(update: UpdateGraphMetadata) -> UpdatedGraphMetadata:
    """Partially update the current graph metadata.

    Setting up the metadata is crucial for the RFM model to work properly. In
    particular,

    * primary keys and time columns need to be correctly specified for each
      table in case they exist;
    * columns need to point to a valid semantic type that describe their
      semantic meaning, or ``None`` if they should be discarded;
    * links need to point to valid foreign key-primary key relationships.

    Omitted fields will be untouched.

    For newly added tables, it is advised to double-check semantic types and
    modify in a follow-up step if necessary.

    Args:
        update: The metadata updates to perform.

    Returns:
        The updated graph metadata and any errors encountered during the update
        process.
    """
    session = SessionManager.get_default_session()
    session._model = None  # Need to reset the model if graph changes.
    graph = session.graph

    errors: list[str] = []
    for table in update.tables_to_add:
        if table.path.lower().endswith('.csv'):
            try:
                df = pd.read_csv(table.path)
            except Exception as e:
                errors.append(f"Could not read file '{table.path}': {e}")
                continue
        elif table.path.lower().endswith('.parquet'):
            try:
                df = pd.read_parquet(table.path)
            except Exception as e:
                errors.append(f"Could not read file '{table.path}': {e}")
                continue
        else:
            errors.append(f"'{table.path}' is not a valid CSV or Parquet file")
            continue

        try:
            local_table = rfm.LocalTable(
                df=df,
                name=table.name,
                primary_key=table.primary_key,
                time_column=table.time_column,
            )
            local_table._path = table.path
            graph.add_table(local_table)
        except Exception as e:
            errors.append(f"Could not add table '{table.name}': {e}")
            continue

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
            errors.append(f"Could not fully update table '{table_name}': {e}")
            continue

    for link in update.links_to_remove:
        try:
            graph.unlink(
                link.source_table,
                link.foreign_key,
                link.destination_table,
            )
        except Exception as e:
            errors.append(f"Could not remove link from source table "
                          f"'{link.source_table}' to destination table "
                          f"'{link.destination_table}' via the "
                          f"'{link.foreign_key}' column: {e}")
            continue

    for link in update.links_to_add:
        try:
            graph.link(
                link.source_table,
                link.foreign_key,
                link.destination_table,
            )
        except Exception as e:
            errors.append(f"Could not add link from source table "
                          f"'{link.source_table}' to destination table "
                          f"'{link.destination_table}' via the "
                          f"'{link.foreign_key}' column: {e}")
            continue

    for table_name in update.tables_to_remove:
        try:
            del graph[table_name]
        except Exception as e:
            errors.append(f"Could not remove table '{table.name}': {e}")
            continue

    try:
        graph.validate()
    except Exception as e:
        errors.append(f"Final graph validation failed: {e}")

    return UpdatedGraphMetadata(graph=inspect_graph_metadata(), errors=errors)


def get_mermaid(show_columns: bool = True) -> str:
    """Return the graph as a Mermaid entity relationship diagram.

    The returned Mermaid markup can be used to input into an artififact to
    render it visually on the client side.

    Args:
        show_columns: Whether tho show all columns in a table. If ``False``,
            will only show the primary key, foreign key(s), and time column of
            each table.

    Returns:
        The raw Mermaid markup as string.
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


def materialize_graph() -> Response[None]:
    """Materialize the graph from the current state of the graph metadata,
    which makes it available for inference operations (e.g., ``predict`` and
    ``evaluate``).

    Any updates to the graph metadata requires re-materializing the graph
    before the KumoRFM model can start making predictions.

    Returns:
        A response denoting whether the operation succeeded with additional log
        information.
    """
    session = SessionManager.get_default_session()

    if session._model is not None:
        raise ToolError("Graph is already materialized")

    logger = ProgressLogger(msg="Materialized graph")
    session._model = rfm.KumoRFM(session.graph, verbose=logger)

    try:
        logger.info("Starting graph materialization...")
        logger.info("KumoRFM model created successfully")
        return dict(
            success=True,
            message="Successfully finalized graph",
        )
    except Exception as e:
        logger.error(f"Failed to finalize graph: {e}")
        return dict(
            success=False,
            message=f"Failed to finalize graph. {e}",
        )


def register_graph_tools(mcp: FastMCP) -> None:
    """Register all graph management tools with the MCP server."""
    mcp.tool()(inspect_graph_metadata)
    mcp.tool()(update_graph_metadata)
    mcp.tool()(get_mermaid)
    mcp.tool()(materialize_graph)
