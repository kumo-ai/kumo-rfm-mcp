import asyncio
import os.path as osp
from collections import defaultdict
from typing import Annotated

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm
from kumoai.graph import Edge
from kumoai.utils import ProgressLogger
from kumoapi.typing import Dtype, Stype
from pydantic import Field

from kumo_rfm_mcp import (
    GraphMetadata,
    LinkMetadata,
    MaterializedGraphInfo,
    SessionManager,
    TableMetadata,
    TableSourcePreview,
    UpdatedGraphMetadata,
    UpdateGraphMetadata,
)

_materialize_lock = asyncio.Lock()


def inspect_graph_metadata() -> GraphMetadata:
    """Inspect the current graph metadata.

    Confirming that the metadata is set up correctly is crucial for the RFM
    model to work properly. In particular,

    * primary keys and time columns need to be correctly specified for each
      table in case they exist;
    * columns need to point to a valid semantic type that describe their
      semantic meaning, or ``None`` if they have been discarded;
    * links need to point to valid foreign key-primary key relationships.
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
                dtypes[column] = rfm.utils.to_dtype(table._data[column])
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
                end_time_column=table._end_time_column,
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

    Make sure that tables are correctly linked before proceeding.

    Note that all operations can be performed in a batch at once, *e.g.*, one
    can add new tables and directly link them to together.

    Important: Before creating and updating graphs, read the documentation
    first at 'kumo://docs/graph-setup'.
    """
    session = SessionManager.get_default_session()
    session._model = None  # Need to reset the model if graph changes.
    graph = session.graph

    errors: list[str] = []
    for table in update.tables_to_add:
        path = osp.expanduser(table.path)
        suffix = path.rsplit('.', maxsplit=1)[-1].lower()

        if table.name in graph and graph[table.name]._path == path:
            graph[table.name].primary_key = table.primary_key
            graph[table.name].time_column = table.time_column
            graph[table.name].end_time_column = table.end_time_column
            continue

        if suffix not in {'csv', 'parquet'}:
            errors.append(f"'{path}' is not a valid CSV or Parquet file")
            continue

        try:
            if suffix == 'csv':
                df = pd.read_csv(path)
            else:
                assert suffix == 'parquet'
                df = pd.read_parquet(path)
        except Exception as e:
            errors.append(f"Could not read file '{path}': {e}")
            continue

        try:
            local_table = rfm.LocalTable(
                df=df,
                name=table.name,
                primary_key=table.primary_key,
                time_column=table.time_column,
                end_time_column=table.end_time_column,
            )
            local_table._path = path
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
            if 'end_time_column' in table_update:
                graph[table_name].end_time_column = table_update[
                    'end_time_column']
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
        except Exception:
            continue

    for link in update.links_to_add:
        if Edge(
                src_table=link.source_table,
                fkey=link.foreign_key,
                dst_table=link.destination_table,
        ) in graph.edges:
            continue

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
        except Exception:
            continue

    try:
        graph.validate()
    except Exception as e:
        errors.append(f"Final graph validation failed: {e}")

    return UpdatedGraphMetadata(graph=inspect_graph_metadata(), errors=errors)


def get_mermaid(
    show_columns: Annotated[
        bool,
        Field(
            default=True,
            description=("Controls whether all columns of a table are shown. "
                         "If `False`, only the primary key, foreign keys and "
                         "time column are displayed. Setting this to `False` "
                         "is recommended for feature-rich tables to avoid "
                         "cluttering the diagram with less relevant details."),
        ),
    ],
) -> str:
    """Return the graph as a Mermaid entity relationship diagram.

    Important: The returned Mermaid markup can be used to input into an
    artifact to render it visually on the client side.
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
                    and column.name != table._time_column
                    and column.name != table._end_time_column):
                feat_columns.append(column)

        lines.append(f"{' ' * 4}{table.name} {{")
        if pkey := table.primary_key:
            lines.append(f"{' ' * 8}{pkey.stype} {pkey.name} PK")
        for fkey_name in fkey_dict[table.name]:
            fkey = table[fkey_name]
            lines.append(f"{' ' * 8}{fkey.stype} {fkey.name} FK")
        if time_col := table.time_column:
            lines.append(f"{' ' * 8}{time_col.stype} {time_col.name}")
        if end_time_col := table.end_time_column:
            lines.append(f"{' ' * 8}{end_time_col.stype} {end_time_col.name}")
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


async def materialize_graph() -> MaterializedGraphInfo:
    """Materialize the graph based on the current state of the graph metadata
    to make it available for inference operations (e.g., ``predict`` and
    ``evaluate``).

    Any updates to the graph metadata require re-materializing the graph before
    the KumoRFM model can start making predictions again.
    """
    session = SessionManager.get_default_session()

    def _materialize_graph() -> rfm.KumoRFM:
        try:
            logger = ProgressLogger("Materializing graph")
            return rfm.KumoRFM(session.graph, verbose=logger)
        except Exception as e:
            raise ToolError(f"Failed to materialize graph: {e}")

    def _get_info(model: rfm.KumoRFM) -> MaterializedGraphInfo:
        store = model._graph_store
        num_nodes = sum(len(df) for df in store.df_dict.values())
        num_edges = sum(len(row) for row in store.row_dict.values())
        time_ranges = {}
        for table in session.graph.tables.values():
            if table._time_column is None:
                continue
            time = store.df_dict[table.name][table._time_column]
            if table.name in store.mask_dict.keys():
                time = time[store.mask_dict[table.name]]
            if len(time) == 0:
                continue
            time_ranges[table.name] = f"{time.min()} - {time.max()}"

        return MaterializedGraphInfo(
            num_nodes=num_nodes,
            num_edges=num_edges,
            time_ranges=time_ranges,
        )

    if session._model is None:
        async with _materialize_lock:
            session._model = await asyncio.to_thread(_materialize_graph)

    return await asyncio.to_thread(_get_info, session._model)


async def lookup_table_rows(
    table_name: Annotated[str, "Table name"],
    ids: Annotated[
        list[str | int | float],
        Field(
            min_length=1,
            max_length=1000,
            description="Primary keys to read",
        ),
    ],
) -> TableSourcePreview:
    """Lookup rows in the raw data frame of a table for a list of primary
    keys.

    In contrast to the 'inspect_table_files' tool, this tool can be used to
    query specific rows in a registered table in the graph.
    It should not be used to understand and analyze table schema.

    Use this tool to look up detailed information about recommended items to
    provide richer, more meaningful recommendations to users.

    The table to read from needs to have a primary key, and the graph has to be
    materialized.
    """
    model = SessionManager.get_default_session()._model

    if model is None:
        raise ToolError("Graph is not yet materialized")

    def _lookup_table_rows() -> TableSourcePreview:
        try:
            node_ids = model._graph_store.get_node_id(
                table_name=table_name,
                pkey=pd.Series(ids),
            )
            df = model._graph_store.df_dict[table_name].iloc[node_ids]
        except Exception as e:
            raise ToolError(str(e)) from e

        df = df.astype(object).where(df.notna(), None)
        return TableSourcePreview(rows=df.to_dict(orient='records'))

    return await asyncio.to_thread(_lookup_table_rows)


def register_graph_tools(mcp: FastMCP) -> None:
    """Register all graph tools to the MCP server."""
    mcp.tool(annotations=dict(
        title="🗂️ Reviewing graph schema…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(inspect_graph_metadata)

    mcp.tool(annotations=dict(
        title="🔄 Updating graph schema…",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(update_graph_metadata)

    mcp.tool(annotations=dict(
        title="🖼️ Creating graph diagram…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(get_mermaid)

    mcp.tool(annotations=dict(
        title="🕸️ Assembling graph…",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(materialize_graph)

    mcp.tool(annotations=dict(
        title="📂 Retrieving table entries…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(lookup_table_rows)
