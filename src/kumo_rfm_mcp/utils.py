from typing import List

import pandas as pd

from .data_models import LinkMetadata, TableMetadata
from .session import Session


def extract_table_metadata(session: Session) -> List[TableMetadata]:
    """Extract table metadata from session graph.

    Args:
        session: The session containing the graph

    Returns:
        List of TableMetadata objects
    """
    tables: List[TableMetadata] = []
    for table in session.graph.tables.values():
        # Collect column stypes from the table metadata
        stypes: dict[str, str] = {}
        for column in table.columns:
            stypes[column.name] = str(column.stype)

        # Pull path from the session catalog if available
        path = ''
        if hasattr(session, 'catalog') and table.name in session.catalog:
            try:
                path = session.catalog[table.name].path
            except Exception:
                path = ''

        tables.append(
            TableMetadata(
                name=table.name,
                path=path,
                primary_key=table._primary_key if table._primary_key else "",
                time_column=table._time_column if table._time_column else None,
                stypes=stypes,
            ))

    return tables


def extract_link_metadata(session: Session) -> List[LinkMetadata]:
    """Extract link metadata from session graph.

    Args:
        session: The session containing the graph

    Returns:
        List of LinkMetadata objects
    """
    links: List[LinkMetadata] = []
    for edge in session.graph.edges:
        links.append(
            LinkMetadata(
                source_table=edge.src_table,
                foreign_key=edge.fkey,
                destination_table=edge.dst_table,
            ))

    return links


def load_dataframe(path: str, nrows: int | None = None) -> pd.DataFrame:
    """Load a DataFrame from a supported file path.

    Supports CSV and Parquet. Raises ValueError for unsupported formats.
    """
    lower = path.lower()
    if not (lower.endswith('.csv') or lower.endswith('.parquet')):
        raise ValueError(
            f"Can not read file from path '{path}'. Only '*.csv' or "
            "'*.parquet' files are supported")

    if lower.endswith('.csv'):
        return pd.read_csv(path, nrows=nrows)
    return pd.read_parquet(path)
