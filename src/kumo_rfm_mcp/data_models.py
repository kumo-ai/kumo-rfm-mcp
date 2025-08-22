from typing import Annotated, Dict, List, Optional

from pydantic import BaseModel


class TableMetadata(BaseModel):
    """Metadata for a single table."""
    name: Annotated[str, "Name of the table"]
    path: Annotated[str, "Path to the table"]
    primary_key: Annotated[str, "Name of the primary key column"]
    time_column: Annotated[Optional[str], "Name of the time column"]
    stypes: Annotated[Dict[str, str],
                      "Column names mapped to their semantic types"]


class LinkMetadata(BaseModel):
    """Metadata for a foreign key relationship between tables."""
    source_table: Annotated[
        str, "Name of the source table containing the foreign key"]
    foreign_key: Annotated[str, "Name of the foreign key column"]
    destination_table: Annotated[
        str, "Name of the destination table containing the primary key"]


class GraphMetadata(BaseModel):
    """Complete metadata schema for the session."""
    tables: Annotated[List[TableMetadata], "List of table metadata"]
    links: Annotated[List[LinkMetadata], "List of foreign key relationships"]
