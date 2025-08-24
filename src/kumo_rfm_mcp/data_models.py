from typing import Annotated, Optional

from kumoapi.typing import Stype
from pydantic import BaseModel


class TableSource(BaseModel):
    """Source information of a table."""
    path: Annotated[str, "Path to the file"]
    bytes: Annotated[int, "Size in bytes of the file"]


class TableMetadata(BaseModel):
    """Metadata for a table."""
    name: Annotated[str, "Name of the table"]
    path: Annotated[str, "Path to the table"]
    primary_key: Annotated[str, "Name of the primary key column"]
    time_column: Annotated[Optional[str], "Name of the time column"]
    stypes: Annotated[
        dict[str, Stype],
        "Column names mapped to their semantic types",
    ]


class LinkMetadata(BaseModel):
    """Metadata for defining a link between two tables."""
    source_table: Annotated[
        str,
        "Name of the source table containing the foreign key",
    ]
    foreign_key: Annotated[str, "Name of the foreign key column"]
    destination_table: Annotated[
        str,
        "Name of the destination table containing the primary key to link to",
    ]


class GraphMetadata(BaseModel):
    """Metadata of a graph holding multiple tables connected via foreign
    key-primary key relationships."""
    tables: Annotated[list[TableMetadata], "List of tables"]
    links: Annotated[
        list[LinkMetadata],
        "List of foreign key-primary key relationships",
    ]
