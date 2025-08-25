from typing import Annotated, Optional

from kumoapi.typing import Dtype, Stype
from pydantic import BaseModel, Field


class TableSource(BaseModel):
    """Source information of a table."""
    path: Annotated[str, "Path to the file"]
    bytes: Annotated[int, "Size in bytes of the file"]


class TableMetadata(BaseModel):
    """Metadata for a table."""
    path: Annotated[str, "Path to the table"]
    name: Annotated[str, "Name of the table"]
    num_rows: Annotated[int, "Number of rows in the table"]
    dtypes: Annotated[
        dict[str, Dtype],
        "Column names mapped to their data types",
    ]
    stypes: Annotated[
        dict[str, Stype | None],
        "Column names mapped to their semantic types or `None` if they have "
        "been discarded",
    ]
    primary_key: Annotated[Optional[str], "Name of the primary key column"]
    time_column: Annotated[Optional[str], "Name of the time column"]


class UpdateTableMetadata(BaseModel):
    """Metadata updates to perform for a table."""
    stypes: Annotated[
        dict[str, Stype | None],
        Field(
            default_factory=dict,
            description=("Update the semantic type of column names. Set to "
                         "`None` if the column should be discarded. Omitted "
                         "columns will be untouched."),
        ),
    ]
    primary_key: Annotated[
        Optional[str],
        Field(
            default=None,
            description=("Update the primary key column. Set to `None` if the "
                         "primary key should be discarded. If omitted, the "
                         "current primary key will be untouched."),
        ),
    ]
    time_column: Annotated[
        Optional[str],
        Field(
            default=None,
            description=("Update the time column. Set to `None` if the time "
                         "column should be discarded. If omitted, the current "
                         "time column will be untouched."),
        ),
    ]


class LinkMetadata(BaseModel):
    """Metadata for defining a link between two tables via foreign key-primary
    key relationships."""
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
    links: Annotated[list[LinkMetadata], "List of links"]


class UpdateGraphMetadata(BaseModel):
    """Metadata updates to perform for a graph holding multiple tables
    connected via foreign key-primary key relationships."""
    tables_to_update: Annotated[
        dict[str, UpdateTableMetadata],
        Field(
            default_factory=dict,
            description="Tables to update. Omitted tables will be untouched.",
        ),
    ]
    links_to_remove: Annotated[
        list[LinkMetadata],
        Field(default_factory=list, description="Links to remove"),
    ]
    links_to_add: Annotated[
        list[LinkMetadata],
        Field(default_factory=list, description="Links to add"),
    ]
