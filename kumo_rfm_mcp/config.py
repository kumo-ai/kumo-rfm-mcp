from pathlib import Path
from typing import Annotated, Any

from kumoapi.rfm import Explanation
from kumoapi.typing import Dtype, Stype
from pydantic import BaseModel, Field


class TableSource(BaseModel):
    """Source information of a table."""
    path: Annotated[
        Path,
        "Path to a local file. Only CSV or Parquet files are supported.",
    ]
    bytes: Annotated[int, "Size in bytes of the file"]


class TableSourcePreview(BaseModel):
    """Preview of the first rows of a table-like file."""
    rows: Annotated[
        list[dict[str, Any]],
        Field(
            default_factory=list,
            description=("Each row in the table source is represented as a "
                         "dictionary mapping column names to their "
                         "corresponding values."),
        ),
    ]


class TableMetadata(BaseModel):
    """Metadata for a table."""
    path: Annotated[
        str,
        ("Path to the table. Can be a local file path, an S3 URI "
         "(s3://...), or an HTTP/HTTPS URL."),
    ]
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
    primary_key: Annotated[str | None, "Name of the primary key column"]
    time_column: Annotated[
        str | None,
        "Name of the time column marking when the record becomes active",
    ]
    end_time_column: Annotated[
        str | None,
        ("Name of the end time column marking when the record stops being "
         "active"),
    ]


class AddTableMetadata(BaseModel):
    """Metadata to add a new table."""
    path: Annotated[
        str,
        ("Path to the table. Can be a local file path, an S3 URI "
         "(s3://...), or an HTTP/HTTPS URL."),
    ]
    name: Annotated[str, "Name of the table"]
    primary_key: Annotated[
        str | None,
        Field(
            default=None,
            description="Name of the primary key column",
        ),
    ]
    time_column: Annotated[
        str | None,
        Field(
            default=None,
            description=("Name of the time column marking when the record "
                         "becomes active"),
        ),
    ]
    end_time_column: Annotated[
        str | None,
        Field(
            default=None,
            description=("Name of the end time column marking when the record "
                         "stops being active"),
        ),
    ]


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
        str | None,
        Field(
            default=None,
            description=("Update the primary key column. Set to `None` if the "
                         "primary key should be discarded. If omitted, the "
                         "current primary key will be untouched."),
        ),
    ]
    time_column: Annotated[
        str | None,
        Field(
            default=None,
            description=("Update the time column. Set to `None` if the time "
                         "column should be discarded. If omitted, the current "
                         "time column will be untouched."),
        ),
    ]
    end_time_column: Annotated[
        str | None,
        Field(
            default=None,
            description=("Update the end time column. Set to `None` if the "
                         "end time column should be discarded. If omitted, "
                         "the current end time column will be untouched."),
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
    tables_to_add: Annotated[
        list[AddTableMetadata],
        Field(default_factory=list, description="Tables to add"),
    ]
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
    tables_to_remove: Annotated[
        list[str],
        Field(default_factory=list, description="Tables to remove"),
    ]


class UpdatedGraphMetadata(BaseModel):
    """Updated metadata of a graph holding multiple tables connected via "
    "foreign key-primary key relationships."""
    graph: Annotated[GraphMetadata, "Updated graph metadata"]
    errors: Annotated[
        list[str],
        Field(
            default_factory=list,
            description="Any errors encountered during the update process",
        ),
    ]


class MaterializedGraphInfo(BaseModel):
    """Information about the materialized graph."""
    num_nodes: Annotated[int, "Number of nodes in the graph"]
    num_edges: Annotated[int, "Number of edges in the graph"]
    time_ranges: Annotated[
        dict[str, str],
        Field(
            default_factory=dict,
            description=("Earliest to latest timestamp for each table in the "
                         "graph that contains a time column"),
        ),
    ]


class PredictResponse(BaseModel):
    predictions: Annotated[
        list[dict[str, Any]],
        Field(
            default_factory=list,
            description=(
                "The predictions, where each row holds information about the "
                "entity, the anchor time, and the prediction scores"),
        ),
    ]
    logs: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=("Prediction-specific log messages such as number of "
                         "context examples, the underlying task type and the "
                         "label distribution"),
        ),
    ]


class EvaluateResponse(BaseModel):
    metrics: Annotated[
        dict[str, float | None],
        Field(
            default_factory=dict,
            description="The metric value for every metric",
        ),
    ]
    logs: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=("Evaluation-specific log messages such as number of "
                         "context and test examples, the underlying task type "
                         "and the label distribution"),
        ),
    ]


class ExplanationResponse(BaseModel):
    prediction: Annotated[
        dict[str, Any],
        ("The prediction, holding information about the entity, the anchor "
         "time, and the prediction scores"),
    ]
    explanation: Annotated[
        Explanation,
        ("The explanation of the prediction. Provides both a global, "
         "column-level analysis and a local, cell-level attribution view. "
         "The global analysis clusters column distributions of in-context "
         "examples into cohorts and relates them to their relevance with "
         "respect to ground-truth labels. The local view computes "
         "gradient-based attribution scores over prediction subgraphs. "
         "Together, these views enable comprehensive interpretation."),
    ]
    logs: Annotated[
        list[str],
        Field(
            default_factory=list,
            description=("Prediction-specific log messages such as number of "
                         "context examples, the underlying task type and the "
                         "label distribution"),
        ),
    ]
