from pathlib import Path
from typing import Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.resources import FileResource


def register_docs_tools(mcp: FastMCP) -> None:
    """Register all documentation tools to the MCP server."""
    @mcp.tool(annotations=dict(
        title="📖 Reading…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    ))
    async def get_docs(
        resource_uri: Literal[
            'kumo://docs/overview',
            'kumo://docs/graph-setup',
            'kumo://docs/predictive-query',
            'kumo://docs/explainability',
        ]
    ) -> str:
        """Get documentation on how to use KumoRFM.

        KumoRFM is a pre-trained Relational Foundation Model (RFM) that
        generates training-free predictions on any relational multi-table
        data by interpreting the data as a (temporal) heterogeneous graph. It
        can be queried via the Predictive Query Language (PQL).

        Internal note: It is NOT related to "Recency, Frequency, Monetary"
        analysis.

        Internally, KumoRFM utilizes in-context learning to transfer patterns
        from historical examples to new unseen examples. Specifically, it
        constructs training/in-context subgraphs with known ground-truth
        labels and relates them to unseen subgraphs.

        See the 'kumo://docs/overview' resource for more information.

        KumoRFM can discover table-like files (e.g., CSV, Parquet), inspect
        them, and structure them into a graph via foreign key-primary key
        relationships. A time column in a table dictates the create time of a
        row, which is used downstream to receive and order historical
        interactions and prevent temporal leakage. Each column within a table
        is assigned a semantic type (numerical, categorical, multi-categorical,
        ID, text, timestamp, sequence, etc) that denotes the semantic meaning
        of the column and how it should be processed within the model.

        Important: Before creating and updating graphs, read the
        documentation first at 'kumo://docs/graph-setup'.

        After a graph is set up and materialized, KumoRFM can generate
        predictions (e.g., missing value imputation, temporal forecasts) and
        evaluations by querying the graph via the Predictive Query Language
        (PQL), a declarative language to formulate machine learning tasks.
        Understanding PQL and how it maps to a machine learning task is
        critical to achieve good model predictions. Besides PQL, various other
        options exist to tune model output, e.g., optimizing the `run_mode` of
        the model, specifying how subgraphs are formed via `num_neighbors`, or
        adjusting the `anchor_time` to denote the point in time for when a
        prediction should be made.

        Important: Before executing or suggesting any predictive queries,
        read the documentation first at 'kumo://docs/predictive-query'.

        KumoRFM can additionally generate explanations for predictions,
        providing both a global column-level analysis and a local, cell-level
        attribution view.
        Together, these views enable comprehensive interpretation.

        Important: Before analyzing the explanation output, read the
        documentation first at 'kumo://docs/explainability'.
        """
        resources = await mcp.get_resources()
        if resource_uri not in resources:
            raise ToolError(f"Resource '{resource_uri}' not found. Available "
                            f"resources: {list(resources.keys())}")

        resource = resources[resource_uri]

        if isinstance(resource, FileResource):
            if getattr(resource, 'path', None):
                path = Path(resource.path)
            else:  # Construct path from URI:
                name = f"{str(resource.uri).rsplit('/', 1)[-1]}.md"
                path = Path(__file__).parent.parent / 'resources' / name

            if not path.exists():
                raise ToolError(f"File resource '{resource_uri}' not found at "
                                "'{path}'")

            return path.read_text(encoding='utf-8')

        raise ToolError(f"Resource '{resource_uri}' is not accessible")
