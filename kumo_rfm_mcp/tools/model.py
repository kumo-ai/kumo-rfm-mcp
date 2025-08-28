import asyncio
from datetime import datetime
from typing import Annotated, Literal

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.utils import ProgressLogger
from pydantic import Field

from kumo_rfm_mcp import EvaluateResponse, PredictResponse, SessionManager

query_doc = ("The predictive query string, e.g., "
             "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1 or "
             "PREDICT users.age FOR users.user_id IN (1, 2)")
anchor_time_doc = (
    "The anchor time for which we are making a prediction for the "
    "the future. If `None`, will use the maximum timestamp in the "
    "data as anchor time. If 'entity', will use the timestamp of "
    "the entity's time column as anchor time (only valid for "
    "static predictive queries for which the entity table "
    "contains a time column), which is useful to prevent future "
    "data leakage when imputing missing values on facts, e.g., "
    "predicting whether a transaction is fraudulent should "
    "happen at the point in time the transaction was created.")
run_mode_doc = (
    "The run mode for the query. Trades runtime with model performance. The "
    "run mode dictates how many training/in-context examples are sampled to "
    "make a prediction, i.e. 1000 for 'fast', 5000 for 'normal', and 10000 "
    "for 'best'.")
num_neighbors_doc = (
    "The number of neighbors to sample for each hop to create subgraphs. For "
    "example, `[24, 12]` samples 24 neighbors in the first hop and 12 "
    "neighbors in the second hop. If `None` (recommended), will use two-hop "
    "sampling with 32 neighbors in 'fast' mode, and 64 neighbors otherwise in "
    "each hop. Up to 6-hop subgraphs are supported. Decreasing the number of "
    "neighbors per hop can prevent oversmoothing. Increasing the number of "
    "neighbors per hop allows the model to look at a larger historical time "
    "window. Increasing the number of hops can improve performance in case "
    "important signal is far away from the entity table, but can result in "
    "massive subgraphs. We advise to let the number of neighbors gradually "
    "shrink down in later hops to prevent recursive neighbor explosion, e.g., "
    "`num_neighbors=[32, 32, 4, 4, 2, 2]`, if more hops are required.")
max_pq_iterations_doc = (
    "The maximum number of iterations to perform to collect valid training/"
    "in-context examples. It is advised to increase the number of iterations "
    "in case the model fails to find the upper bound of supported training "
    "examples w.r.t. the run mode, *i.e.* 1000 for 'fast', 5000 for 'normal' "
    "and 10000 for 'best'.")
metrics_doc = (
    "The metrics to use for evaluation. If `None`, will use a pre-selection "
    "of metrics depending on the given predictive query.")


async def predict(
    query: Annotated[str, query_doc],
    anchor_time: Annotated[
        datetime | Literal['entity'] | None,
        Field(default=None, description=anchor_time_doc),
    ],
    run_mode: Annotated[
        Literal['fast', 'normal', 'best'],
        Field(default='fast', description=run_mode_doc),
    ],
    num_neighbors: Annotated[
        list[int],
        Field(
            default=None,
            min_length=0,
            max_length=6,
            description=num_neighbors_doc,
        ),
    ],
    max_pq_iterations: Annotated[
        int,
        Field(default=20, description=max_pq_iterations_doc),
    ],
) -> PredictResponse:
    """Execute a predictive query and returns model predictions.

    The graph needs to be materialized before the KumoRFM model can start
    generating predictions.

    The query syntax should always follow the format:
    * PREDICT <target_expression>
      - Declares the value or aggregate the model should predict
    * FOR <entity_specification>
      - Specifies the single ID or list of IDs to predict for
    * WHERE <filters> (optional)
      - Filters which historical entities are used as training examples

    Refer to relevant resources for more information:
    # kumo://docs/pql-guide
    # kumo://docs/pql-reference
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        anchor_time = pd.Timestamp(anchor_time)

    def _predict() -> PredictResponse:
        try:
            logger = ProgressLogger(query)
            df = model.predict(
                query,
                anchor_time=anchor_time,
                run_mode=run_mode,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                verbose=logger,
            )
        except Exception as e:
            raise ToolError(f"Prediction failed: {e}") from e

        return PredictResponse(
            predictions=df.to_dict(orient='records'),
            logs=logger.logs + [f'Duration: {logger.duration:2f}s'],
        )

    return await asyncio.to_thread(_predict)


async def evaluate(
    query: Annotated[str, query_doc],
    metrics: Annotated[
        list[str] | None,
        Field(default=None, description=metrics_doc),
    ],
    anchor_time: Annotated[
        datetime | Literal['entity'] | None,
        Field(default=None, description=anchor_time_doc),
    ],
    run_mode: Annotated[
        Literal['fast', 'normal', 'best'],
        Field(default='fast', description=run_mode_doc),
    ],
    num_neighbors: Annotated[
        list[int],
        Field(
            default=None,
            min_length=0,
            max_length=6,
            description=num_neighbors_doc,
        ),
    ],
    max_pq_iterations: Annotated[
        int,
        Field(default=20, description=max_pq_iterations_doc),
    ],
) -> EvaluateResponse:
    """Evaluate a predictive query and return performance metrics which
    compares predictions against known ground-truth labels from historical
    examples.

    Note that the specific entities defined in the predictive query will be
    ignored. Instead, a number of entities with known historical ground-truth
    labels will be sampled for evaluation and to judge the quality of the
    predictive query.

    The graph needs to be materialized before the KumoRFM model can start
    evaluating.

    The query syntax should always follow the format:
    * PREDICT <target_expression>
      - Declares the value or aggregate the model should predict
    * FOR <entity_specification>
      - Specifies the single ID or list of IDs to predict for
    * WHERE <filters> (optional)
      - Filters which historical entities are used as training examples

    Refer to relevant resources for more information:
    # kumo://docs/pql-guide
    # kumo://docs/pql-reference
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        anchor_time = pd.Timestamp(anchor_time)

    def _evaluate() -> EvaluateResponse:
        try:
            logger = ProgressLogger(query)
            df = model.evaluate(
                query,
                metrics=metrics,
                anchor_time=anchor_time,
                run_mode=run_mode,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                verbose=logger,
            )
        except Exception as e:
            raise ToolError(f"Evaluation failed: {e}") from e

        metric_dict = df.set_index('metric')['value'].to_dict()
        return EvaluateResponse(
            metrics=metric_dict,
            logs=logger.logs + [f'Duration: {logger.duration:2f}s'],
        )

    return await asyncio.to_thread(_evaluate)


def register_model_tools(mcp: FastMCP) -> None:
    """Register all model tools to the MCP server."""
    mcp.tool(annotations=dict(
        title="Executing a predictive query",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(predict)

    mcp.tool(annotations=dict(
        title="Evaluating a predictive query",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(evaluate)
