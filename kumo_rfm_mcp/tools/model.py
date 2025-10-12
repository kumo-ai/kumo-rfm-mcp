import asyncio
from datetime import datetime
from typing import Annotated, Literal

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.utils import ProgressLogger
from pydantic import Field

from kumo_rfm_mcp import (
    EvaluateResponse,
    ExplanationResponse,
    PredictResponse,
    SessionManager,
)

query_doc = ("The predictive query string, e.g., "
             "'PREDICT COUNT(orders.*, 0, 30, days)>0 FOR EACH users.user_id' "
             "or 'PREDICT users.age FOR EACH users.user_id'")
indices_doc = ("The primary keys (entity indices) to generate predictions "
               "for. Up to 1000 entities are supported for an individual "
               "query. Predictions will be generated for all indices, "
               "regardless of whether they match any entity filter "
               "constraints.")
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
    "of metrics depending on the given predictive query. The following metrics"
    "are supported:\n"
    "Binary classification: 'acc', 'precision', 'recall', 'f1', 'auroc', "
    "'auprc', 'ap'\n"
    "Multi-class classification: 'acc', 'precision', 'recall', 'f1', 'mrr'\n"
    "Regression: 'mae', 'mape', 'mse', 'rmse', 'smape'\n"
    "Temporal link prediction: 'map@k', 'ndcg@k', 'mrr@k', 'precision@k', "
    "'recall@k', 'f1@k', 'hit_ratio@k' where 'k' needs to be an integer "
    "between 1 and 100")


async def predict(
    query: Annotated[str, query_doc],
    indices: Annotated[list[str] | list[float] | list[int], indices_doc],
    anchor_time: Annotated[
        datetime | Literal['entity'] | None,
        Field(default=None, description=anchor_time_doc),
    ],
    run_mode: Annotated[
        Literal['fast', 'normal', 'best'],
        Field(default='fast', description=run_mode_doc),
    ],
    num_neighbors: Annotated[
        list[int] | None,
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
    """Execute a predictive query and return model predictions.

    The graph needs to be materialized and the session needs to be
    authenticated before the KumoRFM model can start generating predictions.

    The output prediction format depends on the given task type.

    Binary classification:
    | ENTITY | ANCHOR_TIMESTAMP | TARGET_PRED | False_PROB | True_PROB |
    where 'ENTITY' holds the entity ID, 'ANCHOR_TIMESTAMP' holds the anchor
    time of the prediction in unix format, 'TARGET_PRED' holds the final
    prediction based on a threshold of 0.5, and 'False_PROB' and 'True_PROB'
    hold the probabilities.

    Multi-class classification:
    | ENTITY | ANCHOR_TIMESTAMP | CLASS | SCORE | PREDICTED |
    where 'ENTITY' holds the entity ID, 'ANCHOR_TIMESTAMP' holds the anchor
    time of the prediction in unix format. Each row corresponds to an (ENTITY,
    CLASS) pair (up to 10 classes are reported), where 'CLASS' holds the
    predicted value, 'SCORE' holds its probability, and 'PREDICTED' denotes
    whether the (ENTITY, CLASS) pair has the highest likelihood.

    Regression:
    | ENTITY | ANCHOR_TIMESTAMP | TARGET_PRED |
    where 'ENTITY' holds the entity ID, 'ANCHOR_TIMESTAMP' holds the anchor
    time of the prediction in unix format, and 'TARGET_PRED' holds the
    predicted numerical value.

    Temporal link prediction:
    | ENTITY | ANCHOR_TIMESTAMP | CLASS | SCORE |
    where 'ENTITY' holds the entity ID, 'ANCHOR_TIMESTAMP' holds the anchor
    time of the prediction in unix format. Each row corresponds to an (ENTITY,
    CLASS) pair, where 'CLASS' holds the recommended item and 'SCORE' holds its
    likelihood.

    Important: Before executing or suggesting any predictive queries,
    read the documentation first at 'kumo://docs/predictive-query'.
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        anchor_time = pd.Timestamp(anchor_time)

    def _predict() -> PredictResponse:
        logger = ProgressLogger(query)

        try:
            df = model.predict(
                query,
                indices=indices,
                anchor_time=anchor_time,
                run_mode=run_mode,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                verbose=logger,
            )
        except Exception as e:
            raise ToolError(f"Prediction failed: {e}") from e

        logs = logger.logs
        if logger.start_time is not None:
            logs = logs + [f'Duration: {logger.duration:2f}s']

        return PredictResponse(
            predictions=df.to_dict(orient='records'),
            logs=logs,
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
        list[int] | None,
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

    The graph needs to be materialized and the session needs to be
    authenticated before the KumoRFM model can start evaluating.

    Take the label distribution of the predictive query in the output logs into
    account when analyzing the returned metrics.

    Important: Before executing or suggesting any predictive queries,
    read the documentation first at 'kumo://docs/predictive-query'.
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        anchor_time = pd.Timestamp(anchor_time)

    def _evaluate() -> EvaluateResponse:
        logger = ProgressLogger(query)

        try:
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

        df = df.astype(object).where(df.notna(), None)

        logs = logger.logs
        if logger.start_time is not None:
            logs = logs + [f'Duration: {logger.duration:2f}s']

        return EvaluateResponse(
            metrics=df.set_index('metric')['value'].to_dict(),
            logs=logs,
        )

    return await asyncio.to_thread(_evaluate)


async def explain(
    query: Annotated[str, query_doc],
    index: Annotated[
        str | float | int,
        "The primary key (entity index) of the prediction to explain",
    ],
    anchor_time: Annotated[
        datetime | Literal['entity'] | None,
        Field(default=None, description=anchor_time_doc),
    ],
    num_neighbors: Annotated[
        list[int] | None,
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
) -> ExplanationResponse:
    """Execute a predictive query and explain the model prediction.

    The graph needs to be materialized and the session needs to be
    authenticated before the KumoRFM model can start generating an explanation
    for a prediction.

    Only a single entity prediction can be explained at a time.
    The `run_mode` will be fixed to `'fast'` mode for explainability.
    Note that the model prediction returned by the explanation might differ
    slightly from the result of the `predict` tool due to floating-point
    precision. Ignore such small differences.

    Important: Before executing or suggesting any predictive queries,
    read the documentation first at 'kumo://docs/predictive-query'.

    Important: Before analyzing the explanation output, read the documentation
    first at 'kumo://docs/explainability'.
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        anchor_time = pd.Timestamp(anchor_time)

    def _explain() -> ExplanationResponse:
        logger = ProgressLogger(query)

        try:
            out = model.predict(
                query,
                indices=[index],
                explain=True,
                anchor_time=anchor_time,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                verbose=logger,
            )
        except Exception as e:
            raise ToolError(f"Explanation failed: {e}") from e

        logs = logger.logs
        if logger.start_time is not None:
            logs = logs + [f'Duration: {logger.duration:2f}s']

        return ExplanationResponse(
            prediction=out.prediction.to_dict(orient='records')[0],
            explanation=out.details,
            logs=logs,
        )

    return await asyncio.to_thread(_explain)


def register_model_tools(mcp: FastMCP) -> None:
    """Register all model tools to the MCP server."""
    mcp.tool(annotations=dict(
        title="🤖 Running predictive query…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(predict)

    mcp.tool(annotations=dict(
        title="📊 Evaluating predictive query…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(evaluate)

    mcp.tool(annotations=dict(
        title="🧠 Explaining prediction…",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ))(explain)
