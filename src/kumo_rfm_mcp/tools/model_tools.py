from typing import Annotated, Literal

import pandas as pd
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from kumo_rfm_mcp import EvaluateResponse, PredictResponse, SessionManager


def predict(
    query: str,
    anchor_time: str | None = None,
    run_mode: Literal['fast', 'normal', 'best'] = 'fast',
    num_neighbors: Annotated[
        list[int],
        Field(min_length=0, max_length=6),
    ] | None = None,
    max_pq_iterations: int = 20,
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

    Args:
        query: The predictive query, *e.g.*,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"`` or
            ``"PREDICT users.age FOR users.user_id IN (1, 2)"``
        anchor_time: The timestamp for which we are making a prediction for the
            future in YYYY-MM-DD hh:mm:ss format. If ``None``, will use the
            maximum timestamp in the data as anchor timestamp.
            If "entity", will use the timestamp of the entity as anchor
            timestamp (only valid for static predictive queries for which the
            entity table contains a time column), which is useful to prevent
            future data leakage when predicting for facts, *e.g.*, predicting
            whether a transaction is fraudulent should happen at the point in
            time the transaction was created.
        run_mode: The run mode for the query.
            Trades runtime with model performance.
            The run mode dictates how many training examples are sampled to
            make a prediction, *i.e.* 1000 ("fast"), 5000 ("normal") and 10000
            ("best").
        num_neighbors: The number of neighbors to sample for each hop to create
            training and test subgraphs.
            For example, ``[24, 12]`` samples 24 neighbors in the first hop and
            12 neighbors in the second hop.
            If ``None`` (recommended), will use two-hop sampling with 32
            neighbors ("fast") or "64" neighbors ("normal" or "best") in each
            hop.
            Up to 6-hop subgraphs are supported.
            Decreasing the number of neighbors per hop can prevent
            oversmoothing.
            Increasing the number of neighbors per hop allows the model to look
            at a larger historical time window.
            Increasing the number of hops can improve performance in case
            important signal is far away from the entity table, but can result
            in massive subgraphs.
            We advise to let the number of neighbors gradually shrink down in
            later hops to prevent recursive neighbor explosion, *e.g.*,
            ``num_neighbors=[32, 32, 4, 4, 2, 2]``.
        max_pq_iterations: The maximum number of iterations to perform to
            collect valid training labels. It is advised to increase the number
            of iterations in case the model fails to find the upper bound of
            supported training examples w.r.t. the run mode, *i.e.* 1000
            ("fast"), 5000 ("normal") and 10000 ("best").
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        try:
            anchor_time = pd.Timestamp(anchor_time)
        except ValueError:
            raise ToolError(f"Invalid anchor time '{anchor_time}'. Use "
                            f"'YYYY-MM-DD hh:mm:ss' format or 'entity'")
    try:
        df = model.predict(
            query,
            anchor_time=anchor_time,
            run_mode=run_mode,
            num_neighbors=num_neighbors,
            max_pq_iterations=max_pq_iterations,
            verbose=False,
        )
        return df.to_dict(orient='records')
    except Exception as e:
        raise ToolError("Prediction failed: {e}") from e


def evaluate(
    query: str,
    metrics: list[str] | None = None,
    anchor_time: str | None = None,
    run_mode: Literal['fast', 'normal', 'best'] = 'fast',
    num_neighbors: Annotated[
        list[int],
        Field(min_length=0, max_length=6),
    ] | None = None,
    max_pq_iterations: int = 20,
) -> dict[str, float | None]:
    """Evaluate a predictive query and returns performance metrics, comparing
    predictions against known ground-truth labels from historical examples.

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

    Args:
        query: The predictive query, *e.g.*,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"`` or
            ``"PREDICT users.age FOR users.user_id IN (1, 2)"``
        metrics: The metrics to use for evaluation. If ``None``, will use a
            pre-selection of metrics depending on the given predictive query.
        anchor_time: The timestamp for which we are making a prediction for the
            future in YYYY-MM-DD hh:mm:ss format. If ``None``, will use the
            maximum timestamp in the data as anchor timestamp.
            If "entity", will use the timestamp of the entity as anchor
            timestamp (only valid for static predictive queries for which the
            entity table contains a time column), which is useful to prevent
            future data leakage when predicting for facts, *e.g.*, predicting
            whether a transaction is fraudulent should happen at the point in
            time the transaction was created.
        run_mode: The run mode for the query.
            Trades runtime with model performance.
            The run mode dictates how many training examples are sampled to
            make a prediction, *i.e.* 1000 ("fast"), 5000 ("normal") and 10000
            ("best").
        num_neighbors: The number of neighbors to sample for each hop to create
            training and test subgraphs.
            For example, ``[24, 12]`` samples 24 neighbors in the first hop and
            12 neighbors in the second hop.
            If ``None`` (recommended), will use two-hop sampling with 32
            neighbors ("fast") or "64" neighbors ("normal" or "best") in each
            hop.
            Up to 6-hop subgraphs are supported.
            Decreasing the number of neighbors per hop can prevent
            oversmoothing.
            Increasing the number of neighbors per hop allows the model to look
            at a larger historical time window.
            Increasing the number of hops can improve performance in case
            important signal is far away from the entity table, but can result
            in massive subgraphs.
            We advise to let the number of neighbors gradually shrink down in
            later hops to prevent recursive neighbor explosion, *e.g.*,
            ``num_neighbors=[32, 32, 4, 4, 2, 2]``.
        max_pq_iterations: The maximum number of iterations to perform to
            collect valid training labels. It is advised to increase the number
            of iterations in case the model fails to find the upper bound of
            supported training examples w.r.t. the run mode, *i.e.* 1000
            ("fast"), 5000 ("normal") and 10000 ("best").
    """
    model = SessionManager.get_default_session().model

    if anchor_time is not None and anchor_time != "entity":
        try:
            anchor_time = pd.Timestamp(anchor_time)
        except ValueError:
            raise ToolError(f"Invalid anchor time '{anchor_time}'. Use "
                            f"'YYYY-MM-DD hh:mm:ss' format or 'entity'")

    try:
        df = model.evaluate(
            query,
            metrics=metrics,
            anchor_time=anchor_time,
            run_mode=run_mode,
            num_neighbors=num_neighbors,
            max_pq_iterations=max_pq_iterations,
            verbose=False,
        )
        return dict(
            success=True,
            message="Evaluation completed successfully",
            data=dict(metrics=df.to_dict(orient='records')),
        )
    except Exception as e:
        raise ToolError("Evaluation failed: {e}") from e


def register_model_tools(mcp: FastMCP):
    """Register all model tools to the MCP server."""
    mcp.tool()(predict)
    mcp.tool()(evaluate)
