import logging
from typing import Annotated, Any, Dict

from fastmcp import FastMCP
from kumoai.experimental import rfm
from pydantic import Field

from kumo_rfm_mcp import SessionManager

logger = logging.getLogger('kumo-rfm-mcp.model_tools')


def register_model_tools(mcp: FastMCP):
    """Register all model tools with the MCP server."""
    @mcp.tool()
    async def validate_query(query: str) -> Dict[str, Any]:
        """Validates a predictive query string against the current finalized
        graph structure.

        This operation checks if the query syntax is correct and compatible
        with the current graph schema without executing the prediction and
        raises validation errors if the query is not valid. Use this tool to
        check if the query is valid before running it, it is also valuable to
        figure out what the correct query syntax for a particular prediction
        is.

        The query syntax should always follow the format:
        * PREDICT <target_expression>
            - Declares the value or aggregate the model should predict
        * FOR <entity_specification>
            - Specifies the single ID or list of IDs to predict for
        * WHERE <filters> (optional)
            - Filters which historical rows are used to generate features

        Refer to relevant resources for more information:
        # kumo://docs/pql-guide
        # kumo://docs/pql-reference

        Args:
            query: The predictive query to validate (e.g.,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Query validated successfully",
            }
        """
        try:
            session = SessionManager.get_default_session()

            if session.model is None:
                return dict(
                    success=False,
                    message=("No model available. Please call "
                             "'finalize_graph' first"),
                )

            session.model._parse_query(query)

            return dict(
                success=True,
                message="Query validated successfully",
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Query validation failed. {e}",
            )

    @mcp.tool()
    async def predict(
        query: str,
        anchor_time: str = None,
        run_mode: str = "fast",
        num_neighbors: Annotated[
            list[int],
            Field(
                min_length=1,
                max_length=6,
                description=("Number of neighbors to sample for each hop "
                             "(1-6 hops max)"),
            )] | None = None,
        max_pq_iterations: int = 20,
    ) -> Dict[str, Any]:
        """Executes a predictive query and returns model predictions. This tool
        runs the specified predictive query against the KumoRFM model and
        returns the predictions as tabular data. The graph needs to be
        finalized before the KumoRFM model can start generating predictions.

        The query syntax should always follow the format:
        * PREDICT <target_expression>
            - Declares the value or aggregate the model should predict
        * FOR <entity_specification>
            - Specifies the single ID or list of IDs to predict for
        * WHERE <filters> (optional)
            - Filters which historical rows are used to generate features

        Refer to relevant resources for more information:
        # kumo://docs/pql-guide
        # kumo://docs/pql-reference

        Args:
            query: The predictive query to validate (e.g.,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"``)
            anchor_time: The anchor timestamp for the query in YYYY-MM-DD
                format. If None, will use the maximum timestamp in the data.
                If "entity", will use the timestamp of the entity.
            run_mode: The run mode for the query. Options: "fast", "normal",
                "best".
            num_neighbors: The number of neighbors to sample for each hop. E.g.
                [12, 24] means 12 neighbors for the first hop and 24 neighbors
                for the second hop.
            max_pq_iterations: The maximum number of iterations to perform to
                collect valid labels. It is advised to increase the number of
                iterations in case the predictive query has strict entity
                filters.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
        input: {
            "query": "PREDICT COUNT(orders.*, 0, 30, days)>0 FOR
            users.user_id=1",
            "anchor_time": "2019-01-01",
            "run_mode": "fast",
            "num_neighbors": [12, 24],
            "max_pq_iterations": 20
        }

        output: {
                "success": true,
                "message": "Prediction completed successfully",
                "data": {
                    "predictions": [
                        {
                            "ENTITY": 1,
                            "TARGET_PRED": true,
                            "True_PROB": 0.85,
                            "False_PROB": 0.15
                        }
                    ]
                }
            }
        """
        try:
            session = SessionManager.get_default_session()

            if session.model is None:
                return dict(
                    success=False,
                    message=("No model available. Please call "
                             "'finalize_graph' first"),
                )

            # Convert anchor_time string to pandas Timestamp if provided
            if anchor_time and anchor_time != "entity":
                try:
                    import pandas as pd
                    anchor_time = pd.Timestamp(anchor_time)
                except ValueError:
                    return dict(
                        success=False,
                        message=f"Invalid anchor_time format: {anchor_time}. "
                        f"Use YYYY-MM-DD format or 'entity'",
                    )

            logger.info(f"Running prediction for query: {query}")
            result_df = session.model.predict(
                query,
                anchor_time=anchor_time,
                run_mode=run_mode,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                verbose=False,
            )
            logger.info("Prediction completed")

            return dict(
                success=True,
                message="Prediction completed successfully",
                data=dict(predictions=result_df.to_dict(orient='records'), ),
            )
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return dict(
                success=False,
                message=f"Prediction failed. {e}",
            )

    @mcp.tool()
    async def evaluate(
        query: str,
        anchor_time: str = None,
        run_mode: str = "fast",
        num_neighbors: Annotated[
            list[int],
            Field(
                min_length=1,
                max_length=6,
                description=("Number of neighbors to sample for each hop "
                             "(1-6 hops max)"),
            )] | None = None,
        max_pq_iterations: int = 20,
        random_seed: int = None,
    ) -> Dict[str, Any]:
        """Evaluates a predictive query and returns performance metrics. This
        tool runs the specified predictive query in evaluation mode, comparing
        predictions against known ground truth labels and returning performance
        metrics. The graph needs to be finalized before the KumoRFM model can
        start generating evaluation metrics.

        The query syntax should always follow the format:
        * PREDICT <target_expression>
            - Declares the value or aggregate the model should predict
        * FOR <entity_specification>
            - Specifies the single ID or list of IDs to predict for
        * WHERE <filters> (optional)
            - Filters which historical rows are used to generate features

        Refer to relevant resources for more information:
        # kumo://docs/pql-guide
        # kumo://docs/pql-reference

        Args:
            query: The predictive query to validate (e.g.,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"``)
            anchor_time: The anchor timestamp for the query in YYYY-MM-DD
                format. If None, will use the maximum timestamp in the data.
                If "entity", will use the timestamp of the entity.
            run_mode: The run mode for the query. Options: "fast", "normal",
                "best".
            num_neighbors: The number of neighbors to sample for each hop. E.g.
                [12, 24] means 12 neighbors for the first hop and 24 neighbors
                for the second hop.
            max_pq_iterations: The maximum number of iterations to perform to
                collect valid labels. It is advised to increase the number of
                iterations in case the predictive query has strict entity
                filters.
            random_seed: A manual seed for generating pseudo-random numbers.
                If None, uses the default random seed.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
        input: {
            "query": "PREDICT COUNT(orders.*, 0, 30, days)>0
            FOR users.user_id=1",
            "anchor_time": "2019-01-01",
            "run_mode": "fast",
            "num_neighbors": [12, 24],
            "max_pq_iterations": 20,
            "random_seed": 42
        }

        output: {
                "success": true,
                "message": "Evaluation completed successfully",
                "data": {
                    "metrics": [
                        {"metric": "auroc", "value": 0.87},
                        {"metric": "auprc", "value": 0.56}
                    ]
                }
            }
        """
        try:
            session = SessionManager.get_default_session()

            if session.model is None:
                return dict(
                    success=False,
                    message=("No model available. Please call "
                             "'finalize_graph' first"),
                )

            # Convert anchor_time string to pandas Timestamp if provided
            if anchor_time and anchor_time != "entity":
                try:
                    import pandas as pd
                    anchor_time = pd.Timestamp(anchor_time)
                except ValueError:
                    return dict(
                        success=False,
                        message=f"Invalid anchor_time format: {anchor_time}. "
                        f"Use YYYY-MM-DD format or 'entity'",
                    )

            logger.info(f"Running evaluation for query: {query}")
            result_df = session.model.evaluate(
                query,
                anchor_time=anchor_time,
                run_mode=run_mode,
                num_neighbors=num_neighbors,
                max_pq_iterations=max_pq_iterations,
                random_seed=random_seed,
                verbose=False,
            )
            logger.info("Evaluation completed")

            return dict(
                success=True,
                message="Evaluation completed successfully",
                data=dict(metrics=result_df.to_dict(orient='records')),
            )
        except Exception as e:
            logger.error(f"Evaluation failed: {e}")
            return dict(
                success=False,
                message=f"Evaluation failed. {e}",
            )
