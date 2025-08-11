import logging
from typing import Any, Dict

from fastmcp import FastMCP
from kumo_rfm_mcp import SessionManager
from kumoai.experimental import rfm

logger = logging.getLogger('kumo-rfm-mcp')


def register_model_tools(mcp: FastMCP):
    """Register all model tools with the MCP server."""

    @mcp.tool()
    async def finalize_graph() -> Dict[str, Any]:
        """
        This tool creates a KumoRFM model from the **current graph state**,
        making it available for inference operations (e.g., ``predict`` and
        ``evaluate``).

        The graph can be updated using the ``add_table``, ``remove_table``,
        ``link_tables``, and ``unlink_tables`` tools, but the graph needs to be
        finalized again before the KumoRFM model can start generating
        predictions with the new graph state.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Successfully finalized graph",
            }
        """
        try:
            session = SessionManager.get_default_session()
            logger.info("Starting graph materialization...")
            session.model = rfm.KumoRFM(session.graph, verbose=False)
            logger.info("KumoRFM model created successfully")
            return dict(
                success=True,
                message="Successfully finalized graph",
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to finalize graph. {e}",
            )

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
        # TODO(@blaz): Add links to relevant resources

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
    async def predict(query: str) -> Dict[str, Any]:
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
        # TODO(@blaz): Add links to relevant resources

        Args:
            query: The predictive query to validate (e.g.,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
            {
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

            logger.info(f"Running prediction for query: {query}")
            result_df = session.model.predict(query, verbose=False)
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
    async def evaluate(query: str) -> Dict[str, Any]:
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
        # TODO(@blaz): Add links to relevant query syntax resources
        # TODO(@blaz): Add links to relevant evaluation metrics resources

        Args:
            query: The predictive query to validate (e.g.,
            ``"PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1"``)

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
            {
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

            logger.info(f"Running evaluation for query: {query}")
            result_df = session.model.evaluate(query, verbose=False)
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
