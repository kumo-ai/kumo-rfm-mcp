"""Session management tools for KumoRFM MCP server."""

import logging
from typing import Any, Dict

from fastmcp import FastMCP
from kumoai.experimental import rfm

from kumo_rfm_mcp import SessionManager

logger = logging.getLogger('kumo-rfm-mcp')


def register_session_tools(mcp: FastMCP):
    """Register all session management tools with the MCP server."""

    @mcp.tool()
    async def get_session_status() -> Dict[str, Any]:
        """Gets the current session status including graph state
        and KumoRFM model status.

        KumoRFM is a foundational relational model that can be used to
        generate predictions from relational data without training. The model
        uses a graph representation of the relational data and a PQL query (
        Predictive Query Language) to generate predictions.

        The session status includes:
        - initialized: whether the session has been initialized
        - table_names: list of table names in the graph
        - num_links: number of links in the graph
        - is_rfm_model_ready: whether the KumoRFM model is ready

        A graph needs to be finalized before the KumoRFM model can start
        generating predictions.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message
            - data (dict, optional): Additional information on success

        Examples:
            {
                "success": true,
                "message": "Session status retrieved successfully",
                "data": {
                    "initialized": true,
                    "table_names": ["users", "orders", "items"],
                    "num_links": 2,
                    "is_rfm_model_ready": true
                }
            }
        """
        try:
            session = SessionManager.get_default_session()

            return dict(
                success=True,
                message="Session status retrieved successfully",
                data=dict(
                    initialized=session.initialized,
                    table_names=list(session.graph.tables.keys()),
                    num_links=len(session.graph.edges),
                    is_rfm_model_ready=session.model is not None,
                ),
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to get session status. {e}",
            )

    @mcp.tool()
    async def clear_session() -> Dict[str, Any]:
        """Clears the current session by removing all tables, links, and the
        KumoRFM model.

        This operation resets the session to its initial state, allowing you to
        start fresh with new data and graph configuration. This operation will
        reset the graph and the KumoRFM model to an empty state. New tables
        and links will need to be added, and the graph will need to be
        finalized before the KumoRFM model can start generating predictions.

        Returns:
            Dictionary containing:
            - success (bool): ``True`` if operation succeeded
            - message (str): Human-readable status message

        Examples:
            {
                "success": true,
                "message": "Session cleared successfully",
            }
        """
        try:
            session = SessionManager.get_default_session()
            session.graph = rfm.LocalGraph(tables=[])
            session.model = None

            return dict(
                success=True,
                message="Session cleared successfully",
            )
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to clear session. {e}",
            )
