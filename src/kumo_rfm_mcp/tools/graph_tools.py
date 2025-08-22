import logging
from typing import Any, Dict

from fastmcp import FastMCP

logger = logging.getLogger('kumo-rfm-mcp.graph_tools')


def register_graph_tools(mcp: FastMCP):
    """Register all graph management tools with the MCP server."""

    @mcp.tool()
    async def suggest_links() -> Dict[str, Any]:
        """
        This tool suggests links between tables based on the current metadata
        state.
        """
        try:
            raise NotImplementedError("Link inference is not yet implemented")
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to suggest links. {e}",
            )

    @mcp.tool()
    async def visualize_graph() -> Dict[str, Any]:
        """Visualizes the graph as a mermaid diagram.
        """
        try:
            raise NotImplementedError(
                "Graph visualization is not yet implemented")
        except Exception as e:
            return dict(
                success=False,
                message=f"Failed to visualize graph. {e}",
            )
