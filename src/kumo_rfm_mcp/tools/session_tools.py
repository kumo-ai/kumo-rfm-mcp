from fastmcp import FastMCP

from kumo_rfm_mcp import GraphMetadata, SessionManager
from kumo_rfm_mcp.tools.graph_tools import inspect_graph_metadata

# def clear_session() -> GraphMetadata:
#     """Clear the current session by removing all tables, links, and the
#     KumoRFM model.

#     Returns:
#         The empty graph metadata.
#     """
#     session = SessionManager.get_default_session()
#     session.clear()
#     return inspect_graph_metadata()


def register_session_tools(mcp: FastMCP) -> None:
    """Register all session management tools with the MCP server."""
    pass
    # mcp.tool()(clear_session)
