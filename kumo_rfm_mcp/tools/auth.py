import asyncio
import os
from typing import Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from kumoai.experimental import rfm

from kumo_rfm_mcp import SessionManager


async def authenticate(
) -> Literal["KumoRFM session successfully authenticated"]:
    """Authenticate the current KumoRFM session.

    Authentication is needed once before predicting or evaluating with the
    KumoRFM model. If the 'KUMO_API_KEY' environment variable is not set,
    initiates an OAuth2 authentication flow by opening a browser window for
    user login. Sets the 'KUMO_API_KEY' environment variable upon successful
    authentication.
    """
    session = SessionManager.get_default_session()

    if session.is_initialized:
        raise ToolError("KumoRFM session is already authenticated")

    if os.getenv('KUMO_API_KEY') in {None, '', '${user_config.KUMO_API_KEY}'}:
        try:
            await asyncio.to_thread(rfm.authenticate)
        except Exception as e:
            raise ToolError(
                f"Failed to authenticate KumoRFM session: {e}") from e

    session.initialize()
    return "KumoRFM session successfully authenticated"


def register_auth_tools(mcp: FastMCP) -> None:
    """Register all authentication tools to the MCP server."""
    mcp.tool(annotations=dict(
        title="🔑 Signing in to KumoRFM…",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    ))(authenticate)
