"""Documentation tools for KumoRFM MCP server."""

import logging
from typing import Any, Dict

from fastmcp import FastMCP

logger = logging.getLogger('kumo-rfm-mcp')


def register_docs_tools(mcp: FastMCP):
    """Register all documentation tools with the MCP server."""

    @mcp.tool()
    async def get_docs(resource_uri: str) -> Dict[str, Any]:
        """Get documentation by URI.

        Retrieves content from any kumo:// resource URI. This
        provides programmatic access to all documentation, examples,
        and guides available in the system.

        Args:
            resource_uri: The kumo:// URI for the resource
                    Examples:
                    - "kumo://docs/pql-guide" - PQL grammar reference
                    - "kumo://docs/pql-reference" - PQL operators guide
                    - "kumo://docs/table-guide" - Table setup guide
                    - "kumo://docs/graph-guide" - FK patterns
                    - "kumo://docs/data-guide" - Data prep guide
                    - "kumo://examples/e-commerce" - E-commerce examples
                    - "kumo://docs/overview" - System overview

        Returns:
            Dictionary containing:
            - success (bool): True if resource found and retrieved
            - message (str): Human-readable status message
            - content (str, optional): The resource content on success

        Examples:
            get_docs("kumo://docs/pql-guide")
            get_docs("kumo://examples/e-commerce")
        """
        try:
            # Get the resource from the MCP server
            resources = await mcp.get_resources()

            if resource_uri not in resources:
                available_uris = list(resources.keys())
                return dict(success=False,
                            message=f"Resource not found: {resource_uri}",
                            available_resources=available_uris)

            # Call the resource function to get content
            resource = resources[resource_uri]
            if hasattr(resource, 'fn') and callable(resource.fn):
                content = await resource.fn()

                return dict(success=True,
                            message=f"Successfully retrieved: {resource_uri}",
                            content=content)
            else:
                return dict(success=False,
                            message=f"Resource not callable: {resource_uri}")

        except Exception as e:
            logger.error(f"Failed to get resource {resource_uri}: {e}")
            return dict(success=False,
                        message=f"Error retrieving resource: {str(e)}")
