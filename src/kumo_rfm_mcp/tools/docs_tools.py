import logging
from typing import Any, Dict

from fastmcp import FastMCP

logger = logging.getLogger('kumo-rfm-mcp.docs_tools')


def register_docs_tools(mcp: FastMCP):
    """Register all documentation tools with the MCP server."""

    @mcp.tool()
    async def get_docs(resource_uri: str) -> Dict[str, Any]:
        """Gets documentation by URI.

        Retrieves content from any kumo:// resource URI. This
        provides programmatic access to all documentation, examples,
        and guides available in the system.

        Args:
            resource_uri: The kumo:// URI for the resource.
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
            logger.info(f"Getting documentation for resource: {resource_uri}")
            resources = await mcp.get_resources()
            logger.info(f"Found {len(resources)} available resources")

            if resource_uri not in resources:
                logger.warning(f"Resource '{resource_uri}' not found. "
                               f"Available: {list(resources.keys())}")
                return dict(
                    success=False,
                    message=f"Resource '{resource_uri}' not found",
                    available_resources=list(resources.keys()),
                )

            resource = resources[resource_uri]

            # Call the resource function to get content:
            if hasattr(resource, 'fn') and callable(resource.fn):
                logger.info(f"Retrieving content for '{resource_uri}'")
                content = await resource.fn()
                logger.info(f"Successfully retrieved {len(str(content))} "
                            f"characters of content for '{resource_uri}'")

                return dict(
                    success=True,
                    message=f"Successfully retrieved '{resource_uri}'",
                    content=content,
                )
            else:
                logger.error(f"Resource '{resource_uri}' is not callable")
                return dict(
                    success=False,
                    message=f"Resource '{resource_uri}' not callable",
                )

        except Exception as e:
            logger.error(f"Failed to get docs for '{resource_uri}': {e}")
            return dict(
                success=False,
                message=f"Error retrieving resource: {e}",
            )
