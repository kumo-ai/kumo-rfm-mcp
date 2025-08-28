#!/usr/bin/env python3
import logging
import sys

from fastmcp import FastMCP

import kumo_rfm_mcp
from kumo_rfm_mcp import resources, tools

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] - %(asctime)s - %(name)s - %(message)s',
    stream=sys.stderr)
logger = logging.getLogger('kumo-rfm-mcp')

mcp = FastMCP(
    name='KumoRFM',
    version=kumo_rfm_mcp.__version__,
)

# Tools ######################################################################
tools.register_docs_tools(mcp)
tools.register_io_tools(mcp)
tools.register_graph_tools(mcp)
tools.register_model_tools(mcp)

# Resources ##################################################################
resources.register_overview_resources(mcp)
resources.register_docs_resources(mcp)
resources.register_examples_resources(mcp)


def main():
    """Main entry point for the CLI command."""
    try:
        logger.info("Starting KumoRFM MCP Server...")
        mcp.run(transport='stdio')
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start KumoRFM MCP Server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
