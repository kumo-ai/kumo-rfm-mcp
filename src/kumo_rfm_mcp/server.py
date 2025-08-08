#!/usr/bin/env python3
import logging
import sys

from fastmcp import FastMCP

import kumo_rfm_mcp
from kumo_rfm_mcp.tools import (register_graph_tools, register_model_tools,
                                register_session_tools, register_table_tools)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr)
logger = logging.getLogger('kumo-rfm-mcp')

mcp = FastMCP(
    name='KumoRFM',
    version=kumo_rfm_mcp.__version__,
)

# register table tools
register_table_tools(mcp)

# register graph tools
register_graph_tools(mcp)

# register model tools
register_model_tools(mcp)

# register session tools
register_session_tools(mcp)

if __name__ == '__main__':
    logger.info("Starting KumoRFM MCP Server...")
    mcp.run(transport='stdio')
