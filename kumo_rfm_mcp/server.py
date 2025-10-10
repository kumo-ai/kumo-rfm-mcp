#!/usr/bin/env python3
import logging
import sys
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.resources import FileResource
from pydantic import AnyUrl

import kumo_rfm_mcp
from kumo_rfm_mcp import tools

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] - %(asctime)s - %(name)s - %(message)s',
    stream=sys.stderr)
logger = logging.getLogger('kumo-rfm-mcp')

mcp = FastMCP(
    name='KumoRFM (Relational Foundation Model)',
    instructions=("KumoRFM is a pre-trained Relational Foundation Model (RFM) "
                  "that generates training-free predictions on any relational "
                  "multi-table data by interpreting the data as a (temporal) "
                  "heterogeneous graph. It can be queried via the Predictive "
                  "Query Language (PQL)."),
    version=kumo_rfm_mcp.__version__,
)

# Tools ######################################################################
tools.register_docs_tools(mcp)
tools.register_auth_tools(mcp)
tools.register_io_tools(mcp)
tools.register_graph_tools(mcp)
tools.register_model_tools(mcp)

# Resources ##################################################################
mcp.add_resource(
    FileResource(
        uri=AnyUrl('kumo://docs/overview'),
        path=Path(__file__).parent / 'resources' / 'overview.md',
        name="Overview of KumoRFM",
        description="Overview of KumoRFM (Relational Foundation Model)",
        mime_type='text/markdown',
        tags={'documentation'},
    ))
mcp.add_resource(
    FileResource(
        uri=AnyUrl('kumo://docs/graph-setup'),
        path=Path(__file__).parent / 'resources' / 'graph-setup.md',
        name="Graph Setup",
        description="How to set up graphs in KumoRFM",
        mime_type='text/markdown',
        tags={'documentation'},
    ))
mcp.add_resource(
    FileResource(
        uri=AnyUrl('kumo://docs/predictive-query'),
        path=Path(__file__).parent / 'resources' / 'predictive-query.md',
        name="Predictive Query",
        description="How to query and generate predictions in KumoRFM",
        mime_type='text/markdown',
        tags={'documentation'},
    ))
mcp.add_resource(
    FileResource(
        uri=AnyUrl('kumo://docs/explainability'),
        path=Path(__file__).parent / 'resources' / 'explainability.md',
        name="Explainability",
        description="How to interpret and summarize explanations of KumoRFM",
        mime_type='text/markdown',
        tags={'documentation'},
    ))


def main() -> None:
    """Main entry point for the CLI command."""
    try:
        mcp.run(transport='stdio')
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start KumoRFM MCP server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
