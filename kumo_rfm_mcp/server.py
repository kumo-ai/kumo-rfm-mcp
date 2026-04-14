#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Final

from fastmcp import FastMCP
from fastmcp.resources import FileResource
from pydantic import AnyUrl

import kumo_rfm_mcp
from kumo_rfm_mcp import tools
from kumo_rfm_mcp.http_auth import get_http_auth

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] - %(asctime)s - %(name)s - %(message)s',
    stream=sys.stderr)
logger = logging.getLogger('kumo-rfm-mcp')

DEFAULT_HTTP_HOST: Final[str] = '127.0.0.1'
DEFAULT_HTTP_PORT: Final[int] = 8000
DEFAULT_HTTP_PATH: Final[str] = '/mcp'


def create_mcp() -> FastMCP:
    return FastMCP(
        name='KumoRFM (Relational Foundation Model)',
        instructions=("KumoRFM is a pre-trained Relational Foundation Model "
                      "(RFM) that generates training-free predictions on any "
                      "relational multi-table data by interpreting the data "
                      "as a (temporal) heterogeneous graph. It can be queried "
                      "via the Predictive Query Language (PQL)."),
        version=kumo_rfm_mcp.__version__,
        auth=get_http_auth(),
    )


mcp = create_mcp()

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
        parser = argparse.ArgumentParser(
            description='Run the KumoRFM MCP server')
        parser.add_argument(
            '--transport',
            choices=('stdio', 'http', 'streamable-http', 'sse'),
            default=os.getenv('KUMO_MCP_TRANSPORT', 'stdio'),
            help='MCP transport to expose. Use streamable-http for Snowflake.',
        )
        parser.add_argument(
            '--host',
            default=os.getenv('KUMO_MCP_HOST', DEFAULT_HTTP_HOST),
            help='HTTP bind host for HTTP transports.',
        )
        parser.add_argument(
            '--port',
            type=int,
            default=int(os.getenv('KUMO_MCP_PORT', str(DEFAULT_HTTP_PORT))),
            help='HTTP bind port for HTTP transports.',
        )
        parser.add_argument(
            '--path',
            default=os.getenv('KUMO_MCP_PATH', DEFAULT_HTTP_PATH),
            help='HTTP endpoint path for HTTP transports.',
        )
        args = parser.parse_args()

        transport = args.transport
        if transport == 'stdio':
            mcp.run(transport='stdio')
            return

        mcp.run(
            transport=transport,
            host=args.host,
            port=args.port,
            path=args.path,
        )
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start KumoRFM MCP server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
