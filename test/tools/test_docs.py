import pytest
from fastmcp import Client

from kumo_rfm_mcp.server import mcp


@pytest.mark.asyncio
async def test_get_docs() -> None:
    async with Client(mcp) as client:
        result = await client.call_tool('get_docs', {
            'resource_uri': 'kumo://docs/overview',
        })
        assert result.content[0].text.startswith('# Overview of KumoRFM\n')

        result = await client.call_tool(
            'get_docs', {
                'resource_uri': 'kumo://docs/graph-setup',
            })
        assert result.content[0].text.startswith('# Graph Setup\n')

        result = await client.call_tool(
            'get_docs', {
                'resource_uri': 'kumo://docs/predictive-query',
            })
        assert result.content[0].text.startswith('# Predictive Query\n')
