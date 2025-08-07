#!/usr/bin/env python3
import asyncio
import os

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_kumo_mcp():
    api_key = os.environ['KUMO_API_KEY']
    server_params = StdioServerParameters(
        command='bash',
        args=['-c', f'KUMO_API_KEY={api_key} python -m kumo_rfm_mcp.server'],
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("Connected to KumoRFM MCP Server!")
            print()

            tools = await session.list_tools()
            print("Available Tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
            print()

            print("Testing `add_table()`:")
            result = await session.call_tool('add_table', {
                'path': '/tmp/users.csv',
                'name': 'users',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("Testing `inspect_table()`:")
            result = await session.call_tool('inspect_table', {
                'name': 'users',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("Testing `list_tables()`:")
            result = await session.call_tool('list_tables', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Basic MCP server test completed!")


if __name__ == '__main__':
    asyncio.run(test_kumo_mcp())
