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

            # List available tools
            tools = await session.list_tools()
            print("Available Tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
            print()

            # List available resources
            print("=== Available Resources ===")
            resources = await session.list_resources()
            print(f"Found {len(resources.resources)} resources:")
            for resource in resources.resources:
                print(f"  - URI: {resource.uri}")
                if resource.name:
                    print(f"    Name: {resource.name}")
                if resource.description:
                    # Show first line of description
                    desc_first_line = resource.description.split('\n')[0]
                    print(f"    Description: {desc_first_line}")
                print()

            # Test reading a few key resources
            print("=== Testing Resource Content ===")
            test_resource_uris = [
                "kumo://docs/overview", "kumo://docs/pql-guide",
                "kumo://examples/e-commerce"
            ]

            for uri in test_resource_uris:
                try:
                    print(f"Reading resource: {uri}")
                    content = await session.read_resource(uri)
                    if content.contents:
                        text_content = content.contents[0].text
                        lines = text_content.split('\n')
                        print(f"  ✅ Success! Content has {len(lines)} lines")
                        print(f"  📄 First line: {lines[0]}")
                    else:
                        print("  ⚠️  No content returned")
                except Exception as e:
                    print(f"  ❌ Error reading resource: {e}")
                print()
            print()

            print("=== Adding Tables ===")

            print("Adding users table:")
            result = await session.call_tool('add_table', {
                'path': 'tmp/users.csv',
                'name': 'users',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("Adding orders table:")
            result = await session.call_tool('add_table', {
                'path': 'tmp/orders.csv',
                'name': 'orders',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("Adding items table:")
            result = await session.call_tool('add_table', {
                'path': 'tmp/items.csv',
                'name': 'items',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Inspecting Tables ===")

            print("Inspecting users table:")
            result = await session.call_tool('inspect_table', {
                'name': 'users',
            })
            print(f"Result: {result.content[0].text}")
            print()

            print("Listing all tables:")
            result = await session.call_tool('list_tables', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Inferring Links ===")

            print("Running automatic link inference:")
            result = await session.call_tool('infer_links', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Inspecting Graph ===")

            print("Inspecting complete graph structure:")
            result = await session.call_tool('inspect_graph', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Testing Unlink/Link ===")

            print("Unlinking orders and users:")
            result = await session.call_tool(
                'unlink_tables', {
                    'source_table': 'orders',
                    'foreign_key': 'user_id',
                    'destination_table': 'users',
                })
            print(f"Result: {result.content[0].text}")
            print()

            print("Inspecting graph after unlinking:")
            result = await session.call_tool('inspect_graph', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Re-linking orders and users:")
            result = await session.call_tool(
                'link_tables', {
                    'source_table': 'orders',
                    'foreign_key': 'user_id',
                    'destination_table': 'users',
                })
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Final Graph Operations ===")

            print("Final graph inspection:")
            result = await session.call_tool('inspect_graph', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Finalizing graph and creating KumoRFM model:")
            result = await session.call_tool('finalize_graph', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Inference Testing ===")

            test_query = ("PREDICT COUNT(orders.*, 0, 30, days)>0 "
                          "FOR users.user_id=1")

            print("Validating query:")
            result = await session.call_tool(
                'validate_query',
                {'query': test_query},
            )
            print(f"Result: {result.content[0].text}")
            print()

            print("Running prediction:")
            result = await session.call_tool(
                'predict',
                {'query': test_query},
            )
            print(f"Result: {result.content[0].text}")
            print()

            print("Running evaluation:")
            result = await session.call_tool(
                'evaluate',
                {'query': test_query},
            )
            print(f"Result: {result.content[0].text}")
            print()

            print("=== Session Management ===")

            print("Getting session status:")
            result = await session.call_tool('get_session_status', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Clearing session:")
            result = await session.call_tool('clear_session', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Checking session status after clearing:")
            result = await session.call_tool('get_session_status', {})
            print(f"Result: {result.content[0].text}")
            print()

            print("Comprehensive MCP server test completed!")


if __name__ == '__main__':
    asyncio.run(test_kumo_mcp())
