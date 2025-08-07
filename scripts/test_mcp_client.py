#!/usr/bin/env python3
"""
Quick test client for KumoRFM MCP server
Run this to test your server locally during development
"""

import asyncio
import json
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_kumo_mcp():
    """Test the KumoRFM MCP server"""
    
    # Start the MCP server
    server_params = StdioServerParameters(
        command="python", 
        args=["-m", "kumo_rfm_mcp.server"]
    )
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the session
            await session.initialize()
            
            print("Connected to KumoRFM MCP Server!")
            print()
            
            # List available tools
            tools = await session.list_tools()
            print("Available Tools:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
            print()
            
            # Test creating a table (will show "not implemented" message)
            print("Testing kumo_create_table...")
            result = await session.call_tool("kumo_create_table", {
                "file_path": "/tmp/test.csv",
                "table_name": "test_table"
            })
            print(f"Result: {result.content[0].text}")
            print()
            
            # Test with different file types
            print("Testing kumo_create_table with parquet file...")
            result = await session.call_tool("kumo_create_table", {
                "file_path": "/tmp/data.parquet",
                "table_name": "parquet_table"
            })
            print(f"Result: {result.content[0].text}")
            print()
            
            print("Basic MCP server test completed!")


if __name__ == "__main__":
    asyncio.run(test_kumo_mcp())