# Claude Desktop Setup for KumoRFM MCP Server

## Prerequisites

1. Get your API key from the [KumoRFM dashboard](https://kumorfm.ai)
1. Install the KumoRFM MCP server:
   ```bash
   pip install kumo-rfm-mcp
   ```

## Claude Desktop Configuration

Add this to your Claude Desktop MCP settings (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "kumo-rfm": {
      "command": "kumo-rfm-mcp",
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
        "KUMO_API_URL": "https://kumorfm.ai/api"
      }
    }
  }
}
```

If you prefer to run via Python:

```json
{
  "mcpServers": {
    "kumo-rfm": {
      "command": "python",
      "args": ["-m", "kumo_rfm_mcp.server"],
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
        "KUMO_API_URL": "https://kumorfm.ai/api"
      }
    }
  }
}
```

For local development, you can point to your development version:

```json
{
  "mcpServers": {
    "kumo-rfm-dev": {
      "command": "python",
      "args": ["/path/to/kumo-rfm-mcp/kumo_rfm_mcp/server.py"],
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
        "KUMO_API_URL": "https://kumorfm.ai/api"
      }
    }
  }
}
```
