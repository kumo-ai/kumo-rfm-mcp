# Claude Desktop Setup for KumoRFM MCP Server

## Prerequisites

1. Get your API key from KumoRFM dashboard
2. Install the KumoRFM MCP server:
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

### Alternative: Python Module

If you prefer to run via Python:

```json
{
  "mcpServers": {
    "kumo-rfm": {
      "command": "python",
      "args": ["-m", "kumo_rfm_mcp.server"],
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
        "KUMO_API_URL": "https://api.kumo.ai"
      }
    }
  }
}
```

### Development Setup

For local development, you can point to your development version:

```json
{
  "mcpServers": {
    "kumo-rfm-dev": {
      "command": "python",
      "args": ["/path/to/kumo-rfm-mcp/src/kumo_rfm_mcp/server.py"],
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
        "KUMO_API_URL": "https://api.kumo.ai"
      }
    }
  }
}
```

## Available Tools

Once configured, Claude will have access to these KumoRFM tools:

### Authentication
- `kumo_init` - Manual authentication (optional if env vars are set)

### Table Management
- `kumo_create_table` - Create tables from data files
- `kumo_list_tables` - List all tables
- `kumo_inspect_table` - View table metadata
- `kumo_remove_table` - Remove tables

### Graph Operations
- `kumo_finalize_graph` - Create KumoRFM graph
- `kumo_infer_links` - Auto-infer relationships
- `kumo_add_table_link` - Add foreign key relationships
- `kumo_remove_table_link` - Remove relationships
- `kumo_inspect_graph` - View graph structure

### Querying
- `kumo_validate_query` - Validate PQL syntax
- `kumo_predict` - Execute predictions
- `kumo_evaluate` - Execute evaluations

### Session Management
- `kumo_get_session_status` - View session state
- `kumo_clear_session` - Reset session

## Troubleshooting

### Server Not Starting
- Check that your API key is valid
- Ensure `kumo-rfm-mcp` is in your PATH
- Check Claude Desktop logs for error messages

### Authentication Issues
- Verify `KUMO_API_KEY` is set correctly
- Check that the API URL is accessible
- Use `kumo_get_session_status` to check authentication state

### Testing the Setup
1. Restart Claude Desktop after config changes
2. Ask Claude: "What KumoRFM tools do you have available?"
3. Test with: "Show me my current KumoRFM session status"