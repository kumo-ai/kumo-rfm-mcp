<p align="center">
  <img height="180" src="https://s3.us-west-1.amazonaws.com/data.kumo.ai/img/kumo_pink_md.svg" />
</p>

______________________________________________________________________

The KumoRFM MCP implements a Model Context Protocol (MCP) server for
interacting with the Kumo machine learning platform's RFM (Relational Foundation Model)
capabilities ([documentation](https://github.com/kumo-ai/kumo-rfm-mcp/)).

## Installation

The KumoRFM MCP is available for Python 3.10 and above. To install, simply run

```
pip install kumo-rfm-mcp
```

## Registration


```json
{
  "mcpServers": {
    "kumo-rfm": {
      "command": "python",
      "args": ["-m", "kumo_rfm_mcp.server"],
      "env": {
        "KUMO_API_KEY": "your_api_key_here",
      }
    }
  }
}
```

## Available tools

The KumoRFM MCP provides several categories of tools for working with relational data and machine learning models:

### Table Management Tools

- **add_table**: Load CSV or Parquet files into the Kumo graph
- **remove_table**: Remove tables from the graph
- **inspect_table**: View table contents and schema information
- **list_tables**: Get a list of all tables in the current graph

### Graph Management Tools

- **infer_links**: Automatically detect relationships between tables based on matching column names
- **inspect_graph**: View the current graph structure, tables, and links
- **link_tables**: Manually create foreign key relationships between tables
- **unlink_tables**: Remove existing relationships between tables

### Model Tools

- **finalize_graph**: Create a KumoRFM model from the current graph state for inference
- **validate_query**: Check if a PQL (Predictive Query Language) query is syntactically correct
- **predict**: Generate predictions using PQL queries on the finalized model
- **evaluate**: Evaluate model performance using PQL queries with ground truth data

### Session Management Tools

- **get_session_status**: Check the current state of the session, graph, and model
- **clear_session**: Reset the session and clear all data

### Documentation Tools

- **get_docs**: Access documentation, guides, and examples via kumo:// URIs

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and [CLAUDE_DESKTOP_SETUP.md](CLAUDE_DESKTOP_SETUP.md) for Claude Desktop integration.
