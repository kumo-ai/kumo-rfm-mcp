from fastmcp import FastMCP


def register_overview_resources(mcp: FastMCP):
    """Register all overview resources with the MCP server."""

    @mcp.resource("kumo://docs/overview")
    async def get_overview() -> str:
        """Overview of KumoRFM MCP server."""
        return """# KumoRFM MCP Server Overview

## What is KumoRFM?
KumoRFM is a foundational relational model (RFM) that can be used to generate
predictions from relational data without training (do not confuse with "Recency, Frequency, Monetary (RFM) analysis").
The model uses a graph representation of the relational data and a PQL query (Predictive Query Language) to generate predictions.

## Available Documentation

### PQL Reference
- **kumo://docs/pql-guide** - Complete PQL grammar reference
- **kumo://docs/pql-reference** - All keywords and operators in PQL

### Data Preparation
- **kumo://docs/table-guide** - Table
- **kumo://docs/graph-guide** - Foreign key patterns and many-to-many handling
- **kumo://docs/data-guide** - CSV format requirements and data quality checks

### Examples
- **kumo://examples/e-commerce** - E-commerce predictive query examples

## Getting Started
1. Add tables to the graph using `add_table`
2. Link tables using `infer_links` or `add_link`
3. Finalize the graph using `finalize_graph`
4. Generate predictions using `predict` or `evaluate`

## Quick Access
Use the `get_docs` tool to access any resource:
```
get_docs("kumo://docs/pql-syntax")
get_docs("kumo://examples/e-commerce")
"""  # noqa: E501
