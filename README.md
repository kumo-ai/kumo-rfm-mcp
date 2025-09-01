<div align="center">
  <img src="https://kumo-ai.github.io/kumo-sdk/docs/_static/kumo-logo.svg" height="40"/>
  <h1>KumoRFM MCP Server</h1>
</div>

<div align="center">
  <p>
    <a href="https://github.com/kumo-ai/kumo-rfm/">SDK</a> •
    <a href="https://kumo.ai/company/news/kumo-relational-foundation-model/">Blog</a> •
    <a href="https://kumo.ai/research/kumo_relational_foundation_model.pdf">Paper</a> •
    <a href="https://kumorfm.ai">Get an API key</a>
  </p>

  [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/kumo-rfm-mcp?color=FC1373)](https://pypi.org/project/kumo-rfm-mcp/)
  [![PyPI Status](https://img.shields.io/pypi/v/kumo-rfm-mcp.svg?color=FC1373)](https://pypi.org/project/kumo-rfm-mcp/)
  [![Slack](https://img.shields.io/badge/slack-join-pink.svg?logo=slack&color=FC1373)](https://join.slack.com/t/kumoaibuilders/shared_invite/zt-2z9uih3lf-fPM1z2ACZg~oS3ObmiQLKQ)

  🔬 MCP server to query [KumoRFM](https://kumorfm.ai) in your agentic flows
</div>

## 📖 Introduction

KumoRFM is a pre-trained *Relational Foundation Model (RFM)* that generates training-free predictions on any relational multi-table data by interpreting the data as a (temporal) heterogeneous graph.
It can be queried via the *Predictive Query Language (PQL)*.

This repository hosts a full-featured *MCP (Model Context Protocol)* server that empowers AI assistants with KumoRFM intelligence.
This server enables:

- 🕸️ Build, manage, and visualize graphs directly from CSV or Parquet files
- 💬 Convert natural language into PQL queries for seamless interaction
- 🤖 Query, analyze, and evaluate predictions from KumoRFM (missing value imputation, temporal forecasting, *etc*) all without any training required

## 🚀 Installation

### 🐍 Traditional MCP Server

The KumoRFM MCP server is available for Python 3.10 and above. To install, simply run:

```bash
pip install kumo-rfm-mcp
```

Add to your MCP configuration file (*e.g.*, Claude Desktop's `mcp_config.json`):

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

### ⚡ MCP Bundle

We provide a single-click installation via [MCP Bundles (MCPB)](https://github.com/anthropics/mcpb) (*e.g.*, for integration into Claude Desktop):

1. Download the `dxt` file from **[here]()**
1. Double click to install

<img height="250" src="https://kumo-sdk-public.s3.us-west-2.amazonaws.com/mcpb.png" />

## 📚 Available Tools

The KumoRFM MCP provides several categories of tools:

### I/O Operations

- **`find_table_files`**: Find all table-like files (*e.g.*, CSV, Parquet) in a directory.
- **`inspect_table_files`**: Inspect the first rows of table-like files.

### Graph Management

- **`inspect_graph_metadata`**: Inspect the current graph metadata.
- **`update_graph_metadata`**: Partially update the current graph metadata.
- **`get_mermaid`**: Return the graph as a Mermaid entity relationship diagram.
- **`materialize_graph`**: Materialize the graph based on the current state of the graph metadata to make it available for inference operations (*e.g.*, `predict` and `evaluate`).

### Model Execution

- **`predict`**: Execute a predictive query and return model predictions.
- **`evaluate`**: Evaluate a predictive query and return performance metrics which compares predictions against known ground-truth labels from historical examples.

## 🔧 Configuration

### Environment Variables

* **`KUMO_API_KEY`**: Authentication is needed once before predicting or evaluating with the
    KumoRFM model.
    You can generate your KumoRFM API key for free [here](https://kumorfm.ai).
    If not set, you can also authenticate on-the-fly in individual session via an OAuth2 flow.

## We love your feedback! :heart:

As you work with KumoRFM, if you encounter any problems or things that are confusing or don't work quite right, please open a new :octocat:[issue](https://github.com/kumo-ai/kumo-rfm-mcp/issues/new).
You can also submit general feedback and suggestions [here](https://docs.google.com/forms/d/e/1FAIpQLSfr2HYgJN8ghaKyvU0PSRkqrGd_BijL3oyQTnTxLrf8AEk-EA/viewform).
Join [our Slack](https://join.slack.com/t/kumoaibuilders/shared_invite/zt-2z9uih3lf-fPM1z2ACZg~oS3ObmiQLKQ)!
