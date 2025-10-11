<div align="center">
  <img src="https://kumo-ai.github.io/kumo-sdk/docs/_static/kumo-logo.svg" height="40"/>
  <h1>KumoRFM MCP Server</h1>
</div>

<div align="center">
  <p>
    <a href="https://kumorfm.ai">KumoRFM</a> •
    <a href="https://github.com/kumo-ai/kumo-rfm/">Notebooks</a> •
    <a href="https://kumo.ai/company/news/kumorfm-mcp/">Blog</a> •
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
        "KUMO_API_KEY": "your_api_key_here"
      }
    }
  }
}
```

### ⚡ MCP Bundle

We provide a single-click installation via our [MCP Bundle (MCPB)](https://github.com/anthropics/mcpb) (*e.g.*, for integration into Claude Desktop):

1. Download the `dxt` file from [here](https://kumo-sdk-public.s3.us-west-2.amazonaws.com/dxt/kumo-rfm-mcp-0.1.0.dxt)
1. Double click to install

<img src="https://kumo-sdk-public.s3.us-west-2.amazonaws.com/claude_desktop.png" />

The MCP Bundle supports Linux, macOS and Windows, but requires a Python executable to be found in order to create a separate new virtual environment.

## 🎬 Claude Desktop Demo

See [here](https://claude.ai/share/d2a34e63-b1d2-4255-b3e9-a6cb55004497) for the transcript.

https://github.com/user-attachments/assets/56192b0b-d9df-425f-9c10-8517c754420f

## 🔬 Agentic Workflows

You can use the KumoRFM MCP directly in your agentic workflows:

<table>
  <tr>
    <th align="center">
      <a href="https://docs.crewai.com/en/mcp/overview">
        <img src="https://cdn.prod.website-files.com/66cf2bfc3ed15b02da0ca770/66d07240057721394308addd_Logo%20(1).svg" width="150" />
      </a>
      <br/>
      [<a href="https://github.com/kumo-ai/kumo-rfm/blob/master/notebooks/ecom_agent.ipynb">Example</a>]
    </th>
    <td valign="top"><pre lang="python"><code>
from crewai import Agent
from crewai_tools import MCPServerAdapter
from mcp import StdioServerParameters
<br/>
params = StdioServerParameters(
    command='python',
    args=['-m', 'kumo_rfm_mcp.server'],
    env={'KUMO_API_KEY': ...},
)
<br/>
with MCPServerAdapter(params) as mcp_tools:
    agent = Agent(
        role=...,
        goal=...,
        backstory=...,
        tools=mcp_tools,
    )
</code></pre></td>
  </tr>
  <tr>
    <th align="center">
      <a href="https://langchain-ai.github.io/langgraph/agents/mcp/">
        <picture class="github-only">
          <source media="(prefers-color-scheme: light)" srcset="https://langchain-ai.github.io/langgraph/static/wordmark_dark.svg">
          <source media="(prefers-color-scheme: dark)" srcset="https://langchain-ai.github.io/langgraph/static/wordmark_light.svg">
          <img src="https://langchain-ai.github.io/langgraph/static/wordmark_dark.svg" width="250">
        </picture>
      </a>
      <br/>
      [<a href="https://github.com/kumo-ai/kumo-rfm/blob/master/notebooks/insurance_agent.ipynb">Example</a>]
    </th>
    <td valign="top"><pre lang="python"><code>
from langchain_mcp_adapter.client MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
<br/>
client = MultiServerMCPClient({
    'kumo-rfm': {
        'command': 'python',
        'args': ['-m', 'kumo_rfm_mcp.server'],
        'env': {'KUMO_API_KEY': ...},
    }
})
<br/>
agent = create_react_agent(
    llm=...,
    tools=await client.get_tools(),
)
</code></pre></td>
  </tr>
  <tr>
    <th align="center">
      <a href="https://openai.github.io/openai-agents-python/mcp/">
        <picture class="github-only">
          <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/a28d3311-d676-4b2f-923e-49d59fa00dfa">
          <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/27bde36e-e0cc-4944-93f6-66e432df2180">
          <img src="https://github.com/user-attachments/assets/a28d3311-d676-4b2f-923e-49d59fa00dfa" width="180" />
        </picture>
      </a>
      <br/>
      [<a href="https://github.com/kumo-ai/kumo-rfm/blob/master/notebooks/simple_sales_agent.ipynb">Example</a>]
    </th>
    <td valign="top"><pre lang="python"><code>
from agents import Agent
from agents.mcp import MCPServerStdio
<br/>
async with MCPServerStdio(params={
    'command': 'python',
    'args': ['-m', 'kumo_rfm_mcp.server'],
    'env': {'KUMO_API_KEY': ...},
}) as server:
    agent = Agent(
        name=...,
        instructions=...,
        mcp_servers=[server],
    )
</code></pre></td>
  </tr>
  <tr>
    <th align="center">
      <a href="https://docs.anthropic.com/en/docs/claude-code/sdk/sdk-python/">
        <picture class="github-only">
          <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/b4f8fc8a-6d3f-44ba-9623-3dedb29c6a95">
          <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/4408e2ca-7e4b-4a4c-8bb6-eb00dd486315">
          <img src="https://github.com/user-attachments/assets/b4f8fc8a-6d3f-44ba-9623-3dedb29c6a95" width="180" />
        </picture>
      </a>
    </th>
    <td valign="top"><pre lang="python"><code>
from claude_code_sdk import query, ClaudeCodeOptions
<br/>
mcp_servers = {
    'kumo-rfm': {
        'command': 'python',
        'args': ['-m', 'kumo_rfm_mcp.server'],
        'env': {'KUMO_API_KEY': ...},
    }
}
<br/>
async for message in query(
    prompt=...,
    options=ClaudeCodeOptions(
        system_prompt=...,
        mcp_servers=mcp_servers,
        permission_mode='default',
    ),
):
    ...
</code></pre></td>
  </tr>
</table>

Browse our [examples](https://github.com/kumo-ai/kumo-rfm/tree/master/notebooks) to get started with agentic workflows powered by KumoRFM.

## 📚 Available Tools

### I/O Operations

- **🔍 `find_table_files` - Searching for tabular files:** Find all table-like files (*e.g.*, CSV, Parquet) in a directory.
- **🧐 `inspect_table_files` - Analyzing table structure:** Inspect the first rows of table-like files.

### Graph Management

- **🗂️ `inspect_graph_metadata` - Reviewing graph schema:** Inspect the current graph metadata.
- **🔄 `update_graph_metadata` - Updating graph schema:** Partially update the current graph metadata.
- **🖼️ `get_mermaid` - Creating graph diagram:** Return the graph as a Mermaid entity relationship diagram.
- **🕸️ `materialize_graph` - Assembling graph:** Materialize the graph based on the current state of the graph metadata to make it available for inference operations.
- **📂 `lookup_table_rows` - Retrieving table entries:** Lookup rows in the raw data frame of a table for a list of primary keys.

### Model Execution

- **🤖 `predict` - Running predictive query:** Execute a predictive query and return model predictions.
- **📊 `evaluate` - Evaluating predictive query:** Evaluate a predictive query and return performance metrics which compares predictions against known ground-truth labels from historical examples.
- **🧠 `explain` - Explaining prediction:** Execute a predictive query and explain the model prediction.

## 🔧 Configuration

### Environment Variables

- **`KUMO_API_KEY`:** Authentication is needed once before predicting or evaluating with the
  KumoRFM model.
  You can generate your KumoRFM API key for free [here](https://kumorfm.ai).
  If not set, you can also authenticate on-the-fly in individual session via an OAuth2 flow.

## We love your feedback! :heart:

As you work with KumoRFM, if you encounter any problems or things that are confusing or don't work quite right, please open a new :octocat:[issue](https://github.com/kumo-ai/kumo-rfm-mcp/issues/new).
You can also submit general feedback and suggestions [here](https://docs.google.com/forms/d/e/1FAIpQLSfr2HYgJN8ghaKyvU0PSRkqrGd_BijL3oyQTnTxLrf8AEk-EA/viewform).
Join [our Slack](https://join.slack.com/t/kumoaibuilders/shared_invite/zt-2z9uih3lf-fPM1z2ACZg~oS3ObmiQLKQ)!
