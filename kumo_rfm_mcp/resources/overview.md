# Overview of KumoRFM

KumoRFM is a **pre-trained Relational Foundation Model (RFM)** that generates training-free predictions on any relational multi-table data by interpreting the data as a (temporal) heterogeneous graph.
It can be queried via the **Predictive Query Language (PQL)**.

**Internal note:** It is NOT related to "Recency, Frequency, Monetary" analysis.

Internally, KumoRFM utilizes in-context learning to transfer patterns from historical examples to new unseen examples.
Specifically, it constructs training/in-context subgraphs with known ground-truth labels and relates them to unseen subgraphs.

KumoRFM can discover table-like files (e.g., CSV, Parquet), inspect them, and structure them into a graph via foreign key-primary key relationships.
A time column in a table dictates the create time of a row, which is used downstream to receive and order historical interactions and prevent temporal leakage.
Each column within a table is assigned a semantic type (`"numerical"`, `"categorical"`, `"multicategorical"`, `"ID"`, `"text"`, `"timestamp"`, `"sequence"`, etc) that denotes the semantic meaning of the column and how it should be processed within the model.

See the `kumo://docs/graph-setup` resource for more information.

After a graph is set up and materialized, KumoRFM can generate predictions (e.g., missing value imputation, temporal forecasts) and evaluations by querying the graph via the Predictive Query Language (PQL), a declarative language to formulate machine learning tasks.
Understanding PQL and how it maps to a machine learning task is critical to achieve good model predictions.
Besides PQL, various other options exist to tune model output, e.g., optimizing the `run_mode` of the model, specifying how subgraphs are formed via `num_neighbors`, or adjusting the `anchor_time` to denote the point in time for when a prediction should be made.

See the `kumo://docs/predictive-query` resource for more information.

## Getting Started

1. Finding, inspecting and understanding table-like files via `find_table_files` and `inspect_table_files` tools
1. Constructing and updating the graph schema via `update_graph_metadata` by adding/updating and removing tables and their schema, and inter-connecting them via foreign key-primary key relationshsips
1. Visualizing the graph schema as a Mermaid entity relationship diagram via `get_mermaid`
1. Materializing the graph via `materialize_graph` to make it available for inference operations; This step is necessary to efficiently form subgraphs around entities at any given point in time
1. Predicting and evaluating predictive queries on top of the materialized graph via `predict` and `evaluate` to obtain valuable insights for the future

## Quick Access

Use the `get_docs` tool to access any resource:

```
get_docs('kumo://docs/graph-setup')
get_docs('kumo://docs/predictive-query")
```
