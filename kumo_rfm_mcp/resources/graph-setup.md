# Graph Setup

This guide outlines the data requirements and best practices for setting up graphs from relational data in KumoRFM.

KumoRFM operates on relational data organized as inter-connected tables forming a graph structure. The foundation of this process starts with a set of CSV or Parquet files, which are registered as table schemas and assembled into a graph schema.

## Table Schema

A table schema is defined by three concepts:

- **Semantic types (`stypes`):** Semantic types denote the semantic meaning of columns in a table and how they should be processed within the model
- **Primary key (`primary_key`):** A unique identifier for the table
- **Time column (`time_column`):** The column that denotes the create time of rows (marking when the row became active)
- **End time column (`end_time_column`):** The column that denotes the end time of rows (marking when the row stopped being active)

### Semantic Types

The semantic type of a column will determine how it will be encoded downstream.
Correctly setting each column's semantic type is critical for model performance.
For instance, for missing value imputation queries, the semantic type determines whether the task is treated as regression (`stype="numerical"`) or as classification (`stype="categorical"`).

The following semantic types are available:

| `stype`              | Explanation                                       | Supported data types                           | Example                                                                   |
| -------------------- | ------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------- |
| `"numerical"`        | Numerical values (e.g., `price`, `age`)           | `int`, `float`                                 | `25`, `3.14`, `-10`                                                       |
| `"categorical"`      | Discrete categories with limited cardinality      | `int`, `float`, `string`                       | Color: `"red"`, `"blue"`, `"green"` (one cell may only have one category) |
| `"multicategorical"` | Multiple categories in a single cell              | `string`, `stringlist`, `intlist`, `floatlist` | `"Action\|Drama\|Comedy"`, `["Action", "Drama", "Comedy"]`                |
| `"ID"`               | An identifier, e.g., primary keys or foreign keys | `int`, `float`, `string`                       | `123`, `PRD-8729453`                                                      |
| `"text"`             | Natural language text                             | `string`                                       | Descriptions of products                                                  |
| `"timestamp"`        | Specific point in time                            | `date`, `string`                               | `"2025-07-11"`, `"2023-02-12 09:47:58"`                                   |
| `"sequence"`         | Custom embeddings or sequential data              | `floatlist`, `intlist`                         | `[0.25, -0.75, 0.50, ...]`                                                |

Upon table registration, semantic types of columns are estimated based on simple heuristics (e.g., data types, cardinality), but may not be ideal.
For example, low cardinality columns may be mistakenly treated as `"categorical"` rather than `"numerical"`.
You can use your world knowledge and common sense to analyze and correct such mistakes.

If certain columns should be discarded, e.g., in case they have such high cardinality to make proper model generalization infeasible, a semantic type of `None` can be used to discard the column from being encoded.

### Primary Key

The primary key is a unique identifier of each row in a table.
Each table can have at most one primary key.
If there are duplicated primary keys, the system will only keep the first one.
A primary key can be used later to link tables through foreign key-primary key relationships.
However, a primary key does not need to necessarily link to other tables.
Setting a primary key will automatically assing the semantic type `"ID"` to this column.
A primary key may not exist for all tables, but will be required whenever tables need to be linked together or whenever the table is used as the entity in a predictive query.

### Time Column

A time column specifies the timestamp at which an event occured or when this row became active.
It is used to prevent temporal leakage during subgraph sampling, i.e. for a given anchor time only events are preserved with timestamp less than or equal to the given anchor time.
Time column data must obey to datetime format to be correctly parsed by `pandas.to_datetime`.
Each table can have at most one time column.
A time column may not exist for all tables, but will be required when predicting future aggregates over fact tables, e.g., the count of all orders in the next seven days.
The system will only keep rows with non-N/A timestamps.
In case there exists multiple time columns in the table, pick the column as time column that most likely refers to the create time of the event.
For example, `create_time` should be preferred over `update_time`.

### End Time Column

An end time column specifies the timestamp at which an event or row stopped being active.
It is used to exclude in-context examples that have already expired relative to a given anchor time.
End time column data must obey to datetime format to be correctly parsed by `pandas.to_datetime`.
Each table can have at most one end time column.
If both a time column and an end time column are present, they must refer to different columns in the dataset.
An end time column is optional and typically only appears alongside a time column.

## Graph Schema

Links between tables are defined via foreign key-primary key relationships, describing many-to-one relations.
Links are the crucial bit that transform individual tables into a connected relational structure, enabling KumoRFM to understand and leverage relationships in your data.
However, it is also possible to use KumoRFM in single table settings or within multiple disjoint graph schemas registered within the same graph.

A link is defined by a source table (`source_table`), the foreign key column in the source table (`foreign_key`), and a destination table (`destination_table`) holding a primary key.
For example, the `orders` source table may hold a foreign key `user_id` to link to the destination table `users`, holding a unique identifier for each user.
Often times, links can be naturally inferred by inspecting the table schemas and relying on name matching to find meaningful connections.
However, this may not always be the case.
You can use your world knowledge and common sense to analyze meaningful connections between tables.

Note that KumoRFM only supports foreign key-primary key links.
In order to connect primary keys to primary keys, you have to remove the primary key in one of the tables.

**Important:** Make sure that tables are correctly linked before proceeding.

## Graph Updates

You can use the `update_graph_metadata` tool to perform partial graph schema updates by registering new tables, changing their semantic types, and linking them together.
Note that all operations can be performed in a batch at once, e.g., one can add new tables and directly link them to together.

## Graph Visualization

You can visualize the graph at any given point in time by rendering it as a Mermaid entity relationship diagram via the `get_mermaid` tool.
Based on the number of columns in each table, it is recommended to set `show_columns` to `False` to avoid cluttering the diagram with less relevant details.

## Graph Materialization

Once a graph is set up, you can materialize the graph to make it ready for model inference operations via the `materialize_graph` tool.
This step creates the relational entity graph, which converts each row into a node, and each primary-foreign ky link into an edge.
Most importantly, it converts the relational data into a data structure from which it can efficiently perform graph traversal and sample subgraphs, which are later used as inputs into the model.

Any updates to the graph schema will require re-materializing the graph before the KumoRFM model can start making predictions again.
