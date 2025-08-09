"""Documentation resources for KumoRFM MCP server."""

import logging

from fastmcp import FastMCP

logger = logging.getLogger('kumo-rfm-mcp')


def register_docs_resources(mcp: FastMCP):
    """Register all documentation resources with the MCP server."""

    @mcp.resource("kumo://docs/pql-guide")
    async def get_pql_guide() -> str:
        """Complete PQL (Predictive Query Language) guide.

        Returns the complete syntax specification for PQL, including:
        - Understanding PQL
        - Basic query structure
            - How target expression works
            - How entity specification works
            - How filters work
        - Task Types
        - Static vs Temporal Tasks
        - Full reference of all PQL keywords and operators

        This resource provides the foundational grammar rules needed to write
        valid PQL queries for KumoRFM predictions.
        """
        return """# PQL (Predictive Query Language) Complete Grammar Reference

## Understanding PQL
A Predictive Query is a declarative syntax used to define a predictive modeling task in Kumo. It specifies the target variable to predict and the data context for training.

## Basic Query Structure
The basic structure of a Predictive Query is:
```{pql}
PREDICT <target_expression> FOR <entity_specification> WHERE <optional_filters>
```
- `target_expression`: Declares the value or aggregate the model should predict
- `entity_specification`: Specifies the single ID or list of IDs to predict for
- `optional_filters`: Filters which entities are included in the context of the prediction

## How target expression works
The target expression is the value or aggregate the model should predict. It can be a single value, an aggregate, or a combination of both.
The target expression can be a single value (column name) or an aggregation
All components of the target expression must be fully qualified by table name and column name. E.g. `transactions.is_fraudulent` or `COUNT(orders.*, 0, 30, days) > 0`, or `users.is_active`, etc.
If the target is a single value, it has to appear in the same table as the entity we are predicting for. For example, if we're predicting if a transaction is fraudulent, the target should be part of the transactions table.
```{pql}
PREDICT transactions.is_fraudulent FOR transactions.transaction_id=1
```

If the target is an aggregation, we have to aggregate over a column that appears in a table which is **connected** to the entity we are predicting for. For example, if we're predicting the average order value for a user, we can write:
```{pql}
PREDICT AVG(orders.amount, 0, 30, days) FOR users.user_id=1
```
Where `orders` is a table that is connected to `users` through a foreign key (i.e. `users.user_id = orders.user_id`).

Many aggregation operations are supported, e.g. `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, but all target expressions with aggregations need to happen over a table that is connected to the entity we are predicting for.

IMPORTANT: Only numerical and categorical columns are valid target columns, except for LIST_DISTINCT() aggregation, where only foreign key targets are supported.

## How entity specification works
entities for each query can be specified in one of two ways:
- By specifying a single entity id
```{pql}
PREDICT transactions.is_fraudulent FOR transactions.transaction_id=1
```
- By specifying a tuple of entity ids
```{pql}
PREDICT MAX(orders.amount, 0, 7, hours) FOR users.user_id IN (1, 2, 3)
```

## How filters work
KumoRFM makes its entity-specific predictions based on context examples, collected from the database. Entity filters can be used
to provide more control over KumoRFM context examples. For example, to exclude users without recent activity from the context, we can write:
```{pql}
PREDICT COUNT(orders.*, 0, 30, days) > 0
FOR users.user_id=1
WHERE COUNT(orders.*, -30, 0, days) > 0
```
This limits the context examples to predicting churn for active users, limiting the context to examples relevant to your case and improving the performance. These filters are NOT applied to the provided entity list.

## Task Types
The task type is determined by the structure of the query, it is automatically inferred from the query. Common task types are:
- Regression -> output: Continuous real number -> example: ```{pql}PREDICT customers.age FOR customers.customer_id=1```
- Binary Classification -> output: True or False -> example: ```{pql}PREDICT fraud_reports.is_fraud FOR transactions.id WHERE transactions.type = "bank transfer"=1```
- Multiclass + Multilabel Classification -> output: Class label -> example: ```{pql}PREDICT FIRST(purchases.type, 0, 7) FOR users.user_id=1```
- Link Prediction -> output: List of items -> example: ```{pql}PREDICT LIST_DISTINCT(transactions.article_id, 0, 7) RANK TOP 10 FOR customers.customer_id=1```

## Static vs Temporal Tasks
Static queries that don’t involve making predictions over some window of time do not require a time column in the target table(s).
In contrast, temporal queries predict some aggregation of values over time (e.g., “purchases each customer will make over the next 7 days”) are more complex.

Example of a temporal query:
```{pql}
PREDICT SUM(transactions.price, 0, 30, days)
FOR customers.customer_id=1432
```
"""  # noqa: E501

    @mcp.resource("kumo://docs/pql-reference")
    async def get_pql_reference() -> str:
        """Complete reference of all PQL keywords and operators.

        Returns comprehensive documentation of all available PQL operators:
        - Primary commands: ASSUMING, FOR, PREDICT, WHERE
        - Aggregation operators: AVG, COUNT, COUNT_DISTINCT, LIST_DISTINCT,
            MAX, MIN, SUM
        - Boolean operators: !=, <, <=, =, >, >=, AND, CONTAINS, ENDS WITH, IN,
            IS NOT NULL, IS NULL, LIKE, NOT LIKE, NOT CONTAINS, NOT, OR,
            STARTS WITH

        Each operator includes syntax, examples, and behavioral notes.
        """
        return """# PQL Operators and Keywords Reference

## Primary Commands
- `ASSUMING` : Specifies the context of the prediction
- `FOR` : Specifies the entity to predict for
- `PREDICT` : Specifies the target to predict
- `WHERE` : Specifies the filters to apply to the prediction

### ASSUMING
ASSUMING <aggregation_function>(<fact_table>.<column_name>, <start>, <end>) <comparison_operator> <constant> (Optional)

To investigate hypothetical scenarios and evaluate impact of your actions or decisions, you can use the ASSUMING keyword. For example, you may want to investigate how much a user will spend if you give them a certain coupon or notification.
ASSUMING keyword is followed by a future-looking assumption, which will be assumed to be true during predictions.

```{pql}
ASSUMING <aggregation_function>(<fact_table>.<column_name>, <start>, <end>, <time_unit>) <comparison_operator> <constant>
ASSUMING COUNT(NOTIFICATIONS.*, 0, 7, days) > 2
ASSUMING LIST_DISTINCT(COUPONS.type, 0, 3, hours) CONTAINS '50 percent off'
ASSUMING COUNT(NOTIFICATIONS.*, 0, 7, days) > 5 AND SUM(NOTIFICATIONS.LENGTH, 0, 7, hours) > 10
```

## FOR
After specifying the prediction target, PQs are completed when you include the next required component, the FOR clause to specify the entity you want to make predictions for.
FOR <entity_specification>
```{pql}
FOR users.user_id=1
FOR users.user_id IN (1, 2, 3)
```

## PREDICT
A PQ always starts with the PREDICT clause that defines a required specification of a <target_expression> to predict.
PREDICT <target_expression>
```{pql}
PREDICT <Aggregation_Function>(<target_table>.<column_name>, <start>, <end>, <time_unit>)
PREDICT SUM(PURCHASES.amount, 0, 30, days)
PREDICT LIST_DISTINCT(PURCHASES.item_category, 0, 14, days)

PREDICT <target_table>.<column_name>
PREDICT CUSTOMERS.industry
```

## WHERE
The WHERE clause filters data before running predictions, allowing you to exclude irrelevant entities or targets from aggregation. Filters can be static (based on direct column values) or temporal (using aggregations over time).

### Temporal Filters
Temporal filters apply conditions based on past activity within a specified time window.
```{pql}
FOR EACH user.user_id WHERE COUNT(views.id, -30, 0) > 0
FOR EACH user.user_id WHERE LIST_DISTINCT(purchases.item_category, -90, 0) CONTAINS 'Food'
FOR EACH user.user_id WHERE LAST(status.status, -1, 0) = 'ACTIVE'
FOR EACH user.user_id WHERE COUNT(transactions.*, -INF, 0) > 0
```

### Static Filters
Static filters apply conditions based on direct column values.
```{pql}
FOR EACH user.user_id WHERE user.country = 'US'
FOR EACH user.user_id WHERE user.industry = 'Food'
```

### Inline and Nested WHERE Filters

### Using WHERE Within an Aggregation
```{pql}
PREDICT COUNT(transaction.* WHERE transaction.value > 10, 0, 7)
FOR EACH user.user_id WHERE COUNT(transaction.*, -7, 0) > 0
```

### Nested Temporal Filters
```{pql}
PREDICT COUNT(transaction.* WHERE transaction.value > 10, 0, 7)
FOR EACH user.user_id WHERE COUNT(transaction.*, -7, 0) > 0
```

### Multiple Target Tables
```{pql}
PREDICT COUNT(session.*, 0, 7) > 10 OR SUM(transaction.value, 0, 5) > 100
FOR EACH user.user_id
```

### Invalid Example (Mixing Static & Temporal)
```{pql}
PREDICT COUNT(session.*, 0, 7) > 10 OR SUM(transaction.value, 0, 5) > 100
FOR EACH user.user_id
```

### Filtering by Specific Date/Time
```{pql}
WHERE customers.date_joined < 2022-01-01 12:45:30
```

## Aggregation Operators
Aggregation operators always apply in the same way:
```{pql}
OPERATOR(<table>.<column>, <start>, <end>, <unit>)
```
Supported aggregation operators are: AVG, COUNT, COUNT_DISTINCT, LIST_DISTINCT, MAX, MIN, SUM

## Boolean Operators
- !=: <expression> != <value>, can be applied to any column type
- <: <expression> < <value>, can be applied to numerical and temporal columns only
- <=: <expression> <= <value>, can be applied to numerical and temporal columns only
- =: <expression> = <value>, can be applied to any column type
- >: <expression> > <value>, can be applied to numerical and temporal columns only
- >=: <expression> >= <value>, can be applied to numerical and temporal columns only
- AND: <expression_A> AND <expression_B>, each expression must itself be a filter operation
- OR: <expression_A> OR <expression_B>, each expression must itself be a filter operation
- NOT: NOT <expression>, works on all fields and targets
- CONTAINS: <expression> CONTAINS <value>, can be applied to text columns only
- NOT CONTAINS: <expression> NOT CONTAINS <value>, can be applied to text columns only
- ENDS WITH: <expression> ENDS WITH <value>, can be applied to text columns only
- IN: <expression> IN (<value_1>, <value_2>, <value_3>), can be applied to any column type
- IS NOT NULL: <expression> IS NOT NULL, can be applied to any column type
- IS NULL: <expression> IS NULL, can be applied to any column type
- LIKE: <expression> LIKE <value>, can be applied to text columns only
- NOT LIKE: <expression> NOT LIKE <value>, can be applied to text columns only
- STARTS WITH: <expression> STARTS WITH <value>, can be applied to text columns only
"""  # noqa: E501

    @mcp.resource("kumo://docs/table-guide")
    async def get_table_guide() -> str:
        """Best practices for working with tables in KumoRFM.

        Returns comprehensive guidelines for preparing tables for KumoRFM:
        - Primary key
        - Time column
        - General guideline for table definition

        This guide ensures tables are properly structured for optimal
        performance and accuracy in KumoRFM predictions.
        """
        return """# Table Guide

Each table can have at most one primary key and at most one time column, but it can contain many foreign keys (primary keys of other tables).

## Primary Key
- Each table can have at most one primary key
- Primary keys should be stable (not change over time)
- Recommended types: integers, UUIDs, or stable strings
- The primary key is used to identify entities in the graph

## Foreign Keys
- Each table can have many foreign keys
- Foreign keys are used to link tables together
- Foreign keys are used to filter temporal predictions
- Foreign keys are used to aggregate temporal data
- Foreign keys are used to join temporal data with other tables
- Foreign keys are used to filter temporal data
- Foreign keys are primary keys of other tables

## Time Column
- Each table can have at most one time column
- The time column is used to track the time of events in the table
- The time column is used to filter temporal predictions
- The time column is used to aggregate temporal data
- The time column is used to join temporal data with other tables
- The time column is used to filter temporal data

## General Guidelines
What makes a good table:
- Unique primary key: Non-null, no duplicates, uniquely identifies each row, preferably stored as integer
- Consistent naming: Foreign keys match their referenced primary key names
- Single time column: One temporal column when temporal data is available
"""  # noqa: E501

    @mcp.resource("kumo://docs/graph-guide")
    async def get_graph_guide() -> str:
        """How to build a graph in KumoRFM.

        Returns detailed guidance on modeling relationships in KumoRFM:
        - What is a graph in KumoRFM?
        - Adding tables to the graph
        - Inferring links between tables
        - Adding links between tables
        - Finalizing the graph
        - Best practices for building a graph

        This guide helps you build a graph in KumoRFM.
        """
        return """# Graph Guide

## What is a graph in KumoRFM?
A graph is a collection of tables and their relationships.

## Adding tables to the graph
To add a table to the graph, you can use the `add_table` method. The table will be added to the graph with no links.

## Inferring links between tables
To infer links between tables, you can use the `infer_links` method. This will infer links between tables based on foreign keys.

## Adding links between tables
To add a link between tables, you can use the `add_link` method.

## Finalizing the graph
To finalize the graph, you can use the `finalize_graph` method. This will finalize the graph and make it ready for predictions.

## One-to-One Relationships
- Direct foreign key reference
- Consider table consolidation when appropriate

## One to Many Relationships
- Foreign key in the "many" table
- Most common relationship pattern
- Properly indexed foreign keys
- Crucial for aggregations in both static and temporal predictions

## Many to Many Relationships
- Use junction/bridge tables
- Junction table naming conventions
- Include additional relationship metadata when needed


## Best Practices
- Meaningful links: Edges should represent meaningful relationships between tables, not just technical connections
- Entities are well-defined: Each table should represent either a single entity or a single event, not a mix of both
- Includes prediction ready structure: graph structure imposes limitations on the queries that can be defined with PQL (see query reference), so make sure that PQL queries you want to run are possible with the graph structure you have built

## Working around limitations
1. Multiple entities in a single table

Tables that mix data from multiple entities should be split for better graph structure. Think about each table as representing a single entity type or event. Here’s an example:
```{python}
# Original table mixing transaction, bank, and user data
mixed_data = pd.DataFrame({
    'transaction_id': [1, 2, 3],
    'bank_id': [101, 102, 101],
    'user_id': [201, 202, 203],
    'transaction_amount': [100.0, 250.0, 75.0],
    'transaction_type': ['deposit', 'withdrawal', 'transfer'],
    'bank_name': ['Chase', 'Wells Fargo', 'Chase'],
    'bank_routing': ['123456', '789012', '123456'],
    'user_name': ['Alice', 'Bob', 'Charlie'],
    'user_email': ['alice@email.com', 'bob@email.com', 'charlie@email.com']
})

# Split into three entity-focused tables

# 1. Transactions table (transaction-specific data)
transactions = mixed_data[['transaction_id', 'bank_id', 'user_id', 'transaction_amount', 'transaction_type']].copy()

# 2. Banks table (bank-specific data)
banks = mixed_data[['bank_id', 'bank_name', 'bank_routing']].drop_duplicates()

# 3. Users table (user-specific data)
users = mixed_data[['user_id', 'user_name', 'user_email']].drop_duplicates()

# Create graph with proper entity relationships
graph = rfm.LocalGraph.from_data({
    'transactions': transactions,
    'banks': banks,
    'users': users
})
# Result: transactions.bank_id -> banks.bank_id and transactions.user_id -> users.user_id
```

2. Many-to-many relationships
KumoRFM only supports primary-foreign key relationships (one-to-many). Many-to-many relationships require a junction table to break them into two one-to-many relationships:
```{python}
# Problem: Table with many-to-many data stored as lists/comma-separated values
user_skills_combined = pd.DataFrame({
    'user_id': [1, 2, 3],
    'user_name': ['Alice', 'Bob', 'Charlie'],
    'skills': [['Python', 'SQL'], ['SQL', 'Machine Learning'], ['Python', 'Machine Learning']],
    'proficiency_levels': [['expert', 'beginner'], ['intermediate', 'advanced'], ['expert', 'expert']]
})

# This structure cannot create proper foreign key relationships in KumoRFM

# Solution: Normalize into three tables with junction table

# 1. Users table (entity table)
users = user_skills_combined[['user_id', 'user_name']].copy()

# 2. Skills table (entity table)
all_skills = []
for skill_list in user_skills_combined['skills']:
    all_skills.extend(skill_list)
unique_skills = list(set(all_skills))

skills = pd.DataFrame({
    'skill_id': range(1, len(unique_skills) + 1),
    'skill_name': unique_skills
})

# 3. Junction table (breaks many-to-many into two one-to-many)
user_skills_records = []
for _, row in user_skills_combined.iterrows():
    for skill, proficiency in zip(row['skills'], row['proficiency_levels']):
        skill_id = skills[skills['skill_name'] == skill]['skill_id'].iloc[0]
        user_skills_records.append({
            'user_skill_id': len(user_skills_records) + 1,
            'user_id': row['user_id'],
            'skill_id': skill_id,
            'proficiency_level': proficiency
        })

user_skills = pd.DataFrame(user_skills_records)

# Create graph with proper one-to-many relationships
graph = rfm.LocalGraph.from_data({
    'users': users,
    'skills': skills,
    'user_skills': user_skills
})
# Result: user_skills.user_id -> users.user_id and user_skills.skill_id -> skills.skill_id
```

"""  # noqa: E501

    @mcp.resource("kumo://docs/data-guide")
    async def get_data_guide() -> str:
        """Best practices for preparing data for KumoRFM.

        How to define:
        - Tables
        - Links
        - Graphs

        to ensure that the data is ready for predictions.
        """
        return """# Data Preparation Guide

## Summary
Following these best practices will help ensure your KumoRFM datasets are well-structured, validated, and optimized for performance:

### Table Design:
- One entity or event per table
- Single time column per table
- Unique primary keys with consistent naming
- Junction tables for many-to-many relationships

### Data Preparation:
- Set proper pandas dtypes before creating tables
- Use meaningful semantic types (ID, categorical, text, numerical)
- Validate metadata and semantic types before proceeding

### Graph Structure:
- Design meaningful entity relationships
- Consider PQL query requirements in your structure
- Ensure single connected component

## Test with validation workflow

These patterns will help you create robust, queryable datasets that work effectively with KumoRFM’s predictive capabilities.
"""  # noqa: E501
