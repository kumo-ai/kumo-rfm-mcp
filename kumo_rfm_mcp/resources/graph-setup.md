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
