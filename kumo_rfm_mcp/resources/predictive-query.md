# Predictive Query

The Predictive Query Language (PQL) is a querying language that allows to define relational machine learning tasks.
PQL lets you define predictive problems by specifying:

1. **The target expression:** Declares the value or aggregate the model should predict
1. **The entity specification:** Specified the single ID or list of IDs to predict for
1. **Optional entity filters:** Filters which historical entities are used as in-context learning examples

The basic structure of a predictive query is:

```
PREDICT <target_expression> FOR EACH <entity_specification> WHERE <optional_filters>
```

Every predictive query needs to contain the `PREDICT` and `FOR EACH` keywords.
All references to columns within predictive queries must be fully qualified by table name and column name as `<table_name>.<column_name>`.

In general, follow these given steps to author a predictive query:

1. **Choose your entity** - a table and its primary key you predict for.
1. **Define the target** - a raw column or an aggregation over a future window.
1. **Refine the context** - if necessary, restrict which historical rows are used as in-context learning examples.
1. **Run & fetch** - run `predict` or `evaluate` on top.

A predictive query uniquely defines a predictive machine learning task.
As such, it also defines the procedure on how to obtain ground-truth labels from historical snapshots of the data, which are used to generate context labels to perform in-context learning within KumoRFM.

**Important:** PQL is not SQL.
Standard SQL operations such as `JOIN`, `SELECT`, `UNION`, `GROUP BY`, and subqueries are not supported in PQL.
PQL uses a simpler, more constrained syntax designed specifically for defining predictive machine learning tasks.
PQL also doesn't support arithmetic operations like `+` or `-`.
Do not make syntax up that is not listed in this document.

## Entity Specification

Entities for each query can be specified via:

```
PREDICT ... FOR EACH users.user_id
```

Note that the entity table needs a primary key to uniquely determine the set of IDs to predict for.

The actual entities to generate predictions for can be fully customized as part of the `predict` tool via the `indices` argument.
Up to 1000 entities are supported for an individual query.
Note that predictions will be generated for all indices, regardless of whether they match any entity filter constraints defined in the `WHERE` clause.

## Target Expression

The target expression is the value or aggregate the model should predict.
It can be a single value, an aggregate, a condition, or a set of logical operations.
We differentiate between two types of queries: static and temporal queries.

### Static Predictive Queries

Static predictive queries are used to impute missing values from an entity table.
That is, the target column has to appear in the same table as the entity you are making a prediction for.
KumoRFM will then mask out the target column and predict the value from related in-context examples.

For example, you can predict the age of users via

```
PREDICT users.age FOR EACH users.user_id
```

You can impute missing values for all `"numerical"` and `"categorical"` columns.
Currently, you cannot impute missing values for other semantic types such as `"timestamp"` or `"text"`.
For `"numerical"` columns, the predictive query is interpreted as a regression task.
For `"categorical"` columns, the predictive query is interpreted as a multi-class classification task.
For binary classification tasks, you can add **conditions** to the target expression:

```
PREDICT users.age > 40 FOR EACH users.user_id
```

The following boolean operators are supported:

- `=`: `<expression> = <value>` - can be applied to any column type
- `!=`: `<expression> != <value>`, can be applied to any column type
- `<`: `<expression> < <value>` - can be applied to numerical and temporal columns only
- `<=`: `<expression> <= <value>` - can be applied to numerical and temporal columns only
- `>`: `<expression> > <value>` - can be applied to numerical and temporal columns only
- `>=`: `<expression> >= <value>` - can be applied to numerical and temporal columns only
- `IN`: `<expression> IN (<value_1>, <value_2>, <value_3>)` - can be applied to any column type

The `<value>` needs to be a constant, pre-defined value.
It cannot be modeled as a target expression.
When using boolean conditions, the value format must match the column's data type:

```
PREDICT users.location='US' FOR EACH users.user_id
PREDICT users.birthday>1990-01-01 FOR EACH users.user_id
```

Multiple conditions can be logically combined via `AND`, `OR` and `NOT` to form complex predictive queries, e.g.:

```
PREDICT (users.age>40 OR users.location='US') AND (NOT users.gender='male') FOR EACH users.user_id
```

The following logical operations are supported:

- `AND`: `<boolean_expression_A> AND <boolean_expression_B>`
- `OR`: `<boolean_expression_A> OR <boolean_expression_B>`
- `NOT`: `NOT <boolean_expression>`

Use parentheses to group logical operations and control their order.

### Temporal Predictive Queries

Temporal predictive queries predict some aggregation of values over time (e.g., purchases each customer will make over the next 7 days).
The target table needs to be directly connected to the entity table via a foreign key-primary key relationship.

An aggregation is defined by an aggregation operator over a **relative** period of time.
You can specify an aggregation operator and the column in the target table representing the value you want to aggregate.
The syntax is as follows:

```
<aggr>(<target_table>.<target_column>, <start_offset>, <end_offset>, <time_unit>)
```

For example:

```
PREDICT SUM(orders.price, 0, 30, days) FOR EACH users.user_id
```

Here, `orders` is a table that is connected to `users` via a foreign key-primary key relationship (`orders.user_id <> users.user_id`).
Within the aggregation function inputs, the `<start_offset>` (`0` in the example) and `<end_offset>` (`30` in the example) parameters refer to the time period you want to aggregate across, relative to a given anchor time.
Both `<start_offset>` and `<end_offset>` should be non-negative, and `<end_offset>` values should be strictly greater than `<start_offset>`.
As such, the example query can be understood as: "Predict the sum of prices of all the orders a user will do in the next 30 days".

Note that by default, the anchor time is set to the maximum timestamp present in your relational data, but can be fully customized in `predict` and `evaluate` tools.
The `<start_offset>` value is not limited to be always `0`.
For example, a `<start_offset>` value of `10` and an `<end_offset>` value of `30` implies that you want to aggregate from 10 days later (excluding the 10th day) to 30 days later (including the 30th day).

The following values for `<time_unit>` are supported: `seconds`, `minutes`, `hours`, `days`, `weeks`, `months`
The time unit of the aggregation defaults to `days` if none is specified.

Similar to static predictive queries, you can add conditions and logical operations to temporal predictive queries to create binary classification tasks:

```
PREDICT SUM(transactions.price, 0, 30, days)=0 FOR EACH users.user_id
```

When using logical operations, it is allowed to aggregate from multiple different target tables:

```
PREDICT COUNT(session.*, 0, 7)>10 OR SUM(transaction.value, 0, 5)>100 FOR EACH user.user_id
```

#### Aggregation Operators

The following aggregation operators are supported:

- `SUM`: Calculates the total of values in a numerical column
- `AVG`: Calculates the average of values in a numerical column
- `MIN`: Finds the minimum value in a numerical column
- `MAX`: Finds the maximum value in a numerical column
- `COUNT`: Counts the number of rows/events.
  Use `COUNT(<target_table>.*, ...)` to count all events, or `COUNT(<target_table>.<target_column>, ...)` to count non-null values in any column type.
  The `COUNT` operator is the only operator where the special `*` syntax is allowed.
- `LIST_DISTINCT`: Returns a distinct list of unique values from a foreign key column (used for recommendations)

##### Recommendation Tasks

The `LIST_DISTINCT` operator is specifically designed for recommendation tasks.
It predicts which foreign key values an entity will interact with in the future.
The basic syntax is:

```
LIST_DISTINCT(<target_table>.<foreign_key>, <start_offset>, <end_offset>, <time_unit>) RANK TOP k FOR EACH ...
```

`LIST_DISTINCT` aggregations must be applied to foreign key columns (not regular columns).
They cannot be combined with conditions or logical operations.
They also must include `RANK TOP k` to specify how many recommendations to return, where `k` can range from 1 to 20 (maximum 20 recommendations per query).
For example:

```
PREDICT LIST_DISTINCT(orders.item_id, 0, 7, days) RANK TOP 10 FOR EACH users.user_id
```

##### Handling Inactive Entities in Temporal Aggregations

In case there is no event for a given entity within the requested time window, predictive query behaves differently depending on the aggregation operator and whether it has a neutral element.

**Zero-Valued Aggregations**: For `SUM` and `COUNT` operations, entities with no activity will return zero values and will be included as in-context learning examples.

**Undefined Aggregations**: For `AVG`, `MIN`, `MAX`, and `LIST_DISTINCT` operations, inactive entities produce undefined results and are excluded from in-context learning.

**Important:** Make sure that treating inactive entities as zero is desirable.
Always use temporal entity filters with `SUM` and `COUNT` aggregations to prevent learning from irrelevant and outdated examples (see below on how to define temporal entity filters).

#### Target Filters

Target filters allow you to further conextualize your predictive query by dropping certain target rows that do not meet a specific condition.
By using a `WHERE` clause within the target expression (valid for all aggregation types), you can drop rows from being aggregated.
For example:

```
PREDICT COUNT(transactions.* WHERE transactions.price > 10, 0, 7, days) FOR EACH users.user_id
```

Note that the `WHERE` clause of target filters need to be part of the aggregation input.
Target filters must be static and thus can **only** reference columns within the target table being aggregated.
Cross-table references, subqueries, and joins are **not** supported.
Do not make syntax up that is not listed in this document.

## Entity Filters

KumoRFM makes entity-specific predictions based on in-context examples, collected from a historical snapshot of the relational data.
Entity filters can be used to provide more control over how KumoRFM collects in-context examples.
For example, to exclude `users` without recent activity from the context, you can write:

```
PREDICT COUNT(orders.*, 0, 30, days)>0 FOR EACH users.user_id WHERE COUNT(orders.*, -30, 0, days) > 0
```

This limits the in-context examples for predicting churn to active users only.
Note that these filters are **not** applied to the provided entity list `indices` as part of the `predict` tool.

Both static and temporal filters can be used as entity filters.
If you use temporal entity filters, the `<start_offset>` and `<end_offset>` parameters need to be backward looking, i.e. `<start_offset> < 0` and `<end_offset> <= 0`.
Still, `<end_offset>` values need to be strictly greater than `<start_offset>` values.
For temporal entity filters, `<start_offset>` can also be defined as `-INF` to include all historical data from the beginning of the dataset.

In order to to investigate hypothetical scenarios and to evaluate impact of your actions or decisions, you can use the `ASSUMING` keyword (instead of `WHERE`) to write forward looking entity filters.
For example, you may want to investigate how much a user will spend if you give them a certain coupon or notification.
The `ASSUMING` keyword is followed by a future-looking assumption, which will be assumed to be true for the entity IDs you predict for.

```
PREDICT COUNT(orders.*, 0, 30, days)>0 FOR EACH users.user_id ASSUMING COUNT(notifications.*, 0, 7, days)>0
```

Standard SQL operations such as `JOIN`, `SELECT`, `UNION`, `GROUP BY`, and subqueries are not supported in PQL.
Do not make syntax up that is not listed in this document.

## Task Types

The predictive query uniquely determines the underlying machine learning task type based on your query structure and the underlying graph schema.
The following machine learning tasks are supported:

- **Binary classification:** When your target expression includes a condition that results in true/false
- **Multi-class classification:** When predicting a categorical column with multiple possible values
- **Regression:** When predicting a numerical value
- **Recommendation/temporal link prediction:** When predicting a ranked list of items using `LIST_DISTINCT`

Note that you don't need to specify the task type.
PQL automatically detects it based on whether you are predicting a condition (binary), categories (multi-class), numbers (regression), or ranked lists (recommendation).

## Best Practices

- Use target filters to filter which events to aggregate.
- Use entity filters to filter which historical examples to learn from.
- Make sure to include temporal entity filters in zero-valued aggregations such as `SUM` or `COUNT`.
- Ensure value formats match column data types in conditions (e.g., `'US'` for strings, `1990-01-01` for dates).
- It might be non-trivial to pick appropriate `<start_offset>` and `<end_offset>` values.
  Choose meaningful time windows that align with domain knowledge and account for event frequency.
  For example, in an e-commerce dataset, predicting churn based on the next seven days might be unrealistic.
  Play around with different time windows and see how it affects the prediction.
- Analyze the label distribution of in-context learning examples in the `predict` and `evaluate` tool logs to understand if your query needs any adjustments, e.g., more or less strict temporal entity filters.
- Take the label distribution of the predictive query into account when analyzing output metrics of the `evaluate` tool.
- When running a predictive query via `predict` or `evaluate` tools, use `run_mode="fast"` for initial exploration, and reserve `run_mode="best"` for final production queries.
- Choose anchor times that represent realistic prediction scenarios.
  Use `anchor_time=None` to make predictions based on the most recent data.
  Use `anchor_time='entity'` for static predictions to prevent temporal leakage if entities denote temporal facts.
- Tune the `max_pq_iterations` argument if you see that the model fails to find sufficient number of in-context examples w.r.t. the `run_mode`, i.e. 1000 for `'fast'`, 5000 for `'normal'` and 10000 for `'best'`.

## Common Mistakes

- Ensure that `<start_offset>` is always less than `<end_offset>`.
- Ensure that `<end_offset>` is less than or equal to `0` in temporal entity filters.
- PQL doesn't support arithmetic operations.
- PQL is not SQL - use only supported operators and conditions.
- `SUM` and `COUNT` queries without temporal entity filters include inactive/irrelevant examples.
  Always use temporal entity filters with `SUM` and `COUNT` to focus on relevant examples.
- Incorrect semantic types may lead to wrong task formulations.
  Carefully review and correct semantic types during graph setup.
- `LIST_DISTINCT` only works on foreign key columns.
- Using `anchor_time='entity'` for temporal queries with aggregations is **not** supported.

## Examples

1. **Recommend movies to users:**

   ```
   PREDICT LIST_DISTINCT(ratings.movie_id, 0, 14, days) RANK TOP 20
   FOR EACH users.user_id
   ```

1. **Predict inactive users:**

   ```
   PREDICT COUNT(sessions.*, 0, 14)=0
   FOR EACH users.user_id WHERE COUNT(sessions.*,-7,0)>0
   ```

1. **Predict 5-star reviews:**

   ```
   PREDICT COUNT(ratings.* WHERE ratings.rating = 5, 0, 30)>0
   FOR EACH products.product_id
   ```

1. **Predict customer churn:**

   ```
   PREDICT COUNT(transactions.price, 0, 3, months)>0
   FOR EACH customers.customer_id
   WHERE SUM(transactions.price, -2, 0, months)>0.05
   ```

1. **Find next best articles:**

   ```
   PREDICT LIST_DISTINCT(transactions.article_id, 0, 90) RANK TOP 20
   FOR EACH customers.customer_id
   ```
