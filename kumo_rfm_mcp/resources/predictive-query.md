# Predictive Query

The Predictive Query Language (PQL) is a querying language that allows to define relational machine learning tasks.
PQL lets you define predictive problems by specifying:

1. **The target expression:** Declares the value or aggregate the model should predict
1. **The entity specification:** Specified the single ID or list of IDs to predict for
1. **Optional entity filters:** Filters which historical entities are used as in-context learning examples

The basic structure of a predictive query is:

```
PREDICT <target_expression> FOR <entity_specification> WHERE <optional_filters>
```

In general, follow these give steps to author a predictive query:

1. **Choose your entity** - a table and its primary key you predict for.
1. **Define the target** - a raw column or an aggregation over a future window.
1. **Pin the entity list** - pass a single ID or multiple IDs to make predictions for.
1. **Refine the context** - filters to restrict which historical rowas are used as in-context learning examples.
1. **Run & fetch** - run `predict` or `evaluate` on top.

## Entity Specification

Entities for each query can be specified in one of two ways:

- By specifying a single entity ID:
  ```
  PREDICT ... FOR users.user_id=1
  ```
- By specifying a tuple of entity ids
  ```
  PREDICT ... FOR users.user_id IN (1, 2, 3)
  ```

Note that the entity table needs a primary key to uniquely define the set of IDs to predict for.
Up to 1000 IDs can be passed at once.

The data type of the primary key in the entity table denotes how entity IDs should be passed.
For string-based IDs, use `users.user_id='1'` or `users.user_id IN ('1', '2')`.
For numerical IDs, use `users.user_id=1` or `users.user_id IN (1, 2)`.

## Target Expression

The target expression is the value or aggregate the model should predict.
It can be a single value, an aggregate, or a combination of both.
All components of the target expression must be fully qualified by table name and column name.
We differentiate between two types of queries: static and temporal queries.

### Static Predictive Queries

Static predictive queries are used to impute missing values from an entity table.
That is, the target column has to appear in the same table as the entity we are predicting for.
KumoRFM will then mask out the target column and predict the value from related in-context examples.

For example, we can predict the age of users via

```
PREDICT users.age FOR users.user_id=1
```

We can impute missing values for all `"numerical"` and `"categorical"` columns.
For `"numerical"` columns, the predictive query is interpreted as a regression task.
For `"categorical"` columns, the predictive query is interpreted as a multi-class classification task.
For binary classification tasks, you can add **conditions** to the target expression:

```
PREDICT users.age > 40 FOR users.user_id=1
```

The following boolean operators are supported:

- `=`: `<expression> = <value>` - can be applied to any column type
- `!=`: `<expression> != <value>`, can be applied to any column type
- `<`: `<expression> < <value>` - can be applied to numerical and temporal columns only
- `<=`: `<expression> <= <value>` - can be applied to numerical and temporal columns only
- `>`: `<expression> > <value>` - can be applied to numerical and temporal columns only
- `>=`: `<expression> >= <value>` - can be applied to numerical and temporal columns only
- `IN`: `<expression> IN (<value_1>, <value_2>, <value_3>)` - can be applied to any column type
- `IS NOT NULL`: `<expression> IS NOT NULL`, can be applied to any column type
- `IS NULL`: `<expression> IS NULL` - can be applied to any column type

Multiple conditions can be combined via `AND` and/or `OR` logical operations to form complex predictive queries, e.g.:

```
PREDICT users.age>40 OR users.location='US' FOR users.user_id=1
```

The following logical operations are supported:

- `AND`: `<boolean_expression_A> AND <boolean_expression_B>`
- `OR`: `<boolean_expression_A> OR <boolean_expression_B>`
- `NOT`: `NOT <boolean_expression>`

Use parentheses to group logical operations and control their order.

### Temporal Predictive Queries

Temporal predictive queries predict some aggregation of values over time (e.g., purchases each customer will make over the next 7 days).
The target table needs to be directly connected to the entity table via a foreign key-primary key relationship.

Aggregations are defined via aggregation operators over a **relative** period of time.
You can specify an aggregation operator and the column representing the value you want to aggregate.
For example:

```
PREDICT SUM(transactions.price, 0, 30, days) for users.user_id=1
```

Here, `transactions` is a table that is connected to `users` through a foreign key (`transactions.user_id <> users.user_id`).

Within the aggregation function inputs, the start (`0`) and end (`30`) parameters refer to the time period you want to aggregate across.
For example, a value of `10` for start and `30` for end implies that you want to aggregate from 10 days later (excluding the 10th day) to 30 days later (including the 30th day).
The time unit of the aggregation defaults to `days` if none is specified.

The following time units are supported: `seconds`, `minutes`, `hours`, `days`, `weeks`, `months`
When using aggregation in a target expression, both start and end values should be non-negative, and end values should be greater than start values.

Similar to static predictive queries, you can add conditions and logical operations to temporal predictive queries to create binary classification tasks:

```
PREDICT SUM(transactions.price, 0, 30, days)=0 for users.user_id=1
```

When using logical operations, you can also aggregate from multiple different target tables.

```
PREDICT COUNT(session.*, 0, 7)>10 OR SUM(transaction.value, 0, 5)>100 FOR user.user_id=1
```

#### Aggregation Operators

The following aggregation operators are supported:

- `SUM` - can be applied to any numerical column
- `AVG` - can be applied to any numerical column
- `MIN` - can be applied to any numerical column
- `MAX` - can be applied to any numerical column
- `COUNT` - can be applied to any column type. Use `*` as column name to count the number of events.
- `LIST_DISTINCT` - can be applied to any foreign key column

Specifically, the `LIST_DISTINCT` syntax can be used to formulate recommendation tasks, which ask for predicting the next items an entity will relate to, e.g., views, orders, likes, etc.
As such, its target column needs to refer to a foreign key column, e.g., `item_id` in the `orders` table.

The `LIST_DISTINCT` operator additionally requires a specification about how many top-k predictions to retrieve.
Up to 20 predictions are supported.

```
PREDICT LIST_DISTINCT(orders.item_id, 0, 7, days) RANK TOP 10 FOR users.user_id=1
```

#### Target Filters

Target filters allow you to further conextualize your predictive query by dropping certain target rows that do not meet a specified condition.
By using a `WHERE` clause, you can drop target rows from your aggregation.
For example, the following is an example of using the `WHERE` operator within an aggregation for target filters:

```
PREDICT COUNT(transactions.* WHERE transactions.price > 10, 0, 7, days) FOR users.user_id=1
```

Target filters can reference any column within the target table, and can be extended via logical operations.

## Entity Filters

KumoRFM makes entity-specific predictions based on in-context examples, collected from a historical snapshot of the relational data.
Entity filters can be used to provide more control over how KumoRFM collects in-context examples.
For example, to exclude `users` without recent activity from the context, we can write:
```
PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1 WHERE COUNT(orders.*, -30, 0, days) > 0
```
This limits the in-context examples to predicting churn for active users only.
These filters are NOT applied to the provided entity list.

Both static and temporal filters can be used as entity filters.
If you use temporal filters, then the start and end parameters need to be backwards looking, e.g., `start < 0` and `end <= 0`.
Still, end values need to be greater than start values.

For forward looking entity filters and to investigate hypothetical scenarios and evaluate impact of your actions or decisions, you can use the `ASSUMING` keyword instead.
For example, you may want to investigate how much a user will spend if you give them a certain coupon or notification.
`ASSUMING` keyword is followed by a future-looking assumption, which will be assumed to be true during predictions.

```
PREDICT COUNT(orders.*, 0, 30, days)>0 FOR users.user_id=1 ASSUMING COUNT(notifications.*, 0, 7, days)>0
```

## Task Types

The predictive query uniquely determines the underlying machine learning task type.
The following machine learning tasks are supported:

- **Binary classification:** Conditioned static target column or aggregate
- **Multi-class classification:** Categorical static target column
- **Regression:** Numerical static target column or aggregate
- **Recommendation/temporal link prediction:** Distinct list of foreign key IDs

## Examples

1. **Recommend movies to users:**
   ```
   PREDICT LIST_DISTINCT(ratings.movie_id, 0, 14, days)
   RANK TOP 20 FOR users.user_id=9987
   ```

2. **Predict inactive users:**
   ```
   PREDICT COUNT(sessions.*, 0, 14)=0 FOR users.user_id=9987
   WHERE COUNT(sessions.*,-7,0)>0
   ```

3. **Predict 5-star reviews:**
   ```
   PREDICT COUNT(ratings.* WHERE ratings.rating = 5, 0,30)>0
   FOR products.product_id=123456
   ```

4. **Predict customer churn:**
   ```
   PREDICT COUNT(transactions.price, 0, 90)>0
   FOR customers.customer_id=123456 WHERE SUM(transactions.price, -60, 0) > 0.05
   ```

5. **Find next best articles:**
   ```
   PREDICT LIST_DISTINCT(transactions.article_id, 0, 90)
   RANK TOP 20 FOR customers.customer_id=123456
   ```
