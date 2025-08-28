## Understanding PQL
A Predictive Query is a declarative syntax used to define a predictive modeling task in Kumo.
It specifies the target variable to predict and the data context for training.

The query syntax should always follow the format:
* PREDICT <target_expression>
  - Declares the value or aggregate the model should predict
* FOR <entity_specification>
  - Specifies the single ID or list of IDs to predict for
* WHERE <filters> (optional)
  - Filters which historical entities are used as training examples

## Basic Query Structure
The basic structure of a Predictive Query is:
```{pql}
PREDICT <target_expression> FOR <entity_specification> WHERE <optional_filters>
```
- `target_expression`: Declares the value or aggregate the model should predict
- `entity_specification`: Specifies the single ID or list of IDs to predict for
- `optional_filters`: Filters which entities are included in the context of the prediction

## How target expression works
The target expression is the value or aggregate the model should predict.
It can be a single value, an aggregate, or a combination of both.
The target expression can be a single value (column name) or an aggregation
All components of the target expression must be fully qualified by table name and column name, e.g., `transactions.is_fraudulent` or `COUNT(orders.*, 0, 30, days) > 0`, or `users.is_active`, etc.
If the target is a single value, it has to appear in the same table as the entity we are predicting for.
For example, if we are predicting if a transaction is fraudulent, the target should be part of the `transactions` table.
```{pql}
PREDICT transactions.is_fraudulent FOR transactions.transaction_id=1
```

If the target is an aggregation, we have to aggregate over a column that appears in a table which is **connected** to the entity we are predicting for.
For example, if we are predicting the average order value for a user, we can write:
```{pql}
PREDICT AVG(orders.amount, 0, 30, days) FOR users.user_id=1
```
Here, `orders` is a table that is connected to `users` through a foreign key (i.e. `users.user_id = orders.user_id`).

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
KumoRFM makes its entity-specific predictions based on context examples, collected from the database.
Entity filters can be used to provide more control over KumoRFM context examples.
For example, to exclude `users` without recent activity from the context, we can write:
```{pql}
PREDICT COUNT(orders.*, 0, 30, days) > 0
FOR users.user_id=1
WHERE COUNT(orders.*, -30, 0, days) > 0
```
This limits the context examples to predicting churn for active users, limiting the context to examples relevant to your case and improving the performance.
These filters are NOT applied to the provided entity list.

## Task Types
The task type is determined by the structure of the query, it is automatically inferred from the query.
Common task types are:
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

## Primary Commands
- `ASSUMING` : Specifies the context of the prediction
- `FOR` : Specifies the entity to predict for
- `PREDICT` : Specifies the target to predict
- `WHERE` : Specifies the filters to apply to the prediction

### ASSUMING
ASSUMING <aggregation_function>(<fact_table>.<column_name>, <start>, <end>) <comparison_operator> <constant> (Optional)

To investigate hypothetical scenarios and evaluate impact of your actions or decisions, you can use the `ASSUMING` keyword.
For example, you may want to investigate how much a user will spend if you give them a certain coupon or notification.
`ASSUMING` keyword is followed by a future-looking assumption, which will be assumed to be true during predictions.

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
FOR user.user_id=1 WHERE COUNT(transaction.*, -7, 0) > 0
```

### Nested Temporal Filters
```{pql}
PREDICT COUNT(transaction.* WHERE transaction.value > 10, 0, 7)
FOR user.user_id=1 WHERE COUNT(transaction.*, -7, 0) > 0
```

### Multiple Target Tables
```{pql}
PREDICT COUNT(session.*, 0, 7) > 10 OR SUM(transaction.value, 0, 5) > 100
FOR user.user_id=1
```

### Invalid Example (Mixing Static & Temporal)
```{pql}
PREDICT COUNT(session.*, 0, 7) > 10 OR SUM(transaction.value, 0, 5) > 100
FOR user.user_id=1
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
Supported aggregation operators are: AVG, COUNT, LIST_DISTINCT, MAX, MIN, SUM

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
- IN: <expression> IN (<value_1>, <value_2>, <value_3>), can be applied to any column type
- IS NOT NULL: <expression> IS NOT NULL, can be applied to any column type
- IS NULL: <expression> IS NULL, can be applied to any column type

1. "Recommend movies to users"
   PREDICT LIST_DISTINCT(ratings.movie_id, 0, 14, days)
   RANK TOP 25 FOR users.user_id=9987

2. "Predict inactive users"
   PREDICT COUNT(sessions.*, 0, 14)=0 FOR users.user_id=9987
   WHERE COUNT(sessions.*,-7,0)>0

3. "Predict 5-star reviews"
   PREDICT COUNT(ratings.* WHERE ratings.rating = 5, 0,30) > 0
   FOR products.product_id=123456

4. "Predict customer churn"
   PREDICT COUNT(transactions.price, 0, 90) > 0
   FOR customers.customer_id=123456 WHERE SUM(transactions.price, -60, 0) > 0.05

5. "Find top articles"
   PREDICT LIST_DISTINCT(transactions.article_id, 0,90)
   RANK TOP 25 FOR customers.customer_id=123456
