# Explainability

KumoRFM explanations provide two complementary views of model predictions:

1. **Global View (Cohorts):** Column-level patterns across in-context examples that reveal what data characteristics drive predictions
1. **Local View (Subgraph):** Cell-level attribution scores showing which specific values in this entity's subgraph influenced the prediction

Together, these views answer: "What patterns does the model see globally?" and "Which specific data points matter for this prediction?"

## Understanding the Global View: Cohorts

Cohorts reveal how different value ranges or categories in columns correlate with prediction outcomes across all in-context examples.

- `table_name`: Which table this analysis covers
- `column_name`: Which column or statistic (e.g., `COUNT(*)`) this analysis covers
- `hop`: Distance from the entity table (0 = entity attributes, 1 = direct neighbors, 2 = second-degree neighbors, ...)
- `stype`: Semantic type (numerical, categorical, timestamp, etc)
- `cohorts`: List of value ranges/categories (e.g., `["[0-5]", "(5-10]", "(10-20+]"]`)
- `populations`: Proportion of in-context examples in each cohort
- `targets`: Average prediction score within each cohort

High-impact columns usually have large variance in `targets` across different cohorts.

**Example for a churn predictive query:**

```
table_name: "orders"
column_name: "COUNT(*)"
hop: 1
cohorts: ["[0-0]", "(0-1]", "(1-2]", "(2-4]", "(4-6+]"]
populations: [0.20, 0.08, 0.07, 0.11, 0.54]
targets: [0.0, 0.78, 0.74, 0.64, 0.35]
```

**What this means:**

- Users with 0 orders have 0% churn risk (they already churned)
- Users with 1-2 orders have ~75% churn risk (early stage, not sticky)
- Users with 6+ orders have 35% churn risk (established, but not immune)
- Key insight: Order count is strongly predictive; more orders = lower churn

## Understanding the Local View: Subgraph

Subgraphs show the actual data neighborhood around the specific entity being predicted, with attribution scores indicating importance.
Node indices are different from primary keys and are mapped to a contiguous range from 0 to N.
The entity being predicted is guaranteed to have ID 0.
Some cells may have a `null` value with non-zero scores, indicating missingness itself is informative.

Each node represents a row from a table, containing:

- `cells`: Dictionary of column values with attribution scores
  - `value`: Actual data value
  - `score`: Gradient-based importance between 0 and 1 (higher = more influential)
- `links`: Connections to other nodes via foreign keys

Scores reflect how much changing this value would change the prediction.
High scores on specific cells explain "why this prediction, not another".

**Score Magnitude Interpretation:**

- 0.00 - 0.05: Negligible influence
- 0.05 - 0.15: Moderate influence
- 0.15 - 0.30: Strong influence
- 0.30+: Critical influence

**Example:**

```
cells: {
  "club_member_status": {value: "ACTIVE", score: 1.0},
  "age": {value: 49, score: 0.089},
  "fashion_news_frequency": {value: "Regularly", score: 0.411}
}
links: {
  "user_id->orders": [1,2,3,...,32]
}
```

**What this means:**

Club membership status is the most important attribute (score=1.0)
Fashion news subscription is moderately important (score=0.411).
Age contributes but is less critical (score=0.089).
User has 32 orders linked (indicates high activity).

You can follow paths in the subgraph to understand data connectivity and how tables/cells far away may contribute to the prediction.

## Connecting Global and Local Views

Often times, you can understand high subgraph attribution scores by relating their cell values to the average prediction of the cohort.

1. **Find influential cells for the prediction in the local view:**
   Which cells have scores > 0.15?
1. **Locate entity in global context:**
   Find which cohorts the specific entity falls into and compare entity's values to high/low risk cohorts.
   Focus on highest-scoring cells and most divergent cohorts.
1. **Relate attribution score and cohort prediction:**
   Check if entity exhibits typical or atypical patterns.
1. **Find general global trends** in the data that might explain the prediction.
   Additionally, look for missing expected signals (why ISN'T something important?)

Tell a coherent story connecting global patterns to local evidence.
Use concrete numbers from the subgraph.
Avoid jargon; explain in business terms.

## Common Interpretation Pitfalls

- **Don't assume correlation = causation:**
  High scores show model importance, not real-world causality.
  For example, "black clothing" might correlate with churn, but color isn't the cause.
- **Consider data distribution:**
  Rare cohorts may show extreme `targets` with small `populations`.
  Focus on cohorts with both significant population AND divergent targets.
- **Missing cohort analysis:**
  Not all columns have a cohort analysis since some semantic types are unsupported.
  For example, text and ID columns typically only appear in local view.
