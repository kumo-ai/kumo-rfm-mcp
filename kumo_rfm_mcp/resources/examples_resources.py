from fastmcp import FastMCP


def register_examples_resources(mcp: FastMCP):
    """Register all example resources with the MCP server."""

    @mcp.resource("kumo://examples/e-commerce")
    async def get_ecommerce_examples() -> str:
        """Predictive query examples for an e-commerce use dataset.

        Each example includes:
        - Complete PQL query syntax
        - Explanation of the prediction logic
        """
        return """# E-commerce Predictive Query Examples

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
"""  # noqa: E501
