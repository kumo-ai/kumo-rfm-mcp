import pytest
from kumoapi.rfm import Explanation

from kumo_rfm_mcp import UpdateGraphMetadata
from kumo_rfm_mcp.tools.graph import materialize_graph, update_graph_metadata
from kumo_rfm_mcp.tools.model import evaluate, explain, predict


@pytest.mark.asyncio
async def test_predict(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    await materialize_graph()

    out = await predict(
        'PREDICT USERS.AGE>20 FOR EACH USERS.USER_ID',
        indices=[0],
        anchor_time=None,
        run_mode='fast',
        num_neighbors=[16, 16],
        max_pq_iterations=20,
    )
    assert len(out.predictions) == 1
    assert len(out.logs) == 0


@pytest.mark.asyncio
async def test_evaluate(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    await materialize_graph()

    out = await evaluate(
        'PREDICT USERS.AGE>20 FOR EACH USERS.USER_ID',
        metrics=None,
        anchor_time=None,
        run_mode='fast',
        num_neighbors=[16, 16],
        max_pq_iterations=20,
    )
    assert set(out.metrics.keys()) == {'ap', 'auprc', 'auroc'}
    assert len(out.logs) == 0


@pytest.mark.asyncio
async def test_explain(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    await materialize_graph()

    out = await explain(
        'PREDICT USERS.AGE>20 FOR EACH USERS.USER_ID',
        index=0,
        anchor_time=None,
        num_neighbors=[16, 16],
        max_pq_iterations=20,
    )
    assert isinstance(out.prediction, dict)
    assert isinstance(out.explanation, Explanation)
    assert len(out.logs) == 0
