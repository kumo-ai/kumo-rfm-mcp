import pytest

from kumo_rfm_mcp import UpdateGraphMetadata
from kumo_rfm_mcp.tools.graph import materialize_graph, update_graph_metadata
from kumo_rfm_mcp.tools.model import evaluate, predict


@pytest.mark.asyncio
async def test_predict(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    await materialize_graph()

    out = await predict(
        'PREDICT USERS.AGE>20 FOR USERS.USER_ID=0',
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
        'PREDICT USERS.AGE>20 FOR USERS.USER_ID=0',
        metrics=None,
        anchor_time=None,
        run_mode='fast',
        num_neighbors=[16, 16],
        max_pq_iterations=20,
    )
    assert set(out.metrics.keys()) == {'ap', 'auprc', 'auroc'}
    assert len(out.logs) == 0
