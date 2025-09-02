from pathlib import Path

import pytest
from kumoapi.typing import Stype

from kumo_rfm_mcp import (
    AddTableMetadata,
    LinkMetadata,
    UpdateGraphMetadata,
    UpdateTableMetadata,
)
from kumo_rfm_mcp.tools.graph import (
    get_mermaid,
    inspect_graph_metadata,
    lookup_table_rows,
    materialize_graph,
    update_graph_metadata,
)


def test_graph_metadata(root_dir: Path) -> None:
    graph = inspect_graph_metadata()
    assert len(graph.tables) == 0
    assert len(graph.links) == 0

    out = update_graph_metadata(
        UpdateGraphMetadata(  # type: ignore[call-arg]
            tables_to_add=[
                AddTableMetadata(
                    path=str(root_dir / 'USERS.csv'),
                    name='USERS',
                    primary_key='USER_ID',
                    time_column=None,
                ),
                AddTableMetadata(
                    path=str(root_dir / 'ORDERS.parquet'),
                    name='ORDERS',
                    primary_key=None,
                    time_column='TIME',
                ),
                AddTableMetadata(
                    path=str(root_dir / 'STORES.csv'),
                    name='STORES',
                    primary_key='STORE_ID',
                    time_column=None,
                ),
            ]))
    assert len(out.graph.tables) == 3
    assert len(out.graph.links) == 0
    assert len(out.errors) == 0

    out = update_graph_metadata(
        UpdateGraphMetadata(  # type: ignore[call-arg]
            tables_to_update={
                'USERS':
                UpdateTableMetadata(  # type: ignore[call-arg]
                    stypes={
                        'AGE': Stype.categorical,
                        'GENDER': None,
                    },
                )
            }))
    assert len(out.graph.tables) == 3
    assert len(out.graph.links) == 0
    assert len(out.errors) == 0
    assert out.graph.tables[0].stypes['AGE'] == Stype.categorical
    assert out.graph.tables[0].stypes['GENDER'] is None

    out = update_graph_metadata(
        UpdateGraphMetadata(  # type: ignore[call-arg]
            links_to_add=[
                LinkMetadata(
                    source_table='ORDERS',
                    foreign_key='USER_ID',
                    destination_table='USERS',
                ),
                LinkMetadata(
                    source_table='ORDERS',
                    foreign_key='STORE_ID',
                    destination_table='STORES',
                ),
            ]))
    assert len(out.graph.tables) == 3
    assert len(out.graph.links) == 2
    assert len(out.errors) == 0

    out = update_graph_metadata(
        UpdateGraphMetadata(  # type: ignore[call-arg]
            links_to_remove=[
                LinkMetadata(
                    source_table='ORDERS',
                    foreign_key='USER_ID',
                    destination_table='USERS',
                ),
            ],
            tables_to_remove=['STORES'],
        ))
    assert len(out.graph.tables) == 2
    assert len(out.graph.links) == 0
    assert len(out.errors) == 0


def test_get_mermaid(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    mermaid = get_mermaid(show_columns=False)
    assert mermaid == ('erDiagram\n'
                       '    USERS {\n'
                       '        ID USER_ID PK\n'
                       '    }\n'
                       '    ORDERS {\n'
                       '        ID USER_ID FK\n'
                       '        ID STORE_ID FK\n'
                       '        timestamp TIME\n'
                       '    }\n'
                       '    STORES {\n'
                       '        ID STORE_ID PK\n'
                       '    }\n'
                       '\n'
                       '    USERS o|--o{ ORDERS : USER_ID\n'
                       '    STORES o|--o{ ORDERS : STORE_ID')


@pytest.mark.asyncio
async def test_materialize_graph(graph: UpdateGraphMetadata) -> None:
    out = update_graph_metadata(graph)
    out = await materialize_graph()
    assert out.num_nodes == 10
    assert out.num_edges == 16
    assert out.time_ranges == {
        'ORDERS': '2025-01-01 00:00:00 - 2025-01-04 00:00:00'
    }


@pytest.mark.asyncio
async def test_lookup_table_rows(graph: UpdateGraphMetadata) -> None:
    update_graph_metadata(graph)
    await materialize_graph()
    preview = await lookup_table_rows('USERS', ids=[1, 0])
    assert preview.rows == [
        {
            'USER_ID': 1,
            'AGE': 30.0,
            'GENDER': 'female',
        },
        {
            'USER_ID': 0,
            'AGE': 20.0,
            'GENDER': 'male',
        },
    ]
