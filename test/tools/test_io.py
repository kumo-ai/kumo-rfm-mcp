from pathlib import Path

import pytest

from kumo_rfm_mcp.tools.io import find_table_files, inspect_table_files


@pytest.mark.asyncio
@pytest.mark.parametrize('recursive', [False, True])
async def test_find_table_files(root_dir: Path, recursive: bool) -> None:
    sources = await find_table_files(root_dir, recursive)
    assert len(sources) == 3
    filenames = {source.path.name for source in sources}
    assert filenames == {'USERS.csv', 'ORDERS.parquet', 'STORES.csv'}


@pytest.mark.asyncio
async def test_inspect_table_files(root_dir: Path) -> None:
    previews = await inspect_table_files(
        paths=[(root_dir / 'USERS.csv').as_posix()],
        num_rows=4,
    )
    assert len(previews) == 1
    preview = previews[(root_dir / 'USERS.csv').as_posix()]
    assert preview.rows == [
        {
            'USER_ID': 0,
            'AGE': 20.0,
            'GENDER': 'male',
        },
        {
            'USER_ID': 1,
            'AGE': 30.0,
            'GENDER': 'female',
        },
        {
            'USER_ID': 2,
            'AGE': 40.0,
            'GENDER': 'female',
        },
        {
            'USER_ID': 3,
            'AGE': None,
            'GENDER': None,
        },
    ]
