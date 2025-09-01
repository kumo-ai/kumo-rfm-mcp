from pathlib import Path

import pandas as pd
import pytest
from pytest import TempPathFactory


@pytest.fixture(scope='session')
def root_dir(tmp_path_factory: TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp('table_files')

    df = pd.DataFrame({
        'USER_ID': [0, 1, 2, 3],
        'AGE': [20, 30, 40, float('NaN')],
        'GENDER': ['male', 'female', 'female', None],
    })
    df.to_csv(path / 'USERS.csv', index=False)

    df = pd.DataFrame({
        'USER_ID': [0, 1, 2, 3],
        'STORE_ID': [0, 1, 0, 1],
        'AMOUNT': [10, 15, float('NaN'), 20],
        'TIME': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04'],
    })
    df.to_parquet(path / 'ORDERS.parquet')

    df = pd.DataFrame({
        'STORE_ID': [0, 1],
        'CAT': ['burger', 'pizza'],
    })
    df.to_csv(path / 'STORES.csv', index=False)

    return path
