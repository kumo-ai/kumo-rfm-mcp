from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from kumoai.experimental import rfm
from kumoai.experimental.rfm.rfm import Explanation
from kumoapi.rfm import Explanation as ExplanationConfig
from kumoapi.task import TaskType
from pytest import TempPathFactory

from kumo_rfm_mcp import (
    AddTableMetadata,
    LinkMetadata,
    SessionManager,
    UpdateGraphMetadata,
)


@pytest.fixture(autouse=True)
def clear_session() -> None:
    SessionManager.get_default_session().clear()


@pytest.fixture(autouse=True)
def mock_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('KUMO_API_KEY', 'DUMMY')
    monkeypatch.setattr(rfm, 'init', lambda *args, **kwargs: None)

    def predict(
        *args: Any,
        explain: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame | Explanation:

        df = pd.DataFrame({
            'ENTITY': [0],
            'ANCHOR_TIMESTAMP': ['2025-01-1'],
            'TARGET_PRED': [True],
            'False_PROB': [0.4],
            'True_PROB': [0.6],
        })

        if not explain:
            return df

        return Explanation(
            prediction=df,
            summary='',
            details=ExplanationConfig(
                task_type=TaskType.BINARY_CLASSIFICATION,
                cohorts=[],
                subgraphs=[],
            ),
        )

    def evaluate(*args: Any, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame({
            'metric': ['ap', 'auprc', 'auroc'],
            'value': [0.8, 0.8, 0.9],
        })

    monkeypatch.setattr(rfm.KumoRFM, 'predict', predict)
    monkeypatch.setattr(rfm.KumoRFM, 'evaluate', evaluate)


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


@pytest.fixture
def graph(root_dir: Path) -> UpdateGraphMetadata:
    return UpdateGraphMetadata(  # type: ignore[call-arg]
        tables_to_add=[
            AddTableMetadata(
                path=(root_dir / 'USERS.csv').as_posix(),
                name='USERS',
                primary_key='USER_ID',
                time_column=None,
                end_time_column=None,
            ),
            AddTableMetadata(
                path=(root_dir / 'ORDERS.parquet').as_posix(),
                name='ORDERS',
                primary_key=None,
                time_column='TIME',
                end_time_column=None,
            ),
            AddTableMetadata(
                path=(root_dir / 'STORES.csv').as_posix(),
                name='STORES',
                primary_key='STORE_ID',
                time_column=None,
                end_time_column=None,
            ),
        ],
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
        ],
    )
