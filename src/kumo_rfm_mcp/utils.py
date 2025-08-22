import pandas as pd


def load_dataframe(path: str, nrows: int | None = None) -> pd.DataFrame:
    """Load a DataFrame from a supported file path.

    Supports CSV and Parquet. Raises ValueError for unsupported formats.
    """
    lower = path.lower()
    if not (lower.endswith('.csv') or lower.endswith('.parquet')):
        raise ValueError(
            f"Can not read file from path '{path}'. Only '*.csv' or "
            "'*.parquet' files are supported")

    if lower.endswith('.csv'):
        return pd.read_csv(path, nrows=nrows)
    return pd.read_parquet(path)
