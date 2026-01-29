from __future__ import annotations

import os
import time

import pandas as pd
import polars as pl

from .config import get_config
from .operations._tracking import get_active_session, get_sample_size


def load_source(
    source_id: str,
    *,
    sample: int | None = None,
) -> pd.DataFrame:
    """
    Load a source DataFrame from a feather file (returns pandas).

    File path: {data_path}/{source_id}.feather

    In build mode (within BuildSession), automatically samples data
    unless explicit sample parameter is provided.

    Args:
        source_id: Source identifier (e.g., "DAT-00000001")
        sample: Override sample size (ignores session sample)

    Returns:
        DataFrame loaded from feather file
    """
    config = get_config()
    file_path = os.path.join(config.data_path, f"{source_id}.feather")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")

    start_time = time.perf_counter()
    df = pd.read_feather(file_path)
    original_rows = len(df)

    effective_sample = sample if sample is not None else get_sample_size()

    if effective_sample is not None and len(df) > effective_sample:
        df = df.head(effective_sample).reset_index(drop=True)

    duration_ms = (time.perf_counter() - start_time) * 1000

    session = get_active_session()
    if session is not None:
        session.record(
            operation="load_source",
            params={"source_id": source_id, "sample": effective_sample},
            rows_before=original_rows,
            rows_after=len(df),
            duration_ms=duration_ms,
            affected_columns=[],
            result_df=df,
        )

    return df


def load_source_polars(
    source_id: str,
    *,
    sample: int | None = None,
) -> pl.DataFrame:
    """
    Load a source DataFrame from a feather file (returns Polars).

    Uses Polars-native IPC/feather reading for maximum performance.

    Args:
        source_id: Source identifier (e.g., "DAT-00000001")
        sample: Override sample size (ignores session sample)

    Returns:
        Polars DataFrame loaded from feather file
    """
    config = get_config()
    file_path = os.path.join(config.data_path, f"{source_id}.feather")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")

    start_time = time.perf_counter()
    df = pl.read_ipc(file_path)
    original_rows = len(df)

    effective_sample = sample if sample is not None else get_sample_size()

    if effective_sample is not None and len(df) > effective_sample:
        df = df.head(effective_sample)

    duration_ms = (time.perf_counter() - start_time) * 1000

    session = get_active_session()
    if session is not None:
        session.record(
            operation="load_source",
            params={"source_id": source_id, "sample": effective_sample, "format": "polars"},
            rows_before=original_rows,
            rows_after=len(df),
            duration_ms=duration_ms,
            affected_columns=[],
            result_df=df.to_pandas(),
        )

    return df
