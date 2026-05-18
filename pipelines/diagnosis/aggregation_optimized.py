"""Optimized diagnosis pipeline with Parquet storage and partial incremental files."""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def build_daily_diagnosis_counts_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily diagnosis code counts using vectorized operations.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Daily diagnosis counts DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Vectorized groupby
    result = (
        df.groupby([date_column, code_column], observed=True)[value_col]
        .sum()
        .reset_index()
    )
    result.columns = [date_column, f"DIAG_{code_column}", "DIAG_COUNT"]

    return result


def build_daily_diagnosis_by_group_optimized(
    df: pd.DataFrame,
    group_col: str,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily diagnosis by group using vectorized operations.

    Args:
        df: Input DataFrame
        group_col: Group column (e.g., RS, UP)
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Daily diagnosis by group DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Select needed columns only
    cols_to_use = [date_column, group_col, code_column, value_col]
    df = df[[c for c in cols_to_use if c in df.columns]].copy()

    # Vectorized groupby
    result = (
        df.groupby([date_column, group_col, code_column], observed=True)[value_col]
        .sum()
        .reset_index()
    )

    # Rename for clarity
    result.columns = [date_column, f"DIAG_{group_col}", f"DIAG_{code_column}", "count"]

    return result


def build_daily_total_general_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build daily total diagnosis counts.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        value_col: Count column

    Returns:
        Daily totals DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Vectorized groupby
    result = df.groupby(date_column, observed=True)[value_col].sum().to_frame()
    result.columns = ["DIAG_TOTAL"]

    return result


def build_diagnosis_wide_format_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    code_column: str = "DIAG_CODE",
    value_col: str = "n",
) -> pd.DataFrame:
    """
    Efficiently build wide format diagnosis matrix (codes as columns).

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        code_column: Diagnosis code column
        value_col: Count column

    Returns:
        Wide DataFrame with diagnosis codes as columns
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Efficient pivot
    result = df.pivot_table(
        index=date_column,
        columns=code_column,
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )

    # Rename columns
    result.columns = [f"DIAG_CODE_{col}" for col in result.columns]

    result.index = pd.to_datetime(result.index)

    # Add timestamp column
    result["timestamp"] = result.index

    return result.sort_index()


def add_incremental_diagnosis_optimized(
    new_df: pd.DataFrame,
    manager,
    timestamp_col: str = "timestamp",
    code_col: str = "DIAG_CODE",
) -> None:
    """
    Add new diagnosis data to incremental storage with deduplication.

    Args:
        new_df: New data to add
        manager: ParquetIncrementalManager instance
        timestamp_col: Timestamp column
        code_col: Diagnosis code column
    """
    if new_df.empty:
        return

    # Ensure timestamp column
    if timestamp_col not in new_df.columns:
        new_df[timestamp_col] = pd.Timestamp.now()

    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])

    id_parts = [new_df[timestamp_col].astype(str)]

    if code_col in new_df.columns:
        id_parts.append(new_df[code_col].astype(str))

    if "UP" in new_df.columns:
        id_parts.append(new_df["UP"].astype(str))
    elif "RS" in new_df.columns:
        id_parts.append(new_df["RS"].astype(str))

    new_df["data_id"] = id_parts[0]
    for part in id_parts[1:]:
        new_df["data_id"] = new_df["data_id"] + "_" + part

    manager.add_data(new_df, timestamp_col=timestamp_col)


def aggregate_diagnosis_final_optimized(
    incremental_manager,
    final_store,
    timestamp_col: str = "timestamp",
    with_range: Optional[tuple] = None,
) -> pd.DataFrame:
    """
    Efficiently aggregate incremental diagnosis data to final output.

    Args:
        incremental_manager: ParquetIncrementalManager instance
        final_store: ParquetFinalStore instance
        timestamp_col: Timestamp column
        with_range: Optional (start_date, end_date) to filter

    Returns:
        Aggregated DataFrame
    """
    # Load all incremental data
    df = incremental_manager.load_all_incremental(timestamp_col)

    if df.empty:
        logger.warning("No diagnosis incremental data to aggregate")
        return pd.DataFrame()

    # Filter by range if specified
    if with_range:
        start, end = with_range
        df = df[(df[timestamp_col] >= start) & (df[timestamp_col] <= end)]

    # Sort and deduplicate
    df = df.sort_values(timestamp_col)
    df = df.drop_duplicates(subset=["data_id"], keep="last")

    # Save final
    final_store.save_final(df, index_col=timestamp_col)

    logger.info(f"Aggregated diagnosis final: {len(df)} rows")

    return df
