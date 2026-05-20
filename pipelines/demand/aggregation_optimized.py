"""Optimized demand pipeline with Parquet storage and partial incremental files."""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def build_daily_total_cat_optimized(
    df: pd.DataFrame,
    date_column: str = "timestamp",
    value_col: str = "counts",
) -> pd.DataFrame:
    """
    Efficiently build daily category totals using vectorized operations.

    Args:
        df: Input DataFrame
        date_column: Timestamp column
        value_col: Value column to aggregate

    Returns:
        Daily totals DataFrame
    """
    df = df.copy()

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    if value_col not in df.columns:
        df[value_col] = 1

    # Vectorized groupby - much faster than iterating
    result = df.groupby(date_column, observed=True)[value_col].sum().to_frame()
    result.columns = ["DEMANDA_TOTAL"]

    return result


def build_daily_features_by_group_optimized(
    df: pd.DataFrame,
    group_col: str,
    date_column: str = "timestamp",
    value_col: str = "counts",
    prefix: str = "demanda",
) -> pd.DataFrame:
    """
    Efficiently build daily features grouped by category using vectorized operations.

    Args:
        df: Input DataFrame
        group_col: Column to group by
        date_column: Timestamp column
        value_col: Value column to aggregate
        prefix: Prefix for column names

    Returns:
        Wide DataFrame with daily features
    """
    df = df.copy()

    if value_col not in df.columns:
        df[value_col] = 1

    if group_col not in df.columns:
        if "UP" in df.columns:
            df[group_col] = df["UP"]
        else:
            df[group_col] = "NA"

    # Ensure datetime
    df[date_column] = pd.to_datetime(df[date_column]).dt.floor("D")

    # Select and clean only needed columns
    cols_to_use = [
        date_column,
        group_col,
        "VISI_LLOC_VISITA",
        "VISI_SITUACIO_VISITA",
        "SERVEI_CODI",
        "TIPUS_CLASS",
        "TIPUS_VISITA_AGRUPAT",
        value_col,
    ]

    df = df[[c for c in cols_to_use if c in df.columns]].copy()

    pieces = []

    # Process each categorical variable efficiently
    categorical_vars = [
        col
        for col in [
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "TIPUS_VISITA_AGRUPAT",
        ]
        if col in df.columns
    ]

    for var in categorical_vars:
        # Vectorized string operations
        tmp = df[[date_column, group_col, var, value_col]].copy()

        # Fast string cleaning
        tmp[group_col] = (
            tmp[group_col].fillna("NA").astype(str).str.strip().replace("", "NA")
        )
        tmp[var] = (
            tmp[var].fillna("NA").astype(str).str.strip().replace("", "NA")
        )

        # Vectorized column creation
        tmp["feature"] = f"{prefix}_{var}_" + tmp[var] + "_" + tmp[group_col]

        # Efficient groupby
        tmp = (
            tmp.groupby([date_column, "feature"], as_index=False, observed=True)[
                value_col
            ]
            .sum()
        )

        pieces.append(tmp)

    # Total per group
    tmp_total = df[[date_column, group_col, value_col]].copy()
    tmp_total[group_col] = tmp_total[group_col].astype(str).str.strip()
    tmp_total["feature"] = f"{prefix}__TOTAL_{group_col}_" + tmp_total[group_col]
    tmp_total = (
        tmp_total.groupby([date_column, "feature"], as_index=False, observed=True)[
            value_col
        ]
        .sum()
    )

    pieces.append(tmp_total)

    # Concatenate all pieces
    long_df = pd.concat(pieces, ignore_index=True)

    # Efficient pivot with memory optimization
    wide = long_df.pivot_table(
        index=date_column,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )

    wide.index = pd.to_datetime(wide.index)

    # Add timestamp column
    wide["timestamp"] = wide.index

    return wide.sort_index()


def add_incremental_optimized(
    new_df: pd.DataFrame,
    manager,
    timestamp_col: str = "timestamp",
) -> None:
    """
    Add new data to incremental storage with deduplication.

    Args:
        new_df: New data to add
        manager: ParquetIncrementalManager instance
        timestamp_col: Timestamp column
    """
    if new_df.empty:
        return

    # Ensure timestamp column
    if timestamp_col not in new_df.columns:
        new_df[timestamp_col] = pd.Timestamp.now()

    new_df[timestamp_col] = pd.to_datetime(new_df[timestamp_col])

    # Add to manager (handles deduplication and retention)
    manager.add_data(new_df, timestamp_col=timestamp_col)


def aggregate_final_optimized(
    incremental_manager,
    final_store,
    timestamp_col: str = "timestamp",
    with_range: Optional[tuple] = None,
) -> pd.DataFrame:
    """
    Efficiently aggregate incremental data to final output.

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
        logger.warning("No incremental data to aggregate")
        return pd.DataFrame()

    # Filter by range if specified
    if with_range:
        start, end = with_range
        df = df[(df[timestamp_col] >= start) & (df[timestamp_col] <= end)]

    df = _build_wide_final_by_timestamp(df, timestamp_col)
    df = _merge_with_existing_final(df, final_store, timestamp_col)

    # Save final
    final_store.save_final(df, index_col=timestamp_col)

    return df


def _build_wide_final_by_timestamp(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Combine partial wide incremental rows into one row per timestamp."""
    out = df.copy().drop(columns=["index"], errors="ignore")

    if timestamp_col not in out.columns:
        if isinstance(out.index, pd.DatetimeIndex):
            out[timestamp_col] = out.index
        else:
            raise ValueError(f"Missing timestamp column: {timestamp_col}")

    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")

    value_cols = [col for col in out.columns if col != timestamp_col]
    out[value_cols] = out[value_cols].apply(pd.to_numeric, errors="coerce")

    return (
        out.groupby(timestamp_col, as_index=False, observed=True)[value_cols]
        .sum(min_count=1)
        .fillna(0)
        .sort_values(timestamp_col)
    )


def _merge_with_existing_final(
    new_df: pd.DataFrame,
    final_store,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Merge new final rows with the existing final parquet, if present."""
    existing_df = final_store.load_final()
    if existing_df.empty:
        return new_df

    existing_df = _build_wide_final_by_timestamp(existing_df, timestamp_col)

    existing_idx = existing_df.set_index(timestamp_col)
    new_idx = new_df.set_index(timestamp_col)

    combined = new_idx.combine_first(existing_idx).sort_index().fillna(0)
    combined.index.name = timestamp_col
    return combined.reset_index()
