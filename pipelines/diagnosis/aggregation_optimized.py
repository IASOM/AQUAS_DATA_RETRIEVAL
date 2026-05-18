"""Optimized diagnosis pipeline with Parquet storage and partial incremental files."""
import hashlib
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

    id_columns = [
        code_col,
        f"DIAG_{code_col}",
        "DIAG_DIAG_CODE",
        "UP",
        "RS",
        "DIAG_UP",
        "DIAG_up_c",
        "DIAG_RS",
    ]
    for col in id_columns:
        if col in new_df.columns:
            id_parts.append(new_df[col].astype(str))

    if len(id_parts) == 1:
        value_cols = sorted(
            col
            for col in new_df.columns
            if col not in {timestamp_col, "data_id"}
        )
        schema_signature = hashlib.md5("|".join(value_cols).encode()).hexdigest()[:8]
        id_parts.append(pd.Series(schema_signature, index=new_df.index))

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

    df = _build_diagnosis_wide_final(df, timestamp_col)
    df = _merge_with_existing_diagnosis_final(df, final_store, timestamp_col)

    # Save final
    final_store.save_final(df, index_col=timestamp_col)

    logger.info(f"Aggregated diagnosis final: {len(df)} rows")

    return df


def _build_diagnosis_wide_final(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Build one diagnosis final row per timestamp from mixed incremental rows."""
    out = df.copy().drop(columns=["index", "data_id"], errors="ignore")

    if timestamp_col not in out.columns:
        if isinstance(out.index, pd.DatetimeIndex):
            out[timestamp_col] = out.index
        else:
            raise ValueError(f"Missing timestamp column: {timestamp_col}")

    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")

    frames = []
    wide_cols = [
        col
        for col in out.columns
        if col == "DIAG_TOTAL" or col.startswith("DIAG_CODE_")
    ]
    if wide_cols:
        frames.append(_sum_numeric_by_timestamp(out, wide_cols, timestamp_col))

    has_code_wide = any(col.startswith("DIAG_CODE_") for col in out.columns)
    if not has_code_wide and {"DIAG_DIAG_CODE", "DIAG_COUNT"}.issubset(out.columns):
        frames.append(
            _pivot_diagnosis_long(
                out,
                code_col="DIAG_DIAG_CODE",
                value_col="DIAG_COUNT",
                prefix="DIAG_CODE",
                timestamp_col=timestamp_col,
            )
        )

    group_specs = [
        ("DIAG_RS", "RS"),
        ("DIAG_UP", "UP"),
        ("DIAG_up_c", "UP"),
        ("RS", "RS"),
        ("UP", "UP"),
    ]
    for group_col, label in group_specs:
        required = {group_col, "DIAG_DIAG_CODE", "count"}
        if required.issubset(out.columns):
            frames.append(
                _pivot_diagnosis_group(
                    out,
                    group_col=group_col,
                    label=label,
                    timestamp_col=timestamp_col,
                )
            )

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    indexed = [frame.set_index(timestamp_col) for frame in frames]
    combined = pd.concat(indexed, axis=1).fillna(0)
    combined = combined.T.groupby(level=0).sum().T
    combined.index.name = timestamp_col
    return combined.sort_index().reset_index()


def _sum_numeric_by_timestamp(
    df: pd.DataFrame,
    value_cols: list[str],
    timestamp_col: str,
) -> pd.DataFrame:
    out = df[[timestamp_col, *value_cols]].copy()
    out[value_cols] = out[value_cols].apply(pd.to_numeric, errors="coerce")
    return (
        out.groupby(timestamp_col, as_index=False, observed=True)[value_cols]
        .sum(min_count=1)
        .fillna(0)
    )


def _pivot_diagnosis_long(
    df: pd.DataFrame,
    code_col: str,
    value_col: str,
    prefix: str,
    timestamp_col: str,
) -> pd.DataFrame:
    out = df[[timestamp_col, code_col, value_col]].dropna(subset=[code_col]).copy()
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce").fillna(0)
    out["feature"] = prefix + "_" + out[code_col].astype(str)

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values=value_col,
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index.name = timestamp_col
    return wide.reset_index()


def _pivot_diagnosis_group(
    df: pd.DataFrame,
    group_col: str,
    label: str,
    timestamp_col: str,
) -> pd.DataFrame:
    out = df[[timestamp_col, group_col, "DIAG_DIAG_CODE", "count"]].dropna(
        subset=[group_col, "DIAG_DIAG_CODE"]
    ).copy()
    out["count"] = pd.to_numeric(out["count"], errors="coerce").fillna(0)
    out["feature"] = (
        f"DIAG_{label}_"
        + out["DIAG_DIAG_CODE"].astype(str)
        + "_"
        + out[group_col].astype(str)
    )

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values="count",
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide.index.name = timestamp_col
    return wide.reset_index()


def _merge_with_existing_diagnosis_final(
    new_df: pd.DataFrame,
    final_store,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Merge new diagnosis final rows with the existing final parquet."""
    existing_df = final_store.load_final()
    if existing_df.empty:
        return new_df

    existing_df = _build_diagnosis_wide_final(existing_df, timestamp_col)
    if existing_df.empty:
        return new_df

    existing_idx = existing_df.set_index(timestamp_col)
    new_idx = new_df.set_index(timestamp_col)

    combined = new_idx.combine_first(existing_idx).sort_index().fillna(0)
    combined.index.name = timestamp_col
    return combined.reset_index()
