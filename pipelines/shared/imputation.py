"""Tail imputation helpers for asynchronous source refreshes."""
from __future__ import annotations

import pandas as pd
from typing import Optional

IMPUTED_COL = "__is_imputed"
IMPUTATION_METHOD_COL = "__imputation_method"
IMPUTATION_SOURCE_LAST_DATE_COL = "__imputation_source_last_date"
IMPUTATION_CREATED_AT_COL = "__imputation_created_at"

IMPUTATION_COLUMNS = {
    IMPUTED_COL,
    IMPUTATION_METHOD_COL,
    IMPUTATION_SOURCE_LAST_DATE_COL,
    IMPUTATION_CREATED_AT_COL,
}

SAME_MONTH_DAY_METHOD = "same_month_day_mean"


def is_imputed_series(values: pd.Series) -> pd.Series:
    """Return a boolean mask for rows marked as imputed."""
    if values.dtype == bool:
        return values.fillna(False)

    normalized = values.astype("string").str.strip().str.lower()
    return normalized.isin({"true", "1", "yes", "y"})


def drop_imputed_rows(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """Remove imputed rows so real values can replace them on later runs."""
    if df.empty or IMPUTED_COL not in df.columns:
        return df.copy()

    out = df.copy()
    out = out[~is_imputed_series(out[IMPUTED_COL])].copy()
    return out.drop(columns=list(IMPUTATION_COLUMNS), errors="ignore")


def add_observed_imputation_metadata(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Add tracking columns for observed rows."""
    out = df.copy()
    out[IMPUTED_COL] = False
    out[IMPUTATION_METHOD_COL] = ""
    out[IMPUTATION_SOURCE_LAST_DATE_COL] = ""
    out[IMPUTATION_CREATED_AT_COL] = ""
    return out


def impute_tail_to_date(
    df: pd.DataFrame,
    observed_until: Optional[pd.Timestamp],
    target_until: Optional[pd.Timestamp] = None,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    """
    Complete a final daily dataframe through target_until with tracked estimates.

    Real rows are kept as observed. Any missing calendar day from the first
    observed day through target_until is imputed. Values are column-wise averages
    from the same calendar month/day in previous observed years. If a column has
    no same-month/day history, the column's observed mean is used as a fallback.
    """
    if df.empty:
        return add_observed_imputation_metadata(df)

    target_day = (
        pd.Timestamp.today().normalize()
        if target_until is None
        else pd.to_datetime(target_until).normalize()
    )
    today = pd.Timestamp.today().normalize()
    target_day = min(target_day, today)

    real = drop_imputed_rows(df, timestamp_col=timestamp_col)
    if real.empty:
        return add_observed_imputation_metadata(real)

    real = real.copy()
    real[timestamp_col] = pd.to_datetime(real[timestamp_col], errors="coerce").dt.floor("D")
    real = real.dropna(subset=[timestamp_col]).sort_values(timestamp_col)

    if observed_until is None or pd.isna(observed_until):
        observed_day = real[timestamp_col].max()
    else:
        observed_day = pd.to_datetime(observed_until).normalize()

    observed_day = min(observed_day, target_day, real[timestamp_col].max())
    real = real[real[timestamp_col] <= observed_day].copy()
    if real.empty:
        return add_observed_imputation_metadata(real)

    numeric_cols = [
        col
        for col in real.columns
        if col != timestamp_col and col not in IMPUTATION_COLUMNS
    ]
    for col in numeric_cols:
        real[col] = pd.to_numeric(real[col], errors="coerce")

    real = add_observed_imputation_metadata(real)

    first_day = real[timestamp_col].min()
    existing_days = set(real[timestamp_col])
    candidate_dates = pd.date_range(first_day, target_day, freq="D")
    imputed_dates = [
        day
        for day in candidate_dates
        if day not in existing_days
    ]

    if not imputed_dates:
        return real.sort_values(timestamp_col).reset_index(drop=True)

    history = real.set_index(timestamp_col)
    fallback_means = history[numeric_cols].mean(skipna=True)
    imputed_rows = []
    created_at = pd.Timestamp.now().isoformat()
    source_last_date = observed_day.date().isoformat()

    for impute_day in imputed_dates:
        same_day_history = history[
            (history.index.month == impute_day.month)
            & (history.index.day == impute_day.day)
            & (history.index < impute_day)
        ]
        if same_day_history.empty:
            values = fallback_means.copy()
        else:
            values = same_day_history[numeric_cols].mean(skipna=True)
            values = values.fillna(fallback_means)

        row = values.fillna(0).to_dict()
        row[timestamp_col] = impute_day
        row[IMPUTED_COL] = True
        row[IMPUTATION_METHOD_COL] = SAME_MONTH_DAY_METHOD
        row[IMPUTATION_SOURCE_LAST_DATE_COL] = source_last_date
        row[IMPUTATION_CREATED_AT_COL] = created_at
        imputed_rows.append(row)

    imputed = pd.DataFrame(imputed_rows)
    combined = pd.concat([real, imputed], ignore_index=True, sort=False)
    return combined.sort_values(timestamp_col).reset_index(drop=True)
