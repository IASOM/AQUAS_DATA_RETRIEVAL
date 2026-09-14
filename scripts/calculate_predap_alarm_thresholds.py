"""Calculate PREDAP alarm thresholds from a final Parquet feature matrix."""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


WINDOWS = [7, 14, 30, 60, 182, 365]
BASE_YELLOW = {7: 80, 14: 60, 30: 40, 60: 30, 182: 20, 365: 15}
BASE_RED = {7: 150, 14: 120, 30: 90, 60: 70, 182: 45, 365: 35}
IMPUTED_TRUE_VALUES = {"true", "1", "yes", "y"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Calculate PREDAP alarm thresholds from final Parquet outputs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/finals/demand_diagnosis_joined.parquet"),
        help="Input final Parquet file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("predap_alarm_thresholds.csv"),
        help="Output CSV with one row per predicted unit.",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Number of observed historical years used for calibration.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(
            f"Input Parquet not found: {args.input}. Run the pipelines first."
        )

    thresholds = calculate_thresholds(args.input, years=args.years)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    thresholds.to_csv(args.output, index=False)
    print(f"Wrote {len(thresholds)} rows to {args.output}")
    return 0


def calculate_thresholds(input_file: Path, years: int = 5) -> pd.DataFrame:
    df = pd.read_parquet(input_file)
    if "timestamp" not in df.columns:
        raise ValueError(f"Missing timestamp column in {input_file}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = _drop_imputed_rows(df)
    if df.empty:
        raise ValueError("No observed rows available after excluding imputed rows.")

    last_day = df["timestamp"].max().normalize()
    start_day = last_day - pd.DateOffset(years=years) + pd.Timedelta(days=1)
    hist = df[(df["timestamp"] >= start_day) & (df["timestamp"] <= last_day)].copy()
    if hist.empty:
        raise ValueError(f"No rows found in the last {years} observed years.")

    units = [
        col
        for col in hist.columns
        if _is_predicted_unit_column(hist, col)
    ]

    rows = []
    for unit in units:
        series = (
            hist[["timestamp", unit]]
            .dropna()
            .set_index("timestamp")[unit]
            .astype(float)
            .sort_index()
        )
        if series.empty:
            continue

        n_p95 = float(series.quantile(0.95))
        top5 = series[series >= n_p95]
        parsed = _parse_unit_name(unit)
        row = {
            "unit": unit,
            **parsed,
            "last_observed_date": last_day.date().isoformat(),
            "history_start_date": start_day.date().isoformat(),
            "history_days": int(series.shape[0]),
            "n_p95_5y": n_p95,
            "n_top5_days_5y": int(top5.shape[0]),
            "n_top5_sum_5y": float(top5.sum()),
            "n_top5_mean_5y": float(top5.mean()) if not top5.empty else 0.0,
            "n_median_5y": float(series.median()),
        }

        for window in WINDOWS:
            _add_window_thresholds(row, series, window)

        row["quality_flag"] = _quality_flag(series)
        rows.append(row)

    return pd.DataFrame(rows).sort_values(["domain", "geo_level", "geo_id", "unit"])


def _drop_imputed_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "__is_imputed" not in df.columns:
        return df

    imputed = (
        df["__is_imputed"]
        .astype("string")
        .str.strip()
        .str.lower()
        .isin(IMPUTED_TRUE_VALUES)
    )
    return df[~imputed].copy()


def _is_predicted_unit_column(df: pd.DataFrame, column: str) -> bool:
    if column == "timestamp" or column.startswith("__"):
        return False
    if not pd.api.types.is_numeric_dtype(df[column]):
        return False
    return column.startswith("DEMAND__") or column.startswith("DIAGNOSIS__")


def _parse_unit_name(unit: str) -> dict[str, str]:
    parts = unit.split("__")
    domain = parts[0] if parts else ""
    geo_level = ""
    geo_id = ""
    diagnosis_group = ""

    if "__RS__" in unit:
        geo_level = "RS"
        geo_id = unit.rsplit("__RS__", 1)[1]
    elif "__UP__" in unit:
        geo_level = "UP"
        geo_id = unit.rsplit("__UP__", 1)[1]
    elif unit.endswith("__TOTAL") or "__TOTAL" in unit:
        geo_level = "CAT"
        geo_id = "CAT"

    match = re.match(r"^DIAGNOSIS__ICD10_3__(.+?)(?:__(?:RS|UP)__.+)?$", unit)
    if match:
        diagnosis_group = match.group(1)

    return {
        "domain": domain,
        "geo_level": geo_level,
        "geo_id": geo_id,
        "diagnosis_group": diagnosis_group,
    }


def _add_window_thresholds(
    row: dict[str, object],
    series: pd.Series,
    window: int,
) -> None:
    recent = series.rolling(window, min_periods=window).sum()
    previous = recent.shift(window)
    n_p95_window = (
        float(recent.dropna().quantile(0.95)) if not recent.dropna().empty else 0.0
    )
    top5_window = recent[recent >= n_p95_window].dropna()
    observed_previous = previous.dropna()
    min_baseline = (
        max(5.0, float(observed_previous.quantile(0.25)))
        if not observed_previous.empty
        else 5.0
    )
    growth = 100 * (recent - previous) / previous.clip(lower=min_baseline)
    positive_growth = growth[growth > 0].dropna()

    p95_growth = (
        float(positive_growth.quantile(0.95)) if not positive_growth.empty else 0.0
    )
    p99_growth = (
        float(positive_growth.quantile(0.99)) if not positive_growth.empty else 0.0
    )

    row[f"n_p95_{window}d_5y"] = n_p95_window
    row[f"n_top5_windows_{window}d_5y"] = int(top5_window.shape[0])
    row[f"n_top5_sum_{window}d_5y"] = float(top5_window.sum())
    row[f"n_top5_mean_{window}d_5y"] = (
        float(top5_window.mean()) if not top5_window.empty else 0.0
    )
    row[f"growth_yellow_{window}d"] = float(max(BASE_YELLOW[window], p95_growth))
    row[f"growth_red_{window}d"] = float(max(BASE_RED[window], p99_growth))
    row[f"min_recent_{window}d"] = float(max(10.0, 0.25 * n_p95_window))
    row[f"min_baseline_{window}d"] = min_baseline


def _quality_flag(series: pd.Series) -> str:
    if series.shape[0] < 365 * 3:
        return "low_history"
    if series.sum() < 100:
        return "low_volume"
    recent = series.tail(30)
    if recent.shape[0] < 30:
        return "missing_recent_data"
    return "ok"


if __name__ == "__main__":
    raise SystemExit(main())
