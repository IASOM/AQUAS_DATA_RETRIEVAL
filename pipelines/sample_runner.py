"""Run the pipelines against bundled synthetic CSV data."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from pipelines.demand.aggregation_optimized import (
    build_daily_features_by_group_optimized,
    build_daily_features_global_optimized,
    build_daily_total_cat_optimized,
)
from pipelines.demand.transformations import prepare_visits_chunk
from pipelines.diagnosis.aggregation_optimized import (
    build_daily_diagnosis_by_group_optimized,
    build_daily_total_general_optimized,
    build_diagnosis_wide_format_optimized,
)
from pipelines.shared.final_joiner import FinalDataJoiner
from pipelines.shared.parquet_storage import drop_future_timestamp_rows


SAMPLE_INPUT_FILES = {
    "up_rs": "up_rs.csv",
    "demand": "demand_visits.csv",
    "diagnosis": "diagnosis_visits.csv",
    "selected_codes": "selected_codes.csv",
}


def run_sample_demand_pipeline(
    input_dir: str | Path,
    output_dir: str | Path,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> Path:
    """Run the demand pipeline with local synthetic input data."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    _ensure_sample_files(input_dir, ["up_rs", "demand"])

    up_rs = _load_up_rs(input_dir)
    visits = pd.read_csv(
        input_dir / SAMPLE_INPUT_FILES["demand"],
        dtype={"UP": str},
    )

    visits = prepare_visits_chunk(visits, up_rs=up_rs)
    visits = _filter_by_date_range(visits, "DATA_VISITA", start_date, end_date)
    visits = visits[visits["DATA_VISITA"] < _tomorrow()].copy()
    visits["timestamp"] = visits["DATA_VISITA"]

    cat_daily = build_daily_total_cat_optimized(visits)
    global_daily = build_daily_features_global_optimized(visits)
    rs_daily = build_daily_features_by_group_optimized(visits, group_col="RS")
    up_daily = build_daily_features_by_group_optimized(visits, group_col="UP")

    incremental_dir = output_dir / "demand_pipeline" / "incremental"
    _save_parquet(_with_timestamp_column(cat_daily), incremental_dir / "demand_cat_daily.parquet")
    _save_parquet(_with_timestamp_column(global_daily), incremental_dir / "demand_global_daily.parquet")
    _save_parquet(_with_timestamp_column(rs_daily), incremental_dir / "demand_rs_daily.parquet")
    _save_parquet(_with_timestamp_column(up_daily), incremental_dir / "demand_up_daily.parquet")

    final = _combine_wide_frames([cat_daily, global_daily, rs_daily, up_daily])
    final_path = output_dir / "demand_pipeline" / "finals" / "demand_final.parquet"
    _save_parquet(final, final_path)
    return final_path


def run_sample_diagnosis_pipeline(
    input_dir: str | Path,
    output_dir: str | Path,
    start_date: pd.Timestamp | None = None,
    end_date: pd.Timestamp | None = None,
) -> Path:
    """Run the diagnosis pipeline with local synthetic input data."""
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    _ensure_sample_files(input_dir, ["up_rs", "diagnosis", "selected_codes"])

    up_rs = _load_up_rs(input_dir)
    diagnosis = pd.read_csv(
        input_dir / SAMPLE_INPUT_FILES["diagnosis"],
        dtype={"up_c": str, "problema_salut_c": str},
    )
    selected_codes = _load_selected_codes(input_dir)

    diagnosis["problema_salut_c"] = _normalize_diag_codes(diagnosis["problema_salut_c"])
    if selected_codes:
        diagnosis = diagnosis[diagnosis["problema_salut_c"].isin(selected_codes)].copy()
        diagnosis["problema_salut_c"] = diagnosis["problema_salut_c"].map(selected_codes)

    diagnosis["timestamp"] = pd.to_datetime(
        diagnosis["data_visita"],
        errors="coerce",
    ).dt.floor("D")
    diagnosis = diagnosis.dropna(subset=["timestamp"])
    diagnosis = _filter_by_date_range(diagnosis, "timestamp", start_date, end_date)
    diagnosis = diagnosis[diagnosis["timestamp"] < _tomorrow()].copy()
    diagnosis["up_c"] = diagnosis["up_c"].astype(str).str.zfill(5)
    diagnosis["n"] = 1
    diagnosis = diagnosis.rename(columns={"problema_salut_c": "DIAG_CODE"})

    up_rs_map = up_rs[["Codi UP", "RS"]].copy()
    up_rs_map["Codi UP"] = up_rs_map["Codi UP"].astype(str).str.zfill(5)
    up_rs_map = up_rs_map.rename(columns={"Codi UP": "up_c"})
    diagnosis = diagnosis.merge(up_rs_map, on="up_c", how="left")
    diagnosis["RS"] = diagnosis["RS"].fillna("UNKNOWN")

    total_daily = build_daily_total_general_optimized(diagnosis)
    code_daily = build_diagnosis_wide_format_optimized(diagnosis)
    rs_long = build_daily_diagnosis_by_group_optimized(diagnosis, group_col="RS")
    up_long = build_daily_diagnosis_by_group_optimized(diagnosis, group_col="up_c")

    rs_wide = _pivot_diagnosis_group(rs_long, group_column="DIAG_RS", label="RS")
    up_wide = _pivot_diagnosis_group(up_long, group_column="DIAG_up_c", label="UP")

    incremental_dir = output_dir / "diagnosis_pipeline" / "incremental"
    _save_parquet(_with_timestamp_column(total_daily), incremental_dir / "diagnosis_total_daily.parquet")
    _save_parquet(_with_timestamp_column(code_daily), incremental_dir / "diagnosis_code_daily.parquet")
    _save_parquet(rs_long, incremental_dir / "diagnosis_rs_long.parquet")
    _save_parquet(up_long, incremental_dir / "diagnosis_up_long.parquet")

    final = _combine_wide_frames([total_daily, code_daily, rs_wide, up_wide])
    final_path = output_dir / "diagnosis_pipeline" / "finals" / "diagnosis_final.parquet"
    _save_parquet(final, final_path)
    return final_path


def join_sample_outputs(output_dir: str | Path) -> Path:
    """Join sample demand and diagnosis final outputs."""
    output_dir = Path(output_dir)
    demand_path = output_dir / "demand_pipeline" / "finals" / "demand_final.parquet"
    diagnosis_path = output_dir / "diagnosis_pipeline" / "finals" / "diagnosis_final.parquet"
    joined_path = output_dir / "finals" / "demand_diagnosis_joined.parquet"

    joiner = FinalDataJoiner(
        demand_final_file=demand_path,
        diagnosis_final_file=diagnosis_path,
        output_file=joined_path,
    )
    return joiner.join_and_save(
        demand_prefix="DEMAND",
        diagnosis_prefix="DIAGNOSIS",
        fill_method="ffill",
        compression="snappy",
    )


def _ensure_sample_files(input_dir: Path, keys: Iterable[str]) -> None:
    missing = [
        input_dir / SAMPLE_INPUT_FILES[key]
        for key in keys
        if not (input_dir / SAMPLE_INPUT_FILES[key]).exists()
    ]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing sample input files: {missing_text}")


def _load_up_rs(input_dir: Path) -> pd.DataFrame:
    up_rs = pd.read_csv(input_dir / SAMPLE_INPUT_FILES["up_rs"], dtype={"Codi UP": str})
    up_rs["Codi UP"] = up_rs["Codi UP"].astype(str).str.zfill(5)
    return up_rs


def _load_selected_codes(input_dir: Path) -> dict[str, str]:
    path = input_dir / SAMPLE_INPUT_FILES["selected_codes"]
    if not path.exists():
        return {}
    selected = pd.read_csv(path, dtype=str)
    if selected.empty:
        return {}

    aliases = {}
    alias_col = selected.columns[1] if len(selected.columns) > 1 else None
    for _, row in selected.iterrows():
        code = _normalize_diag_codes(pd.Series([row.iloc[0]])).iloc[0]
        if pd.isna(code) or not code:
            continue
        alias = _normalize_feature_name(row[alias_col], fallback=code) if alias_col else code
        aliases[code] = alias
    return aliases


def _normalize_diag_codes(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip().str.upper().str[:3]


def _normalize_feature_name(value, fallback: str) -> str:
    if pd.isna(value):
        return fallback
    token = str(value).strip()
    if not token:
        return fallback
    token = "".join(char if char.isalnum() else "_" for char in token)
    token = "_".join(part for part in token.upper().split("_") if part)
    return token or fallback


def _tomorrow() -> pd.Timestamp:
    return pd.Timestamp.today().normalize() + pd.Timedelta(days=1)


def _filter_by_date_range(
    df: pd.DataFrame,
    timestamp_col: str,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], errors="coerce").dt.floor("D")
    out = out.dropna(subset=[timestamp_col])

    if start_date is not None:
        out = out[out[timestamp_col] >= pd.to_datetime(start_date).normalize()]
    if end_date is not None:
        end_exclusive = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1)
        out = out[out[timestamp_col] < end_exclusive]

    return out.copy()


def _with_timestamp_column(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy()
    if timestamp_col in out.columns:
        out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
        return out.reset_index(drop=True)

    out.index = pd.to_datetime(out.index).floor("D")
    out.index.name = timestamp_col
    return out.reset_index()


def _combine_wide_frames(
    frames: Iterable[pd.DataFrame],
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    indexed = [_as_timestamp_index(frame, timestamp_col) for frame in frames]
    combined = pd.concat(indexed, axis=1).fillna(0).sort_index()
    combined.index.name = timestamp_col
    return drop_future_timestamp_rows(combined.reset_index(), timestamp_col)


def _as_timestamp_index(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy().drop(columns=["index"], errors="ignore")

    if timestamp_col in out.columns:
        out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
        out = out.set_index(timestamp_col)
    else:
        out.index = pd.to_datetime(out.index).floor("D")

    return out.groupby(level=0).sum(numeric_only=True).sort_index()


def _pivot_diagnosis_group(
    df: pd.DataFrame,
    group_column: str,
    label: str,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col]).dt.floor("D")
    out[group_column] = out[group_column].fillna("UNKNOWN").astype(str)
    out["DIAG_DIAG_CODE"] = out["DIAG_DIAG_CODE"].fillna("UNKNOWN").astype(str)
    out["feature"] = (
        f"DIAG_{label}_"
        + out["DIAG_DIAG_CODE"]
        + "_"
        + out[group_column]
    )

    wide = out.pivot_table(
        index=timestamp_col,
        columns="feature",
        values="count",
        aggfunc="sum",
        fill_value=0,
        observed=True,
    )
    wide["timestamp"] = wide.index
    return wide


def _save_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = drop_future_timestamp_rows(df, "timestamp")
    df.to_parquet(path, compression="snappy", index=False)

