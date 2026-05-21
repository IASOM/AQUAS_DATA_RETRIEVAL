"""Optimized diagnosis pipeline main runner with Parquet storage."""
import pandas as pd
import logging
import re
from pathlib import Path
from typing import Optional

from pipelines.shared import (
    get_connection,
    setup_logging,
    get_min_max_date,
    get_year_ranges,
    get_incremental_processing_window,
    latest_timestamp,
)
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_diagnosis_counts_optimized,
    build_daily_diagnosis_by_group_optimized,
    build_daily_total_by_group_optimized,
    build_daily_total_general_optimized,
    aggregate_diagnosis_final_optimized,
    _build_diagnosis_wide_final,
)

logger = setup_logging()


def validate_table_columns(
    conn,
    schema: str,
    table_name: str,
    required_columns: list[str],
) -> None:
    """Validate that the required columns exist in the target table."""
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    existing = pd.read_sql_query(query, conn, params=[schema, table_name])
    existing_columns = {str(c).upper() for c in existing["COLUMN_NAME"].tolist()}

    missing = [col for col in required_columns if str(col).upper() not in existing_columns]
    if missing:
        raise ValueError(
            f"Missing columns in {schema}.{table_name}: {missing}. "
            f"Available columns: {sorted(existing_columns)}"
        )


def get_diagnosis_data_for_year_optimized(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str,
    diag_code_column: str,
    year_start: pd.Timestamp,
    year_end: pd.Timestamp,
    last_loaded_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Query diagnosis data already aggregated by day, UP, and code prefix."""
    date_expr = f"CAST([{date_column}] AS date)"
    code_expr = (
        f"UPPER(LEFT(LTRIM(RTRIM(CAST([{diag_code_column}] AS VARCHAR(50)))), 3))"
    )

    if last_loaded_date is None:
        query = f"""
        SELECT
            {date_expr} AS [timestamp],
            [{up_column}] AS [{up_column}],
            {code_expr} AS [DIAG_CODE],
            COUNT_BIG(*) AS [n]
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{diag_code_column}] IS NOT NULL
        GROUP BY {date_expr}, [{up_column}], {code_expr}
        ORDER BY [timestamp] ASC
        """
        params = [year_start, year_end]
    else:
        query = f"""
        SELECT
            {date_expr} AS [timestamp],
            [{up_column}] AS [{up_column}],
            {code_expr} AS [DIAG_CODE],
            COUNT_BIG(*) AS [n]
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{date_column}] > ?
            AND [{diag_code_column}] IS NOT NULL
        GROUP BY {date_expr}, [{up_column}], {code_expr}
        ORDER BY [timestamp] ASC
        """
        params = [year_start, year_end, last_loaded_date]

    return pd.read_sql_query(query, conn, params=params)


def _normalize_diag_codes(values: pd.Series) -> pd.Series:
    """Normalize diagnosis values to the ICD10 3-character code used by filters."""
    return values.astype("string").str.strip().str.upper().str[:3]


def _normalize_up_codes(values: pd.Series) -> pd.Series:
    """Normalize UP codes so they match the Excel mapping."""
    return values.astype("string").str.strip().str.zfill(5)


def _normalize_rs_values(values: pd.Series) -> pd.Series:
    """Normalize RS labels for stable matching against selected RS files."""
    return values.astype("string").str.strip().str.upper()


def _normalize_feature_name(value, fallback: str) -> str:
    """Build a stable column-name token from an optional human label."""
    if pd.isna(value):
        return fallback

    token = str(value).strip()
    if not token:
        return fallback

    token = re.sub(r"[^0-9A-Za-z]+", "_", token).strip("_").upper()
    return token or fallback


def _load_selection_values(
    selection_file: Optional[str | Path],
    filename: str,
    label: str,
    normalizer,
    legacy_filename: Optional[str] = None,
    legacy_subdir: Optional[str] = None,
) -> Optional[set[str]]:
    """Load a one-column selection CSV from the shared selections folder."""
    candidates = []
    if selection_file:
        active_path = Path(selection_file)
        candidates.append(active_path)
        resolved = active_path.resolve()
        for parent_index in (1, 3):
            try:
                base_dir = resolved.parents[parent_index]
            except IndexError:
                continue
            candidates.append(base_dir / "selections" / filename)
            if legacy_subdir and legacy_filename:
                candidates.append(
                    base_dir / "diagnosis_pipeline" / legacy_subdir / legacy_filename
                )

    candidates.append(Path.cwd() / "selections" / filename)
    if legacy_subdir and legacy_filename:
        candidates.append(Path.cwd() / "diagnosis_pipeline" / legacy_subdir / legacy_filename)

    seen = set()
    found_file = False
    for path in candidates:
        path = Path(path)
        path_key = path.resolve()
        if path_key in seen:
            continue
        seen.add(path_key)

        if not path.exists():
            continue

        found_file = True
        selection_df = pd.read_csv(path)
        values = set(normalizer(selection_df.iloc[:, 0]).dropna())
        values.discard("")
        if not values:
            logger.warning(f"Selected {label} file is empty: {path}")
            continue

        logger.info(f"Loaded {len(values)} selected {label} from {path}")
        return values

    if found_file:
        logger.info(f"No selected {label} configured in selections/{filename}.")
    else:
        logger.warning(
            f"No selected {label} file found. Expected "
            f"selections/{filename}."
        )
    return None


def _load_selected_codes(
    selected_codes_file: Optional[str | Path],
) -> Optional[dict[str, str]]:
    """Load selected diagnosis-code prefixes mapped to output feature aliases."""
    candidates = []
    if selected_codes_file:
        active_path = Path(selected_codes_file)
        candidates.append(active_path)
        resolved = active_path.resolve()
        for parent_index in (1, 3):
            try:
                base_dir = resolved.parents[parent_index]
            except IndexError:
                continue
            candidates.append(base_dir / "selections" / "selected_diagnosis_codes.csv")
            candidates.append(
                base_dir
                / "diagnosis_pipeline"
                / "selected_codes"
                / "selected_codes.csv"
            )

    candidates.append(Path.cwd() / "selections" / "selected_diagnosis_codes.csv")
    candidates.append(
        Path.cwd() / "diagnosis_pipeline" / "selected_codes" / "selected_codes.csv"
    )

    seen = set()
    found_file = False
    for path in candidates:
        path = Path(path)
        path_key = path.resolve()
        if path_key in seen:
            continue
        seen.add(path_key)

        if not path.exists():
            continue

        found_file = True
        selected_df = pd.read_csv(path)
        if selected_df.empty:
            logger.warning(f"Selected diagnosis codes file is empty: {path}")
            continue

        code_aliases = {}
        alias_col = selected_df.columns[1] if len(selected_df.columns) > 1 else None
        for _, row in selected_df.iterrows():
            code_series = _normalize_diag_codes(pd.Series([row.iloc[0]]))
            if code_series.empty or pd.isna(code_series.iloc[0]):
                continue
            code = str(code_series.iloc[0])
            if not code:
                continue

            alias = (
                _normalize_feature_name(row[alias_col], fallback=code)
                if alias_col is not None
                else code
            )
            code_aliases[code] = alias

        if not code_aliases:
            logger.warning(f"Selected diagnosis codes file has no usable codes: {path}")
            continue

        alias_count = len(set(code_aliases.values()))
        logger.info(
            f"Loaded {len(code_aliases)} selected diagnosis codes as "
            f"{alias_count} output groups from {path}"
        )
        return code_aliases

    if found_file:
        logger.info(
            "No selected diagnosis codes configured in "
            "selections/selected_diagnosis_codes.csv."
        )
    else:
        logger.warning(
            "No selected diagnosis codes file found. Expected "
            "selections/selected_diagnosis_codes.csv."
        )
    return None


def _load_selected_rs(selected_rs_file: Optional[str | Path]) -> Optional[set[str]]:
    """Load selected RS labels."""
    return _load_selection_values(
        selection_file=selected_rs_file,
        filename="selected_rs.csv",
        label="RS values",
        normalizer=_normalize_rs_values,
    )


def _load_selected_up(selected_up_file: Optional[str | Path]) -> Optional[set[str]]:
    """Load selected UP codes."""
    return _load_selection_values(
        selection_file=selected_up_file,
        filename="selected_up.csv",
        label="UP values",
        normalizer=_normalize_up_codes,
    )


def _filter_if_selected(
    df: pd.DataFrame,
    column: str,
    selected_values: Optional[set[str]],
) -> pd.DataFrame:
    """Filter a dataframe only when a selection file was provided."""
    if selected_values is None:
        return df
    return df[df[column].isin(selected_values)].copy()


def run_incremental_diagnosis_pipeline_optimized(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str,
    diag_code_column: str,
    up_rs: pd.DataFrame,
    incremental_dir: str | Path,
    final_file: str | Path,
    selected_codes_file: Optional[str | Path] = None,
    selected_rs_file: Optional[str | Path] = None,
    selected_up_file: Optional[str | Path] = None,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: Optional[int] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """
    Run optimized incremental diagnosis pipeline with Parquet storage.

    Args:
        db_server: Database server
        db_database: Database name
        schema: Schema name
        table_name: Table name
        date_column: Date column in table
        up_column: UP column name
        diag_code_column: Diagnosis code column
        up_rs: UP-RS mapping DataFrame
        incremental_dir: Directory for incremental parquet files
        final_file: Final output parquet file
        selected_codes_file: Optional file with selected diagnosis codes to filter
        selected_rs_file: Optional file with selected RS values for grouped outputs
        selected_up_file: Optional file with selected UP values for grouped outputs
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep. None keeps all history,
            which is required when rebuilding final daily files from 2008 onward.
        start_date: Optional inclusive start day. If provided, overrides resume.
        end_date: Optional inclusive end day. Defaults to today when omitted.
    """
    logger.info("Starting optimized diagnosis pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=10000,
    )
    final_store = ParquetFinalStore(final_file)

    selected_codes = _load_selected_codes(selected_codes_file)
    selected_rs = _load_selected_rs(selected_rs_file)
    selected_up = _load_selected_up(selected_up_file)

    # Get last processed day from metadata, with final parquet as a fallback.
    metadata_last_date = incremental_mgr.get_last_timestamp()
    final_last_date = final_store.get_last_timestamp()
    last_loaded_date = latest_timestamp(metadata_last_date, final_last_date)
    logger.info(
        f"Last processed day: {last_loaded_date} "
        f"(metadata={metadata_last_date}, final={final_last_date})"
    )

    # Connect to database
    conn = get_connection(db_server, db_database, auth_mode=auth_mode)

    try:
        # Validate table schema before querying
        validate_table_columns(
            conn=conn,
            schema=schema,
            table_name=table_name,
            required_columns=[date_column, up_column, diag_code_column],
        )

        # Get data range
        min_date, max_date = get_min_max_date(
            conn=conn,
            schema=schema,
            table_name=table_name,
            date_column=date_column,
            min_valid_date=min_valid_date,
        )

        if min_date is None or max_date is None:
            logger.info("No valid data in source table")
            return

        window = get_incremental_processing_window(
            min_date=min_date,
            max_date=max_date,
            last_processed_date=last_loaded_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        if window is None:
            logger.info("No new diagnosis days to process on or before today")
            return

        start_date, end_exclusive, max_process_day = window
        logger.info(
            f"Processing new diagnosis days: {start_date.date()} -> "
            f"{max_process_day.date()}"
        )

        # Process by year for memory efficiency
        year_ranges = get_year_ranges(start_date, max_process_day)
        global_max_loaded = last_loaded_date

        for year, year_start, year_end in year_ranges:
            logger.info(f"Processing diagnosis year {year}")
            effective_year_start = max(year_start, start_date)
            effective_year_end = min(year_end, end_exclusive)
            if effective_year_end <= effective_year_start:
                logger.info(f"No diagnosis data on or before today for year {year}")
                continue

            # Query data efficiently
            df_chunk = get_diagnosis_data_for_year_optimized(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                up_column=up_column,
                diag_code_column=diag_code_column,
                year_start=effective_year_start,
                year_end=effective_year_end,
                last_loaded_date=None,
            )

            if df_chunk.empty:
                logger.info(f"No data for year {year}")
                continue

            logger.info(f"Year {year}: {len(df_chunk)} rows")

            df_chunk["DIAG_CODE"] = _normalize_diag_codes(df_chunk["DIAG_CODE"])
            df_chunk = df_chunk.dropna(subset=["DIAG_CODE"])
            df_chunk = df_chunk[df_chunk["DIAG_CODE"] != ""]

            if df_chunk.empty:
                continue

            # Prepare data
            df_chunk["timestamp"] = pd.to_datetime(df_chunk["timestamp"]).dt.floor("D")
            df_chunk = df_chunk.dropna(subset=["timestamp"])
            df_chunk = df_chunk[df_chunk["timestamp"] < end_exclusive].copy()
            if df_chunk.empty:
                logger.info(f"No diagnosis rows on or before today for year {year}")
                continue

            df_chunk[up_column] = _normalize_up_codes(df_chunk[up_column])
            df_chunk["n"] = pd.to_numeric(df_chunk["n"], errors="coerce").fillna(0)

            # Add UP-RS mapping
            up_rs_map = up_rs[["Codi UP", "RS"]].copy()
            up_rs_map["Codi UP"] = _normalize_up_codes(up_rs_map["Codi UP"])
            up_rs_map["RS"] = _normalize_rs_values(up_rs_map["RS"])
            up_rs_map.columns = [up_column, "RS"]
            before_merge = len(df_chunk)
            df_chunk = df_chunk.merge(
                up_rs_map, on=up_column, how="left"
            ).fillna("UNKNOWN")
            unknown_count = (df_chunk["RS"] == "UNKNOWN").sum()
            if unknown_count > 0:
                logger.warning(f"Found {unknown_count} rows with unknown UP codes (out of {before_merge} total)")
                unknown_ups = df_chunk[df_chunk["RS"] == "UNKNOWN"][up_column].unique()
                logger.warning(f"Unknown UP codes: {list(unknown_ups)[:10]}...")  # Show first 10

            # Build aggregations efficiently
            general_total = build_daily_total_general_optimized(df_chunk).reset_index()
            rs_total = build_daily_total_by_group_optimized(
                _filter_if_selected(df_chunk, "RS", selected_rs),
                group_col="RS",
                group_label="RS",
            )
            up_total = build_daily_total_by_group_optimized(
                _filter_if_selected(df_chunk, up_column, selected_up),
                group_col=up_column,
                group_label="UP",
            )

            if selected_codes:
                code_df = df_chunk[df_chunk["DIAG_CODE"].isin(selected_codes)].copy()
                code_df["DIAG_CODE"] = code_df["DIAG_CODE"].map(selected_codes)
                logger.info(
                    f"Selected diagnosis-code rows for year {year}: "
                    f"{len(code_df)} of {len(df_chunk)} aggregated rows "
                    f"across {code_df['DIAG_CODE'].nunique()} output groups"
                )
            else:
                code_df = df_chunk.iloc[0:0].copy()
                logger.warning(
                    "Skipping code-specific diagnosis features because no selected "
                    "diagnosis-code file was found"
                )

            code_daily = build_daily_diagnosis_counts_optimized(code_df)
            rs_daily = build_daily_diagnosis_by_group_optimized(
                _filter_if_selected(code_df, "RS", selected_rs),
                group_col="RS",
            )
            up_daily = build_daily_diagnosis_by_group_optimized(
                _filter_if_selected(code_df, up_column, selected_up),
                group_col=up_column,
            )

            # Store one already-aggregated daily file per processed year.
            yearly_daily = _build_diagnosis_wide_final(
                pd.concat(
                    [
                        general_total,
                        rs_total,
                        up_total,
                        code_daily,
                        rs_daily,
                        up_daily,
                    ],
                    ignore_index=True,
                    sort=False,
                )
            )
            incremental_mgr.add_data(yearly_daily, timestamp_col="timestamp")
            logger.info(
                f"Saved diagnosis year {year} daily aggregate "
                f"({len(yearly_daily)} rows)"
            )

            # Track max date
            chunk_max = df_chunk["timestamp"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            # Clean up
            del (
                df_chunk,
                general_total,
                rs_total,
                up_total,
                code_df,
                code_daily,
                rs_daily,
                up_daily,
                yearly_daily,
            )

        # Aggregate to final
        logger.info("Aggregating diagnosis to final output...")
        aggregate_diagnosis_final_optimized(incremental_mgr, final_store)

        logger.info("Diagnosis pipeline completed successfully")

    finally:
        conn.close()


def run_diagnosis_pipeline_main_optimized(
    config,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """Main entry point for optimized diagnosis pipeline."""
    run_incremental_diagnosis_pipeline_optimized(
        db_server=config.DB_SERVER,
        db_database=config.DB_DATABASE,
        schema=config.SCHEMA,
        table_name=config.TABLE_NAME,
        date_column=config.DATE_COLUMN,
        up_column=config.UP_COLUMN,
        diag_code_column=config.DIAG_CODE_COLUMN,
        up_rs=pd.read_excel(config.UP_RS_FILE, sheet_name=config.UP_RS_SHEET),
        incremental_dir=config.PIPELINE_DATA_DIR / "incremental",
        final_file=config.PIPELINE_DATA_DIR / "finals" / "diagnosis_final.parquet",
        selected_codes_file=config.SELECTED_CODES_FILE,
        selected_rs_file=config.SELECTED_RS_FILE,
        selected_up_file=config.SELECTED_UP_FILE,
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=None,
        start_date=start_date,
        end_date=end_date,
    )

