"""Optimized demand pipeline main runner with Parquet storage."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from pipelines.shared import (
    get_connection,
    setup_logging,
    get_min_max_date,
    get_year_ranges,
    get_data_for_year,
    get_incremental_processing_window,
    latest_timestamp,
)
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_total_cat_optimized,
    build_daily_features_global_optimized,
    build_daily_features_by_group_optimized,
    aggregate_final_optimized,
    refresh_final_imputation,
    _build_wide_final_by_timestamp,
)
from .transformations import prepare_visits_chunk

logger = setup_logging()


def _normalize_up_codes(values: pd.Series) -> pd.Series:
    """Normalize UP codes for matching selected UP files."""
    return values.astype("string").str.strip().str.zfill(5)


def _normalize_rs_values(values: pd.Series) -> pd.Series:
    """Normalize RS labels for matching selected RS files."""
    return values.astype("string").str.strip().str.upper()


def _load_selection_values(
    selection_file: Optional[str | Path],
    filename: str,
    label: str,
    normalizer,
) -> Optional[set[str]]:
    """Load a one-column shared selection CSV."""
    candidates = []
    if selection_file:
        active_path = Path(selection_file)
        candidates.append(active_path)
        try:
            base_dir = active_path.resolve().parents[1]
            candidates.append(base_dir / "selections" / filename)
        except IndexError:
            pass

    candidates.append(Path.cwd() / "selections" / filename)

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
            logger.warning(f"Selected demand {label} file is empty: {path}")
            continue

        logger.info(f"Loaded {len(values)} selected demand {label} from {path}")
        return values

    if found_file:
        logger.info(
            f"No selected demand {label} configured; all values will be included "
            "for this grouped output."
        )
    else:
        logger.warning(
            f"No selected demand {label} file found. Expected "
            f"selections/{filename}. "
            "All values will be included for this grouped output."
        )
    return None


def _load_selected_rs(selected_rs_file: Optional[str | Path]) -> Optional[set[str]]:
    """Load selected RS labels for demand grouped outputs."""
    return _load_selection_values(
        selection_file=selected_rs_file,
        filename="selected_rs.csv",
        label="RS values",
        normalizer=_normalize_rs_values,
    )


def _load_selected_up(selected_up_file: Optional[str | Path]) -> Optional[set[str]]:
    """Load selected UP codes for demand grouped outputs."""
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
    normalizer,
) -> pd.DataFrame:
    """Filter a dataframe only when a selection file contains values."""
    if selected_values is None:
        return df

    normalized = normalizer(df[column])
    return df[normalized.isin(selected_values)].copy()


def run_incremental_pipeline_optimized(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_rs: pd.DataFrame,
    incremental_dir: str | Path,
    final_file: str | Path,
    selected_rs_file: Optional[str | Path] = None,
    selected_up_file: Optional[str | Path] = None,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: Optional[int] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """
    Run optimized incremental demand pipeline with Parquet storage.

    Args:
        db_server: Database server
        db_database: Database name
        schema: Schema name
        table_name: Table name
        date_column: Date column in table
        up_rs: UP-RS mapping DataFrame
        incremental_dir: Directory for incremental parquet files
        final_file: Final output parquet file
        selected_rs_file: Optional file with selected RS values for grouped outputs
        selected_up_file: Optional file with selected UP values for grouped outputs
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep. None keeps all history,
            which is required when rebuilding final daily files from 2008 onward.
        start_date: Optional inclusive start day. If provided, overrides resume.
        end_date: Optional inclusive end day. Defaults to today when omitted.
    """
    logger.info("Starting optimized demand pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=10000,
    )
    final_store = ParquetFinalStore(final_file)
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

        today_day = pd.Timestamp.today().normalize()
        requested_end_day = (
            today_day
            if end_date is None
            else min(pd.to_datetime(end_date).normalize(), today_day)
        )
        source_observed_until = min(pd.to_datetime(max_date).normalize(), requested_end_day)

        window = get_incremental_processing_window(
            min_date=min_date,
            max_date=max_date,
            last_processed_date=last_loaded_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
        )
        if window is None:
            logger.info("No new demand days to process on or before today")
            refresh_final_imputation(
                final_store,
                observed_until=source_observed_until,
                impute_until=requested_end_day,
            )
            return

        start_date, end_exclusive, max_process_day = window
        logger.info(
            f"Processing new demand days: {start_date.date()} -> "
            f"{max_process_day.date()}"
        )

        # Process by year for memory efficiency
        year_ranges = get_year_ranges(start_date, max_process_day)
        global_max_loaded = last_loaded_date

        for year, year_start, year_end in year_ranges:
            logger.info(f"Processing year {year}")
            effective_year_start = max(year_start, start_date)
            effective_year_end = min(year_end, end_exclusive)
            if effective_year_end <= effective_year_start:
                logger.info(f"No demand data on or before today for year {year}")
                continue

            # Query data efficiently
            df_chunk = get_data_for_year(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                year_start=effective_year_start,
                year_end=effective_year_end,
                last_loaded_date=None,
                selected_cols=[
                    "DATA_VISITA",
                    "UP",
                    "VISI_LLOC_VISITA",
                    "VISI_SITUACIO_VISITA",
                    "SERVEI_CODI",
                    "TIPUS_CLASS",
                    "VISI_TIPUS_VISITA",
                ],
            )

            if df_chunk.empty:
                logger.info(f"No data for year {year}")
                continue

            logger.info(f"Year {year}: {len(df_chunk)} rows")

            # Transform chunk
            df_chunk = prepare_visits_chunk(df_chunk, up_rs=up_rs)
            df_chunk = df_chunk[df_chunk["DATA_VISITA"] < end_exclusive].copy()
            if df_chunk.empty:
                logger.info(f"No demand rows on or before today for year {year}")
                continue

            # Rename timestamp column for consistency
            df_chunk["timestamp"] = df_chunk["DATA_VISITA"]

            # Build aggregations efficiently
            cat_daily = build_daily_total_cat_optimized(df_chunk)
            global_daily = build_daily_features_global_optimized(df_chunk)
            rs_daily = build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "RS", selected_rs, _normalize_rs_values),
                group_col="RS",
            )
            up_daily = build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "UP", selected_up, _normalize_up_codes),
                group_col="UP",
            )

            # Store one already-aggregated daily file per processed year.
            yearly_daily = _build_wide_final_by_timestamp(
                pd.concat(
                    [
                        cat_daily.reset_index(),
                        global_daily,
                        rs_daily,
                        up_daily,
                    ],
                    ignore_index=True,
                    sort=False,
                )
            )
            incremental_mgr.add_data(yearly_daily, timestamp_col="timestamp")
            logger.info(
                f"Saved demand year {year} daily aggregate "
                f"({len(yearly_daily)} rows)"
            )

            # Track max date
            chunk_max = df_chunk["timestamp"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            # Clean up
            del df_chunk, cat_daily, global_daily, rs_daily, up_daily, yearly_daily

        # Aggregate to final
        logger.info("Aggregating to final output...")
        aggregate_final_optimized(
            incremental_mgr,
            final_store,
            observed_until=max_process_day,
            impute_until=requested_end_day,
        )

        logger.info("Demand pipeline completed successfully")

    finally:
        conn.close()


def run_demand_pipeline_main_optimized(
    config,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> None:
    """Main entry point for optimized demand pipeline."""
    run_incremental_pipeline_optimized(
        db_server=config.DB_SERVER,
        db_database=config.DB_DATABASE,
        schema=config.SCHEMA,
        table_name=config.TABLE_NAME,
        date_column=config.DATE_COLUMN,
        up_rs=pd.read_excel(config.UP_RS_FILE, sheet_name=config.UP_RS_SHEET),
        incremental_dir=config.PIPELINE_DATA_DIR / "incremental",
        final_file=config.PIPELINE_DATA_DIR / "finals" / "demand_final.parquet",
        selected_rs_file=config.SELECTED_RS_FILE,
        selected_up_file=config.SELECTED_UP_FILE,
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=None,
        start_date=start_date,
        end_date=end_date,
    )
