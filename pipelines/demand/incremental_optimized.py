"""Optimized demand pipeline main runner with Parquet storage."""
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
    get_data_for_year,
    get_incremental_processing_window,
)
from pipelines.shared.imputation import drop_imputed_rows
from pipelines.shared.naming import DOMAIN_DEMAND, GEO_RS, GEO_UP, total_code
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


def _normalize_geo_alias(value, fallback: str) -> str:
    """Normalize a configured territorial subset identifier for output names."""
    if pd.isna(value):
        return fallback

    token = str(value).strip()
    if not token:
        return fallback

    token = re.sub(r"[^0-9A-Za-z]+", "_", token).strip("_").upper()
    return token or fallback


def _selection_map_from_df(
    selection_df: pd.DataFrame,
    normalizer,
) -> dict[str, str]:
    """Build source-value -> output-geo-id mapping from a selection CSV."""
    if selection_df.empty:
        return {}

    columns = list(selection_df.columns)
    column_lookup = {str(col).strip().lower(): col for col in columns}
    source_col = (
        column_lookup.get("rs")
        or column_lookup.get("up")
        or column_lookup.get("value")
        or column_lookup.get("source_value")
        or (columns[1] if len(columns) > 1 else columns[0])
    )
    alias_col = (
        column_lookup.get("geo_id")
        or column_lookup.get("id")
        or column_lookup.get("subset_id")
        or (columns[0] if len(columns) > 1 else source_col)
    )

    mapping = {}
    for _, row in selection_df.iterrows():
        source_values = normalizer(pd.Series([row[source_col]])).dropna()
        if source_values.empty:
            continue

        source = str(source_values.iloc[0])
        if not source:
            continue

        alias = _normalize_geo_alias(row[alias_col], fallback=source)
        mapping[source] = alias
    return mapping


def _load_selection_values(
    selection_file: Optional[str | Path],
    filename: str,
    label: str,
    normalizer,
) -> Optional[dict[str, str]]:
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
        selection_df = pd.read_csv(path, dtype=str)
        values = _selection_map_from_df(selection_df, normalizer)
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


def _load_selected_rs(selected_rs_file: Optional[str | Path]) -> Optional[dict[str, str]]:
    """Load selected RS labels for demand grouped outputs."""
    return _load_selection_values(
        selection_file=selected_rs_file,
        filename="selected_rs.csv",
        label="RS values",
        normalizer=_normalize_rs_values,
    )


def _load_selected_up(selected_up_file: Optional[str | Path]) -> Optional[dict[str, str]]:
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
    selected_values: Optional[dict[str, str]],
    normalizer,
) -> pd.DataFrame:
    """Filter a dataframe only when a selection file contains values."""
    if selected_values is None:
        return df

    normalized = normalizer(df[column])
    mask = normalized.isin(selected_values.keys())
    out = df[mask].copy()
    if out.empty:
        return out

    out[column] = normalized[mask].map(selected_values).astype(str)
    return out


def _split_into_six_month_ranges(
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    ranges = []
    cursor = pd.to_datetime(start).normalize()
    end_exclusive = pd.to_datetime(end_exclusive).normalize()
    while cursor < end_exclusive:
        next_cursor = min(cursor + pd.DateOffset(months=6), end_exclusive)
        ranges.append((cursor, next_cursor))
        cursor = next_cursor
    return ranges


def _existing_observed_range(
    final_store: ParquetFinalStore,
    timestamp_col: str = "timestamp",
) -> Optional[tuple[pd.Timestamp, pd.Timestamp]]:
    existing = final_store.load_final()
    if existing.empty or timestamp_col not in existing.columns:
        return None

    existing = drop_imputed_rows(existing, timestamp_col=timestamp_col)
    if existing.empty:
        return None

    timestamps = pd.to_datetime(existing[timestamp_col], errors="coerce").dropna()
    if timestamps.empty:
        return None

    return timestamps.min().normalize(), timestamps.max().normalize()


def _missing_selected_geos(
    final_store: ParquetFinalStore,
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
) -> tuple[dict[str, str], dict[str, str]]:
    existing = final_store.load_final()
    if existing.empty:
        return {}, {}

    columns = set(existing.columns)
    missing_rs = {
        source: alias
        for source, alias in (selected_rs or {}).items()
        if total_code(DOMAIN_DEMAND, GEO_RS, alias) not in columns
    }
    missing_up = {
        source: alias
        for source, alias in (selected_up or {}).items()
        if total_code(DOMAIN_DEMAND, GEO_UP, alias) not in columns
    }
    return missing_rs, missing_up


def _source_ups_for_selected_rs(
    up_rs: pd.DataFrame,
    selected_rs: dict[str, str],
) -> list[str]:
    if not selected_rs or not {"Codi UP", "RS"}.issubset(up_rs.columns):
        return []

    lookup = up_rs[["Codi UP", "RS"]].copy()
    lookup["Codi UP"] = _normalize_up_codes(lookup["Codi UP"])
    lookup["RS"] = _normalize_rs_values(lookup["RS"])
    return (
        lookup.loc[lookup["RS"].isin(selected_rs.keys()), "Codi UP"]
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()
    )


def _metadata_snapshot(manager: ParquetIncrementalManager) -> Optional[pd.DataFrame]:
    if not manager.metadata_file.exists():
        return None
    try:
        return pd.read_parquet(manager.metadata_file)
    except Exception as exc:
        logger.warning(f"Could not snapshot processing metadata: {exc}")
        return None


def _restore_metadata_snapshot(
    manager: ParquetIncrementalManager,
    snapshot: Optional[pd.DataFrame],
) -> None:
    if snapshot is not None:
        snapshot.to_parquet(manager.metadata_file, index=False)
    elif manager.metadata_file.exists():
        manager.metadata_file.unlink()


def _selection_backfill_window(
    existing_range: tuple[pd.Timestamp, pd.Timestamp],
    source_observed_until: pd.Timestamp,
    requested_start_date: Optional[str | pd.Timestamp],
    requested_end_day: pd.Timestamp,
) -> Optional[tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    start_day, existing_end_day = existing_range
    if requested_start_date is not None:
        start_day = max(start_day, pd.to_datetime(requested_start_date).normalize())

    end_day = min(existing_end_day, source_observed_until, requested_end_day)
    if start_day > end_day:
        return None

    return start_day, end_day + pd.Timedelta(days=1), end_day


def _process_demand_range(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    up_rs: pd.DataFrame,
    incremental_mgr: ParquetIncrementalManager,
    period_start: pd.Timestamp,
    period_end_exclusive: pd.Timestamp,
    selected_rs: Optional[dict[str, str]],
    selected_up: Optional[dict[str, str]],
    build_global_outputs: bool = True,
    build_rs_outputs: bool = True,
    build_up_outputs: bool = True,
    source_up_filter: Optional[list[str]] = None,
    label: str = "period",
) -> Optional[pd.Timestamp]:
    df_chunk = get_data_for_year(
        conn=conn,
        schema=schema,
        table_name=table_name,
        date_column=date_column,
        year_start=period_start,
        year_end=period_end_exclusive,
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
        normalized_up_values=source_up_filter,
    )

    if df_chunk.empty:
        logger.info(f"No demand data for {label}")
        return None

    logger.info(f"Demand {label}: {len(df_chunk)} rows")

    df_chunk = prepare_visits_chunk(df_chunk, up_rs=up_rs)
    df_chunk = df_chunk[df_chunk["DATA_VISITA"] < period_end_exclusive].copy()
    if df_chunk.empty:
        logger.info(f"No demand rows on or before today for {label}")
        return None

    df_chunk["timestamp"] = df_chunk["DATA_VISITA"]
    pieces = []

    if build_global_outputs:
        pieces.append(build_daily_total_cat_optimized(df_chunk).reset_index())
        pieces.append(build_daily_features_global_optimized(df_chunk))

    if build_rs_outputs:
        pieces.append(
            build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "RS", selected_rs, _normalize_rs_values),
                group_col="RS",
            )
        )

    if build_up_outputs:
        pieces.append(
            build_daily_features_by_group_optimized(
                _filter_if_selected(df_chunk, "UP", selected_up, _normalize_up_codes),
                group_col="UP",
            )
        )

    pieces = [piece for piece in pieces if not piece.empty]
    if not pieces:
        logger.info(f"No demand aggregates produced for {label}")
        return None

    daily = _build_wide_final_by_timestamp(
        pd.concat(pieces, ignore_index=True, sort=False)
    )
    incremental_mgr.add_data(daily, timestamp_col="timestamp")
    logger.info(f"Saved demand {label} daily aggregate ({len(daily)} rows)")
    return df_chunk["timestamp"].max()


def _process_demand_range_with_semester_retry(
    **kwargs,
) -> Optional[pd.Timestamp]:
    period_start = kwargs["period_start"]
    period_end_exclusive = kwargs["period_end_exclusive"]
    label = kwargs.get("label", "period")

    try:
        return _process_demand_range(**kwargs)
    except MemoryError:
        subranges = _split_into_six_month_ranges(period_start, period_end_exclusive)
        if len(subranges) <= 1:
            raise

        logger.warning(
            f"Demand {label} exhausted memory; retrying in 6-month windows"
        )
        max_loaded = None
        for sub_start, sub_end in subranges:
            sub_kwargs = dict(kwargs)
            sub_kwargs["period_start"] = sub_start
            sub_kwargs["period_end_exclusive"] = sub_end
            sub_kwargs["label"] = (
                f"{sub_start.date()} -> {(sub_end - pd.Timedelta(days=1)).date()}"
            )
            loaded = _process_demand_range(**sub_kwargs)
            if pd.notna(loaded) and (max_loaded is None or loaded > max_loaded):
                max_loaded = loaded
        return max_loaded


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
    metadata_exists = incremental_mgr.metadata_file.exists()
    metadata_last_date = incremental_mgr.get_last_timestamp()
    final_last_date = final_store.get_last_contiguous_timestamp()
    if metadata_exists and metadata_last_date is not None and final_last_date is not None:
        last_loaded_date = min(metadata_last_date, final_last_date)
    elif metadata_exists:
        last_loaded_date = metadata_last_date
    else:
        last_loaded_date = final_last_date
    logger.info(
        f"Last processed day: {last_loaded_date} "
        f"(metadata={metadata_last_date}, final_contiguous={final_last_date}, "
        f"metadata_exists={metadata_exists})"
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

        missing_rs, missing_up = _missing_selected_geos(
            final_store,
            selected_rs,
            selected_up,
        )
        existing_range = _existing_observed_range(final_store)
        backfilled_selection = False
        if existing_range and (missing_rs or missing_up):
            backfill_window = _selection_backfill_window(
                existing_range=existing_range,
                source_observed_until=source_observed_until,
                requested_start_date=start_date,
                requested_end_day=requested_end_day,
            )
            if backfill_window is not None:
                backfill_start, backfill_end_exclusive, backfill_max_day = backfill_window
                source_up_filter = sorted(
                    set(missing_up.keys())
                    | set(_source_ups_for_selected_rs(up_rs, missing_rs))
                )
                if source_up_filter:
                    logger.info(
                        "Backfilling missing demand selections only: "
                        f"{len(missing_rs)} RS, {len(missing_up)} UP; "
                        f"{backfill_start.date()} -> {backfill_max_day.date()}"
                    )
                    metadata_before_backfill = _metadata_snapshot(incremental_mgr)
                    backfill_max_loaded = None
                    for period_start, period_end in _split_into_six_month_ranges(
                        backfill_start,
                        backfill_end_exclusive,
                    ):
                        loaded = _process_demand_range(
                            conn=conn,
                            schema=schema,
                            table_name=table_name,
                            date_column=date_column,
                            up_rs=up_rs,
                            incremental_mgr=incremental_mgr,
                            period_start=period_start,
                            period_end_exclusive=period_end,
                            selected_rs=missing_rs or {},
                            selected_up=missing_up or {},
                            build_global_outputs=False,
                            build_rs_outputs=bool(missing_rs),
                            build_up_outputs=bool(missing_up),
                            source_up_filter=source_up_filter,
                            label=(
                                "selection backfill "
                                f"{period_start.date()} -> "
                                f"{(period_end - pd.Timedelta(days=1)).date()}"
                            ),
                        )
                        if pd.notna(loaded) and (
                            backfill_max_loaded is None or loaded > backfill_max_loaded
                        ):
                            backfill_max_loaded = loaded

                    if backfill_max_loaded is not None:
                        aggregate_final_optimized(
                            incremental_mgr,
                            final_store,
                            observed_until=backfill_max_day,
                            impute_until=requested_end_day,
                            replace_overlapping_days=False,
                        )
                        backfilled_selection = True

                    _restore_metadata_snapshot(incremental_mgr, metadata_before_backfill)
                else:
                    logger.warning(
                        "Demand selections are missing from the final, but no matching "
                        "source UP codes were found in UPperRS.xlsx."
                    )

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

        if backfilled_selection and start_date is not None and existing_range:
            requested_start_day = pd.to_datetime(start_date).normalize()
            existing_start_day, existing_end_day = existing_range
            explicit_end_day = min(source_observed_until, requested_end_day)
            if (
                existing_start_day <= requested_start_day
                and existing_end_day >= explicit_end_day
            ):
                logger.info(
                    "Requested demand range was already present; missing selection "
                    "backfill completed without reloading the full range."
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

            chunk_max = _process_demand_range_with_semester_retry(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                up_rs=up_rs,
                incremental_mgr=incremental_mgr,
                period_start=effective_year_start,
                period_end_exclusive=effective_year_end,
                selected_rs=selected_rs,
                selected_up=selected_up,
                label=f"year {year}",
            )

            # Track max date
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

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
        up_rs=pd.read_excel(config.resolve_up_rs_file(), sheet_name=config.UP_RS_SHEET),
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
