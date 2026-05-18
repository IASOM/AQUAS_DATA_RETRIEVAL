"""Optimized diagnosis pipeline main runner with Parquet storage."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from pipelines.shared import get_connection, setup_logging, get_min_max_date, get_year_ranges
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_diagnosis_counts_optimized,
    build_daily_diagnosis_by_group_optimized,
    build_diagnosis_wide_format_optimized,
    add_incremental_diagnosis_optimized,
    aggregate_diagnosis_final_optimized,
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
    """Optimized query for diagnosis data."""
    selected_cols = [date_column, up_column, diag_code_column]
    cols_sql = ", ".join(f"[{c}]" for c in selected_cols)

    if last_loaded_date is None:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{diag_code_column}] IS NOT NULL
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end]
    else:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{date_column}] > ?
            AND [{diag_code_column}] IS NOT NULL
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end, last_loaded_date]

    return pd.read_sql_query(query, conn, params=params)


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
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: int = 90,
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
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep
    """
    logger.info("Starting optimized diagnosis pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=50000,
    )
    final_store = ParquetFinalStore(final_file)

    # Load selected codes if provided
    selected_codes = None
    if selected_codes_file and Path(selected_codes_file).exists():
        try:
            selected_codes_df = pd.read_csv(selected_codes_file)
            selected_codes = set(selected_codes_df.iloc[:, 0].unique())
            logger.info(f"Loaded {len(selected_codes)} selected diagnosis codes")
        except Exception as e:
            logger.warning(f"Could not load selected codes: {e}")

    # Get last processed date
    last_loaded_date = incremental_mgr.get_last_timestamp()
    logger.info(f"Last processed: {last_loaded_date}")

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

        # Adjust to today
        today_date = pd.Timestamp.today().normalize()
        if max_date > today_date:
            max_date = today_date

        start_date = min_date if last_loaded_date is None else last_loaded_date
        logger.info(f"Processing range: {start_date} -> {max_date}")

        # Process by year for memory efficiency
        year_ranges = get_year_ranges(start_date, max_date)
        global_max_loaded = last_loaded_date

        for year, year_start, year_end in year_ranges:
            logger.info(f"Processing diagnosis year {year}")

            # Query data efficiently
            df_chunk = get_diagnosis_data_for_year_optimized(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                up_column=up_column,
                diag_code_column=diag_code_column,
                year_start=year_start,
                year_end=year_end,
                last_loaded_date=last_loaded_date,
            )

            if df_chunk.empty:
                logger.info(f"No data for year {year}")
                continue

            logger.info(f"Year {year}: {len(df_chunk)} rows")

            # Filter by selected codes if provided
            if selected_codes:
                before = len(df_chunk)
                df_chunk = df_chunk[df_chunk[diag_code_column].isin(selected_codes)]
                after = len(df_chunk)
                logger.info(f"Filtered to {after} rows (from {before})")

            if df_chunk.empty:
                continue

            # Prepare data
            df_chunk["timestamp"] = pd.to_datetime(df_chunk[date_column]).dt.floor("D")
            df_chunk["n"] = 1
            df_chunk.rename(columns={diag_code_column: "DIAG_CODE"}, inplace=True)

            # Add UP-RS mapping
            up_rs_map = up_rs[["Codi UP", "RS"]].copy()
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
            total_daily = build_daily_diagnosis_counts_optimized(df_chunk)
            rs_daily = build_daily_diagnosis_by_group_optimized(
                df_chunk, group_col="RS"
            )
            up_daily = build_daily_diagnosis_by_group_optimized(
                df_chunk, group_col=up_column
            )
            wide_format = build_diagnosis_wide_format_optimized(df_chunk)

            # Add to incremental storage
            add_incremental_diagnosis_optimized(total_daily, incremental_mgr)
            add_incremental_diagnosis_optimized(rs_daily, incremental_mgr)
            add_incremental_diagnosis_optimized(up_daily, incremental_mgr)
            add_incremental_diagnosis_optimized(
                wide_format.reset_index(), incremental_mgr
            )

            # Track max date
            chunk_max = df_chunk["timestamp"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            # Clean up
            del df_chunk, total_daily, rs_daily, up_daily, wide_format

        # Aggregate to final
        logger.info("Aggregating diagnosis to final output...")
        aggregate_diagnosis_final_optimized(incremental_mgr, final_store)

        logger.info("Diagnosis pipeline completed successfully")

    finally:
        conn.close()


def run_diagnosis_pipeline_main_optimized(config) -> None:
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
        selected_codes_file=config.PIPELINE_DATA_DIR / "selected_codes" / "selected_codes.csv",
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=90,
    )
