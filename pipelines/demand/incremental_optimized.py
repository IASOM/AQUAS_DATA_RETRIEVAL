"""Optimized demand pipeline main runner with Parquet storage."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from pipelines.shared import get_connection, setup_logging, get_min_max_date, get_year_ranges, get_data_for_year
from pipelines.shared.parquet_storage import ParquetIncrementalManager, ParquetFinalStore
from .aggregation_optimized import (
    build_daily_total_cat_optimized,
    build_daily_features_by_group_optimized,
    add_incremental_optimized,
    aggregate_final_optimized,
)
from .transformations import prepare_visits_chunk

logger = setup_logging()


def run_incremental_pipeline_optimized(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_rs: pd.DataFrame,
    incremental_dir: str | Path,
    final_file: str | Path,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
    retention_days: int = 90,
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
        auth_mode: Database authentication mode
        min_valid_date: Minimum date to process
        retention_days: Days of incremental data to keep
    """
    logger.info("Starting optimized demand pipeline...")

    # Initialize storage managers
    incremental_mgr = ParquetIncrementalManager(
        incremental_dir,
        retention_days=retention_days,
        chunk_size=50000,
    )
    final_store = ParquetFinalStore(final_file)

    # Get last processed date
    last_loaded_date = incremental_mgr.get_last_timestamp()
    logger.info(f"Last processed: {last_loaded_date}")

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
            logger.info(f"Processing year {year}")

            # Query data efficiently
            df_chunk = get_data_for_year(
                conn=conn,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                year_start=year_start,
                year_end=year_end,
                last_loaded_date=last_loaded_date,
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

            # Rename timestamp column for consistency
            df_chunk["timestamp"] = df_chunk["DATA_VISITA"]

            # Build aggregations efficiently
            cat_daily = build_daily_total_cat_optimized(df_chunk)
            rs_daily = build_daily_features_by_group_optimized(
                df_chunk, group_col="RS"
            )
            up_daily = build_daily_features_by_group_optimized(
                df_chunk, group_col="UP"
            )

            # Add to incremental storage
            add_incremental_optimized(cat_daily.reset_index(), incremental_mgr)
            add_incremental_optimized(rs_daily, incremental_mgr)
            add_incremental_optimized(up_daily, incremental_mgr)

            # Track max date
            chunk_max = df_chunk["timestamp"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            # Clean up
            del df_chunk, cat_daily, rs_daily, up_daily

        # Aggregate to final
        logger.info("Aggregating to final output...")
        aggregate_final_optimized(incremental_mgr, final_store)

        logger.info("Demand pipeline completed successfully")

    finally:
        conn.close()


def run_demand_pipeline_main_optimized(config) -> None:
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
        auth_mode=config.AUTH_MODE,
        min_valid_date=config.MIN_VALID_DATE,
        retention_days=90,
    )
