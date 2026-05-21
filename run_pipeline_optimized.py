"""
Optimized pipeline runner with Parquet storage.

This version supports two execution modes:
- Production mode: query SQL Server / Azure Synapse.
- Sample mode: use bundled synthetic CSV data and write sample Parquet outputs.
"""
import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.config import DemandConfig, DiagnosisConfig, get_config
from pipelines.shared import FinalDataJoiner, setup_logging

logger = setup_logging()


def convert_parquet_file(
    input_file: str | Path,
    output_format: str,
    output_file: Optional[str | Path] = None,
) -> Path:
    """Convert one Parquet file to CSV or Excel."""
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {input_path}")

    output_format = output_format.lower()
    if output_format == "xlsx":
        output_format = "excel"
    if output_format not in {"csv", "excel"}:
        raise ValueError("output_format must be 'csv' or 'excel'")

    suffix = ".csv" if output_format == "csv" else ".xlsx"
    output_path = Path(output_file) if output_file else input_path.with_suffix(suffix)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading parquet file: {input_path}")
    df = pd.read_parquet(input_path)

    if output_format == "csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        df.to_excel(output_path, index=False)

    logger.info(
        f"Converted {input_path} to {output_path} "
        f"({len(df)} rows, {len(df.columns)} columns)"
    )
    return output_path


def _parse_cli_date(value: Optional[str], arg_name: str) -> Optional[pd.Timestamp]:
    """Parse an optional YYYY-MM-DD CLI date as a normalized Timestamp."""
    if value is None:
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{arg_name} must be a valid date in YYYY-MM-DD format")

    return parsed.normalize()


def run_demand_pipeline_optimized(
    config: Optional[DemandConfig] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> bool:
    """Run optimized demand pipeline with Parquet storage."""
    if config is None:
        config = get_config("demand")

    logger.info("=" * 80)
    logger.info("STARTING OPTIMIZED DEMAND PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.demand.incremental_optimized import run_demand_pipeline_main_optimized

        run_demand_pipeline_main_optimized(
            config,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Demand pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Demand pipeline failed: {e}", exc_info=True)
        return False


def run_diagnosis_pipeline_optimized(
    config: Optional[DiagnosisConfig] = None,
    start_date: Optional[str | pd.Timestamp] = None,
    end_date: Optional[str | pd.Timestamp] = None,
) -> bool:
    """Run optimized diagnosis pipeline with Parquet storage."""
    if config is None:
        config = get_config("diagnosis")

    logger.info("=" * 80)
    logger.info("STARTING OPTIMIZED DIAGNOSIS PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.diagnosis.incremental_optimized import (
            run_diagnosis_pipeline_main_optimized,
        )

        run_diagnosis_pipeline_main_optimized(
            config,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Diagnosis pipeline completed successfully")
        return True
    except Exception as e:
        logger.error(f"Diagnosis pipeline failed: {e}", exc_info=True)
        return False


def join_final_outputs(
    demand_file: Optional[Path] = None,
    diagnosis_file: Optional[Path] = None,
    output_file: Optional[Path] = None,
) -> bool:
    """Join final demand and diagnosis outputs columnwise."""
    logger.info("=" * 80)
    logger.info("JOINING DEMAND AND DIAGNOSIS DATA COLUMNWISE")
    logger.info("=" * 80)

    try:
        demand_config = get_config("demand")
        diagnosis_config = get_config("diagnosis")

        demand_path = demand_file or (
            demand_config.PIPELINE_DATA_DIR / "finals" / "demand_final.parquet"
        )
        diagnosis_path = diagnosis_file or (
            diagnosis_config.PIPELINE_DATA_DIR / "finals" / "diagnosis_final.parquet"
        )
        output_path = output_file or (
            Path(demand_config.PIPELINE_DATA_DIR.parent)
            / "finals"
            / "demand_diagnosis_joined.parquet"
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        joiner = FinalDataJoiner(
            demand_final_file=demand_path,
            diagnosis_final_file=diagnosis_path,
            output_file=output_path,
        )
        joiner.join_and_save(
            demand_prefix="DEMAND",
            diagnosis_prefix="DIAGNOSIS",
            fill_method="ffill",
            compression="snappy",
        )

        logger.info(f"Final join completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Final join failed: {e}", exc_info=True)
        return False


def run_demand_sample_pipeline(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> bool:
    """Run demand pipeline with bundled synthetic sample data."""
    logger.info("=" * 80)
    logger.info("STARTING SAMPLE DEMAND PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import run_sample_demand_pipeline

        output_path = run_sample_demand_pipeline(
            input_dir,
            output_dir,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"Sample demand pipeline completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample demand pipeline failed: {e}", exc_info=True)
        return False


def run_diagnosis_sample_pipeline(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> bool:
    """Run diagnosis pipeline with bundled synthetic sample data."""
    logger.info("=" * 80)
    logger.info("STARTING SAMPLE DIAGNOSIS PIPELINE")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import run_sample_diagnosis_pipeline

        output_path = run_sample_diagnosis_pipeline(
            input_dir,
            output_dir,
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"Sample diagnosis pipeline completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample diagnosis pipeline failed: {e}", exc_info=True)
        return False


def join_sample_final_outputs(output_dir: Path) -> bool:
    """Join sample demand and diagnosis outputs."""
    logger.info("=" * 80)
    logger.info("JOINING SAMPLE DEMAND AND DIAGNOSIS DATA")
    logger.info("=" * 80)

    try:
        from pipelines.sample_runner import join_sample_outputs

        output_path = join_sample_outputs(output_dir)
        logger.info(f"Sample final join completed: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Sample final join failed: {e}", exc_info=True)
        return False


def main() -> int:
    """Main entry point for optimized pipeline runner."""
    parser = argparse.ArgumentParser(
        description="Run PREDAP data processing pipelines (Optimized with Parquet)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --demand                        Run demand pipeline only
  python run_pipeline.py --diagnosis                     Run diagnosis pipeline only
  python run_pipeline.py --both                          Run both pipelines
  python run_pipeline.py --all                           Run both + final join
  python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
  python run_pipeline.py --sample --all                  Run sample data + final join
  python run_pipeline.py --join-final                    Join final outputs only
  python run_pipeline.py --convert-parquet data/finals/x.parquet --to csv
  python run_pipeline.py --help                          Show this help

Features:
  - Parquet format for efficient storage (snappy compression)
  - Incremental and final Parquet outputs
  - Optional explicit date ranges with --start-date and --end-date
  - Parquet conversion to CSV or Excel with --convert-parquet
  - Future-dated rows are excluded from incremental and final outputs
  - Timestamp columns for tracking
  - Columnwise joining of demand and diagnosis
  - Synthetic sample mode without database access
  - Optimized data types and memory usage
        """,
    )

    parser.add_argument(
        "--demand",
        action="store_true",
        help="Run demand pipeline only",
    )
    parser.add_argument(
        "--diagnosis",
        action="store_true",
        help="Run diagnosis pipeline only",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="Run both pipelines (default)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run both pipelines + join final outputs",
    )
    parser.add_argument(
        "--join-final",
        action="store_true",
        help="Join final demand and diagnosis outputs columnwise",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use local synthetic CSV data instead of querying the database",
    )
    parser.add_argument(
        "--sample-input-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "sample" / "input",
        help="Directory with synthetic sample CSV inputs",
    )
    parser.add_argument(
        "--sample-output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "sample" / "output",
        help="Directory where sample Parquet outputs will be written",
    )
    parser.add_argument(
        "--start-date",
        help=(
            "Optional inclusive start date (YYYY-MM-DD). If omitted, the "
            "pipeline resumes from the last processed day, or from 2008-01-01 "
            "on a first run."
        ),
    )
    parser.add_argument(
        "--end-date",
        help="Optional inclusive end date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--convert-parquet",
        type=Path,
        help="Convert a final or incremental Parquet file to CSV or Excel",
    )
    parser.add_argument(
        "--to",
        choices=["csv", "excel", "xlsx"],
        default="csv",
        help="Output format for --convert-parquet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path for --convert-parquet",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    try:
        start_date = _parse_cli_date(args.start_date, "--start-date")
        end_date = _parse_cli_date(args.end_date, "--end-date")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("--start-date cannot be after --end-date")
    except ValueError as e:
        logger.error(str(e))
        return 1

    if args.convert_parquet:
        try:
            output_path = convert_parquet_file(
                input_file=args.convert_parquet,
                output_format=args.to,
                output_file=args.output,
            )
            logger.info(f"Conversion complete: {output_path}")
            return 0
        except Exception as e:
            logger.error(f"Parquet conversion failed: {e}", exc_info=True)
            return 1

    run_demand = False
    run_diagnosis = False
    run_join = False

    if args.demand:
        run_demand = True
    elif args.diagnosis:
        run_diagnosis = True
    elif args.join_final:
        run_join = True
    elif args.all:
        run_demand = True
        run_diagnosis = True
        run_join = True
    else:
        run_demand = True
        run_diagnosis = True

    if args.both:
        run_demand = True
        run_diagnosis = True

    logger.info("=" * 80)
    if args.sample:
        logger.info("SAMPLE PREDAP PIPELINE EXECUTION")
        logger.info(f"Sample input dir: {args.sample_input_dir}")
        logger.info(f"Sample output dir: {args.sample_output_dir}")
    else:
        logger.info("OPTIMIZED PREDAP PIPELINE EXECUTION")
    if start_date is not None or end_date is not None:
        logger.info(
            "Requested date range: "
            f"{start_date.date() if start_date is not None else 'auto'} -> "
            f"{end_date.date() if end_date is not None else 'today'}"
        )
    logger.info("=" * 80)

    results = []

    if run_demand:
        if args.sample:
            success = run_demand_sample_pipeline(
                args.sample_input_dir,
                args.sample_output_dir,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Sample Demand Pipeline", success))
        else:
            success = run_demand_pipeline_optimized(
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Demand Pipeline", success))

    if run_diagnosis:
        if args.sample:
            success = run_diagnosis_sample_pipeline(
                args.sample_input_dir,
                args.sample_output_dir,
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Sample Diagnosis Pipeline", success))
        else:
            success = run_diagnosis_pipeline_optimized(
                start_date=start_date,
                end_date=end_date,
            )
            results.append(("Diagnosis Pipeline", success))

    if run_join:
        if args.sample:
            success = join_sample_final_outputs(args.sample_output_dir)
            results.append(("Sample Final Join", success))
        else:
            success = join_final_outputs()
            results.append(("Final Join", success))

    logger.info("=" * 80)
    logger.info("EXECUTION SUMMARY:")
    for name, success in results:
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"  {name}: {status}")
    logger.info("=" * 80)

    all_success = all(success for _, success in results)
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
