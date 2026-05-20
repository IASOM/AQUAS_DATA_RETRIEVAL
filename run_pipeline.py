"""Compatibility entry point for the optimized Parquet pipelines.

The original runner referenced non-optimized modules that are no longer present
under ``pipelines/``. Keep this file as the stable command users can run, while
delegating the actual work to ``run_pipeline_optimized.py``.
"""
import sys

from run_pipeline_optimized import (
    join_final_outputs,
    main as _optimized_main,
    run_demand_pipeline_optimized,
    run_diagnosis_pipeline_optimized,
)


def run_demand_pipeline(config=None):
    """Run the demand pipeline through the optimized implementation."""
    return run_demand_pipeline_optimized(config)


def run_diagnosis_pipeline(config=None):
    """Run the diagnosis pipeline through the optimized implementation."""
    return run_diagnosis_pipeline_optimized(config)


def main():
    """Delegate CLI handling to the optimized runner."""
    return _optimized_main()


if __name__ == "__main__":
    sys.exit(main())
