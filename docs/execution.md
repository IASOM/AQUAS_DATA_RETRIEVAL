# Execution

The stable entry point is:

```bash
python run_pipeline.py
```

`run_pipeline.py` delegates to `run_pipeline_optimized.py`, which contains the active Parquet implementation.

## Common Commands

Run both pipelines:

```bash
python run_pipeline_optimized.py --both
```

Run demand only:

```bash
python run_pipeline_optimized.py --demand
```

Run diagnosis only:

```bash
python run_pipeline_optimized.py --diagnosis
```

Run demand, diagnosis, and the final joined output:

```bash
python run_pipeline_optimized.py --all
```

Join existing final outputs only:

```bash
python run_pipeline_optimized.py --join-final
```

## Date Ranges

Manual date ranges are inclusive:

```bash
python run_pipeline_optimized.py --all --start-date 2024-01-01 --end-date 2024-12-31
python run_pipeline_optimized.py --demand --start-date 2026-05-01
python run_pipeline_optimized.py --diagnosis --end-date 2026-05-20
```

If no date range is passed, the pipeline resumes incrementally. If no previous final file exists, it starts from `2008-01-01`.

## Future Dates And Imputation

The pipeline excludes future-dated source rows. If the source database has not yet published data through the requested final date, final Parquet outputs can be extended with imputed rows.

Imputed rows are marked with:

```text
__is_imputed
__imputation_method
__imputation_source_last_date
__imputation_created_at
```
