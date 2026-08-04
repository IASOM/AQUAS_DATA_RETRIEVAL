# Outputs

## Real Pipeline Outputs

| Path | Content |
| --- | --- |
| `data/demand_pipeline/incremental/*.parquet` | Incremental demand chunks |
| `data/demand_pipeline/finals/demand_final.parquet` | Final demand feature matrix |
| `data/diagnosis_pipeline/incremental/*.parquet` | Incremental diagnosis chunks |
| `data/diagnosis_pipeline/finals/diagnosis_final.parquet` | Final diagnosis feature matrix |
| `data/finals/demand_diagnosis_joined.parquet` | Demand and diagnosis joined by `timestamp` |

The output directories are created automatically when the pipeline runs.

## Toy Data Outputs

Recommended toy output path:

```text
data/sample/toy_check/
```

Main files:

| Path | Content |
| --- | --- |
| `data/sample/toy_check/demand_pipeline/finals/demand_final.parquet` | Toy demand final |
| `data/sample/toy_check/diagnosis_pipeline/finals/diagnosis_final.parquet` | Toy diagnosis final |
| `data/sample/toy_check/finals/demand_diagnosis_joined.parquet` | Toy joined output |

## Imputation Metadata

Final Parquet files can have metadata sidecars:

```text
*_imputation_metadata.json
*_imputed_rows.csv
```

These describe imputed rows and exact imputed dates.

## Convert Parquet To CSV

```bash
python run_pipeline_optimized.py --convert-parquet data/sample/toy_check/finals/demand_diagnosis_joined.parquet --to csv
```

## Inspect Rows

```bash
python run_pipeline_optimized.py --show-parquet data/sample/toy_check/finals/demand_diagnosis_joined.parquet --parquet-limit 10
```
