# AQUAS / PREDAP Data Pipelines

This documentation describes the active Parquet implementation of the AQUAS / PREDAP data pipelines.

The project builds daily feature matrices for:

| Pipeline | Source table | Main output |
| --- | --- | --- |
| Demand | `z_inv.P1038_visites` | `data/demand_pipeline/finals/demand_final.parquet` |
| Diagnosis | `z_inv.P1038_prstb015r_filtrat` | `data/diagnosis_pipeline/finals/diagnosis_final.parquet` |

The two final matrices can also be joined by `timestamp`:

```text
data/finals/demand_diagnosis_joined.parquet
```

## Main Concepts

- Outputs are daily Parquet files with `timestamp` as the date column.
- Column names follow the canonical Qualud grammar: `DEMAND__...` and `DIAGNOSIS__...`.
- Diagnosis selections include grouped outputs `G01` to `G05` and concrete diagnosis outputs `D01` to `D09`.
- Territory selections can map source RS and UP values to stable IDs such as `RS_64` and `MICRO_01`.
- Toy data can be used to validate the full flow without database access.

## Useful Entry Points

```bash
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
python run_pipeline_optimized.py --all
python -m pytest -q
```
