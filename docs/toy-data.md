# Toy Data

Toy data lets you validate the pipeline without ODBC or database permissions.

## Run The Full Toy Flow

```bash
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
```

This runs:

- demand pipeline
- diagnosis pipeline
- final demand + diagnosis join

The sample runner reads CSV files from:

```text
data/sample/input/
```

and writes Parquet outputs to:

```text
data/sample/toy_check/
```

## Sample Inputs

| File | Purpose |
| --- | --- |
| `data/sample/input/up_rs.csv` | Toy UP to RS mapping |
| `data/sample/input/demand_visits.csv` | Toy demand visits |
| `data/sample/input/diagnosis_visits.csv` | Toy diagnosis visits that activate `G01`-`G05` and `D01`-`D09` |
| `data/sample/input/selected_codes.csv` | Local fallback for diagnosis selections |

The normal sample run prioritizes the shared selection files in `selections/`.

## Inspect Toy Outputs

Demand:

```bash
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/demand_pipeline/finals/demand_final.parquet'); print(df.shape); print('\n'.join(df.columns[:30]))"
```

Diagnosis:

```bash
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/diagnosis_pipeline/finals/diagnosis_final.parquet'); print(df.shape); print('\n'.join(df.columns))"
```

Joined output:

```bash
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/finals/demand_diagnosis_joined.parquet'); print(df.shape)"
```

## Expected Diagnosis Columns

The toy diagnosis output should include columns such as:

```text
DIAGNOSIS__ICD10_3__G01
DIAGNOSIS__ICD10_3__G04
DIAGNOSIS__ICD10_3__D08
DIAGNOSIS__ICD10_3__G01__RS__RS_67
DIAGNOSIS__ICD10_3__G01__UP__MICRO_01
DIAGNOSIS__TOTAL__RS__RS_67
DIAGNOSIS__TOTAL__UP__MICRO_01
```

## Automated Test

```bash
python -m pytest -q
```
