# Validation

## Unit Tests

```bash
python -m pytest -q
```

Expected result:

```text
19 passed
```

## Compile Main Modules

```bash
python -m py_compile run_pipeline_optimized.py config/config.py pipelines/sample_runner.py pipelines/demand/incremental_optimized.py pipelines/diagnosis/incremental_optimized.py pipelines/shared/final_joiner.py pipelines/shared/naming.py
```

## Run Toy Data End To End

```bash
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
```

## Check Output Column Names

```bash
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/diagnosis_pipeline/finals/diagnosis_final.parquet'); print(df.shape); print('\n'.join(df.columns))"
```

The output should contain canonical columns such as:

```text
DIAGNOSIS__ICD10_3__G01
DIAGNOSIS__ICD10_3__D09
DIAGNOSIS__TOTAL__RS__RS_67
DIAGNOSIS__TOTAL__UP__MICRO_01
```

## Check Selection Loading

```bash
python -c "from pipelines.diagnosis.incremental_optimized import _load_selected_codes; codes=_load_selected_codes('selections/selected_diagnosis_codes.csv'); print(len(codes)); print(sorted({a for xs in codes.values() for a in xs}))"
```

The current configuration expands to 355 source ICD10_3 codes and 14 output groups.
