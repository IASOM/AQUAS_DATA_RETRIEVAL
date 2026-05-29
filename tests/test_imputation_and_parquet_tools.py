from pathlib import Path

import pandas as pd

from pipelines.shared.imputation import (
    IMPUTATION_METHOD_COL,
    IMPUTATION_SOURCE_LAST_DATE_COL,
    IMPUTED_COL,
    SAME_MONTH_DAY_METHOD,
    drop_imputed_rows,
    impute_tail_to_date,
)
from run_pipeline_optimized import (
    check_parquet_imputation,
    delete_parquet_rows,
    print_parquet_rows,
)


def test_impute_tail_to_date_marks_estimated_rows_and_uses_same_month_day_mean():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2022-01-02",
                    "2023-01-02",
                    "2024-01-01",
                ]
            ),
            "value": [8, 12, 30],
        }
    )

    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )

    imputed_row = result[result["timestamp"] == pd.Timestamp("2024-01-02")].iloc[0]
    assert bool(imputed_row[IMPUTED_COL]) is True
    assert imputed_row[IMPUTATION_METHOD_COL] == SAME_MONTH_DAY_METHOD
    assert imputed_row[IMPUTATION_SOURCE_LAST_DATE_COL] == "2024-01-01"
    assert imputed_row["value"] == 10

    observed = result[result["timestamp"] <= pd.Timestamp("2024-01-01")]
    assert observed[IMPUTED_COL].eq(False).all()


def test_drop_imputed_rows_removes_estimates_and_metadata_columns():
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01"]),
            "value": [30],
        }
    )
    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )

    cleaned = drop_imputed_rows(result)

    assert cleaned["timestamp"].tolist() == [pd.Timestamp("2024-01-01")]
    assert IMPUTED_COL not in cleaned.columns


def test_delete_parquet_rows_removes_inclusive_date_range_and_writes_backup(tmp_path):
    parquet_path = tmp_path / "rows.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    ).to_parquet(parquet_path, index=False)

    deleted_rows, remaining_rows, backup_path = delete_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2024-01-02"),
        end_date=pd.Timestamp("2024-01-02"),
    )

    remaining = pd.read_parquet(parquet_path)
    assert deleted_rows == 1
    assert remaining_rows == 2
    assert backup_path is not None
    assert Path(backup_path).exists()
    assert remaining["timestamp"].tolist() == [
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-03"),
    ]


def test_print_parquet_rows_filters_by_date_range(tmp_path, capsys):
    parquet_path = tmp_path / "rows.parquet"
    pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    ).to_parquet(parquet_path, index=False)

    filtered = print_parquet_rows(
        parquet_path,
        start_date=pd.Timestamp("2024-01-02"),
        end_date=pd.Timestamp("2024-01-03"),
        columns=["value"],
        limit=0,
    )

    output = capsys.readouterr().out
    assert len(filtered) == 2
    assert "Rows matched from 2024-01-02 to 2024-01-03: 2 of 3" in output
    assert "2024-01-02" in output
    assert "2024-01-03" in output


def test_check_parquet_imputation_accepts_valid_metadata(tmp_path):
    parquet_path = tmp_path / "imputed.parquet"
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2022-01-02",
                    "2023-01-02",
                    "2024-01-01",
                ]
            ),
            "value": [8, 12, 30],
        }
    )
    result = impute_tail_to_date(
        df,
        observed_until=pd.Timestamp("2024-01-01"),
        target_until=pd.Timestamp("2024-01-02"),
    )
    result.to_parquet(parquet_path, index=False)

    assert check_parquet_imputation(parquet_path)
