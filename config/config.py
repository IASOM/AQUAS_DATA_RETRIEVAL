"""
Centralized configuration management.

This module loads configuration from environment variables and provides
defaults for both demand and diagnosis pipelines.
"""
import os
from pathlib import Path
from typing import Optional


class Config:
    """Base configuration class."""

    # Database Configuration
    DB_SERVER = os.getenv("DB_SERVER", "synw-aquas.sql.azuresynapse.net")
    DB_DATABASE = os.getenv("DB_DATABASE", "aquas")
    AUTH_MODE = os.getenv("AUTH_MODE", "ActiveDirectoryIntegrated")

    # Base Paths
    BASE_DIR = Path(os.getenv("BASE_DIR", Path.cwd()))
    DATA_DIR = BASE_DIR / "data"
    CONFIG_DIR = BASE_DIR / "config"

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


class DemandConfig(Config):
    """Demand pipeline configuration."""

    SCHEMA = "z_inv"
    TABLE_NAME = "P1038_visites"
    DATE_COLUMN = "DATA_VISITA"

    # Data paths
    PIPELINE_DATA_DIR = Config.DATA_DIR / "demand_pipeline"
    STATE_FILE = PIPELINE_DATA_DIR / "state" / "state.json"

    OUTPUT_CAT_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_CAT_incremental.csv"
    OUTPUT_RS_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_RS_incremental.csv"
    OUTPUT_UP_FILE = PIPELINE_DATA_DIR / "incremental" / "demanda_UP_incremental.csv"

    FINAL_CAT_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_CAT.csv"
    FINAL_RS_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_RS.csv"
    FINAL_UP_FILE = PIPELINE_DATA_DIR / "finals" / "demanda_UP.csv"

    # Reference data
    UP_RS_FILE = Path(os.getenv("UP_RS_FILE", Config.BASE_DIR / "UPperRS.xlsx"))
    UP_RS_SHEET = "UP per RS"

    # Date settings
    MIN_VALID_DATE = "2008-01-01"
    FINAL_START_DATE = "2008-01-01"
    FINAL_END_DATE = None  # Will use today - 1 day at runtime

    @classmethod
    def get_final_end_date(cls):
        """Get final end date (today - 1 day)."""
        import pandas as pd
        return pd.Timestamp.today().normalize() - pd.Timedelta(days=1)


class DiagnosisConfig(Config):
    """Diagnosis pipeline configuration."""

    SCHEMA = "z_inv"
    TABLE_NAME = "P1038_prstb015r_filtrat"
    DATE_COLUMN = "data_visita"
    UP_COLUMN = "up_c"
    DIAG_CODE_COLUMN = "problema_salut_c"

    # Data paths
    PIPELINE_DATA_DIR = Config.DATA_DIR / "diagnosis_pipeline"
    STATE_FILE = PIPELINE_DATA_DIR / "state" / "state.json"
    SELECTED_CODES_FILE = PIPELINE_DATA_DIR / "selected_codes" / "selected_codes.csv"

    OUTPUT_CAT_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_CAT_incremental.csv"
    OUTPUT_RS_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_RS_incremental.csv"
    OUTPUT_UP_FILE = PIPELINE_DATA_DIR / "incremental" / "selected_UP_incremental.csv"

    FINAL_CAT_FILE = PIPELINE_DATA_DIR / "finals" / "selected_CAT.csv"
    FINAL_RS_FILE = PIPELINE_DATA_DIR / "finals" / "selected_RS.csv"
    FINAL_UP_FILE = PIPELINE_DATA_DIR / "finals" / "selected_UP.csv"

    # Reference data
    UP_RS_FILE = Path(os.getenv("UP_RS_FILE", Config.BASE_DIR / "UPperRS.xlsx"))
    UP_RS_SHEET = "UP per RS"

    # Date settings
    MIN_VALID_DATE = "2008-01-01"
    FINAL_START_DATE = "2008-01-01"
    FINAL_END_DATE = None  # Will use today - 1 day at runtime

    @classmethod
    def get_final_end_date(cls):
        """Get final end date (today - 1 day)."""
        import pandas as pd
        return pd.Timestamp.today().normalize() - pd.Timedelta(days=1)


def get_config(pipeline: str = "demand") -> Config:
    """
    Get configuration for a specific pipeline.

    Args:
        pipeline: "demand" or "diagnosis"

    Returns:
        Config: Configuration object
    """
    if pipeline.lower() == "demand":
        return DemandConfig()
    elif pipeline.lower() == "diagnosis":
        return DiagnosisConfig()
    else:
        raise ValueError(f"Unknown pipeline: {pipeline}")
