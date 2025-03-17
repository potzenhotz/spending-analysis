import logging
import sys
import os
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
pl.Config.set_fmt_str_lengths(260)
pl.Config.set_tbl_width_chars(260)
pl.Config(tbl_cols=-1)
YEAR = "2024"
MONTH = "06"

current_file_path = Path(__file__).parent.resolve()


def load_data(file_name: Optional[str]) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    if not file_name:
        file_name = "20250314-Export-Alle_Buchungen.xlsx"
    sheet_name = file_name.replace("-", "_").replace(".xlsx", "")
    xlsx_path = Path(f"{current_file_path}/resources/input/{file_name}")
    df_raw = pl.read_excel(
        source=xlsx_path,
        sheet_name=sheet_name,
    )
    return df_raw


def calc_monthly_spending(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    monthly_spending = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .group_by(["Analyse-Monat", "Buchungstag"])
        .agg(pl.sum("Betrag"))
        .sort(pl.col("Buchungstag"))
        .with_columns(pl.col("Betrag").cum_sum().over(["Analyse-Monat"]).alias("Summe"))
        .select(["Analyse-Monat", "Buchungstag", "Summe"])
        .sort(pl.col("Buchungstag"))
    )

    return monthly_spending


def calc_monthly_top_spending(df: pl.DataFrame, num_top: int = 10) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    monthly_top_spending = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .select(
            [
                "Buchungstag",
                "Beguenstigter/Auftraggeber",
                "Betrag",
                "Analyse-Hauptkategorie",
                "Analyse-Unterkategorie",
            ]
        )
        .sort(pl.col("Betrag"), descending=True)
        .head(num_top)
    )

    return monthly_top_spending


def calc_category_spending(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    category_spending = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .group_by(["Analyse-Monat", "Analyse-Hauptkategorie"])
        .agg(
            [
                pl.sum("Betrag").alias("Summe"),
                pl.count("Betrag").alias("Anzahl Buchungen pro Kategorie"),
            ]
        )
        .sort(["Analyse-Hauptkategorie"])
    )
    return category_spending


def calc_sub_category_spending(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    sub_category_spending = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .group_by(
            [
                "Analyse-Monat",
                "Analyse-Hauptkategorie",
                "Analyse-Unterkategorie",
            ]
        )
        .agg(
            [
                pl.sum("Betrag").alias("Summe"),
                pl.count("Betrag").alias("Anzahl Buchungen pro Unterkategorie"),
            ]
        )
        .sort(["Analyse-Hauptkategorie", "Analyse-Unterkategorie"])
    )
    return sub_category_spending


def write_data(data_dict, output_dir):
    """
    Write transformed data to files
    """
    print(f"Writing data to {output_dir}")

    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Write each dataframe to a parquet file
    for name, df in data_dict.items():
        output_path = os.path.join(output_dir, f"{name}.parquet")
        df.write_parquet(output_path)
        print(f"Written {name} to {output_path}")

    return output_dir
