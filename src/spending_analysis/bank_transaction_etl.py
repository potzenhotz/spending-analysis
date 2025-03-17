import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
from dateutil.relativedelta import relativedelta
from polars.dataframe import group_by

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


def calc_previous_month(YEAR_MONTH: str, number_of_month: int = 5) -> list[str]:
    date_obj = datetime.strptime(YEAR_MONTH, "%Y-%m")

    previous_months = [
        (date_obj - relativedelta(months=i)).strftime("%Y-%m")
        for i in range(1, number_of_month + 1)
    ]
    return previous_months


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
        .sort(pl.col("Betrag"), descending=True)
        .with_columns(
            pl.col("Betrag")
            .rank(method="dense", descending=True)
            .over(
                [
                    "Analyse-Monat",
                ]
            )
            .alias("Rank")
        )
        .filter(pl.col("Rank") <= num_top)
        .select(
            [
                "Buchungstag",
                "Beguenstigter/Auftraggeber",
                "Analyse-Hauptkategorie",
                "Analyse-Unterkategorie",
                "Betrag",
                "Analyse-Monat",
            ]
        )
    )

    return monthly_top_spending


def calc_top_category(df: pl.DataFrame, num_top: int = 5) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    monthly_top_sub_category = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .sort(pl.col("Betrag"), descending=True)
        .with_columns(
            pl.col("Betrag")
            .rank(method="dense", descending=True)
            .over(
                [
                    "Analyse-Monat",
                    "Analyse-Hauptkategorie",
                ]
            )
            .alias("Rank")
        )
        .filter(pl.col("Rank") <= num_top)
        .select(
            [
                "Buchungstag",
                "Beguenstigter/Auftraggeber",
                "Analyse-Hauptkategorie",
                "Betrag",
                "Analyse-Monat",
            ]
        )
        .sort(["Analyse-Hauptkategorie", "Betrag"], descending=[False, True])
    )

    return monthly_top_sub_category


def calc_income_expenses(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    df = df.select(["Analyse-Monat", "Betrag"])
    expenses = (
        df.filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .group_by("Analyse-Monat")
        .agg(pl.sum("Betrag").alias("Ausgaben"))
    )
    income = (
        df.filter(pl.col("Betrag") > 0)
        .group_by("Analyse-Monat")
        .agg(pl.sum("Betrag").alias("Einnahmen"))
    )
    income_expenses = income.join(expenses, on="Analyse-Monat")
    return income_expenses


def _calc_spending_for_specific_column(
    df: pl.DataFrame, category_column
) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    category_spending = (
        df.select(["Analyse-Monat", category_column, "Betrag"])
        .filter(pl.col("Betrag") < 0)
        .with_columns(pl.col("Betrag") * -1)
        .with_columns(
            pl.col("Betrag")
            .sum()
            .over(["Analyse-Monat", category_column])
            .alias("Summe")
        )
        .with_columns(
            pl.col("Betrag")
            .mean()
            .over(["Analyse-Monat", category_column])
            .alias("Mittelwert")
        )
    )
    return category_spending


def calc_category_spending(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    return _calc_spending_for_specific_column(df, "Analyse-Hauptkategorie")


def calc_sub_category_spending(df: pl.DataFrame) -> pl.DataFrame:
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    return _calc_spending_for_specific_column(df, "Analyse-Unterkategorie")


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
