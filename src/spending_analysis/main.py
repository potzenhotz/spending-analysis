import logging
from pathlib import Path

import polars as pl

from spending_analysis.bank_report import generate_pdf
from spending_analysis.bank_transaction_etl import (
    calc_category_spending,
    calc_income_expenses,
    calc_monthly_spending,
    calc_monthly_top_spending,
    calc_previous_month,
    calc_sub_category_spending,
    calc_top_category,
    load_data,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
YEAR_MONTH = "2025-02"

current_file_path = Path(__file__).parent.resolve()

if __name__ == "__main__":
    exclude_house_renovation = True
    file_name = "20250317-Export-Alle_Buchungen.xlsx"
    input_file = f"{current_file_path}/resources/input/{file_name}"
    output_dir = f"{current_file_path}/resources/output/"

    previous_months = calc_previous_month(YEAR_MONTH, 11)
    months = [YEAR_MONTH] + previous_months
    df = load_data(file_name=file_name).filter(pl.col("Analyse-Monat").is_in(months))
    df_ing = df.filter(pl.col("Name Referenzkonto") == "Girokonto")
    if exclude_house_renovation:
        df_ing = df.filter(pl.col("Name Referenzkonto") == "Girokonto").filter(
            pl.col("Analyse-Unterkategorie") != "Bauen / Renovieren"
        )
    else:
        df_ing = df.filter(pl.col("Name Referenzkonto") == "Girokonto")

    df_income_expenses = calc_income_expenses(df_ing)
    df_monthly_spending = calc_monthly_spending(df_ing)
    df_monthly_top_spending = calc_monthly_top_spending(df_ing)
    df_monthly_top_category = calc_top_category(df_ing, 3)
    df_category_spending = calc_category_spending(df_ing)
    df_sub_category_spending = calc_sub_category_spending(df_ing)

    generate_pdf(
        YEAR_MONTH,
        df_income_expenses=df_income_expenses,
        df_monthly_spending=df_monthly_spending,
        df_monthly_top_spending=df_monthly_top_spending.filter(
            pl.col("Analyse-Monat") == YEAR_MONTH
        ).drop("Analyse-Monat"),
        df_monthly_top_category=df_monthly_top_category.filter(
            pl.col("Analyse-Monat") == YEAR_MONTH
        ).drop("Analyse-Monat"),
        df_category_spending=df_category_spending.filter(
            pl.col("Analyse-Monat") == YEAR_MONTH
        ),
        df_sub_category_spending=df_sub_category_spending.filter(
            pl.col("Analyse-Monat") == YEAR_MONTH
        ),
    )
