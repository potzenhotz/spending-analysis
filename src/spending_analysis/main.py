import logging
from pathlib import Path
import polars as pl

from spending_analysis.bank_report import generate_pdf
from spending_analysis.bank_transaction_etl import (
    calc_category_spending,
    calc_monthly_spending,
    calc_monthly_top_spending,
    calc_sub_category_spending,
    load_data,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
YEAR_MONTH = "2025-02"

current_file_path = Path(__file__).parent.resolve()

if __name__ == "__main__":
    input_file = (
        f"{current_file_path}/resources/input/20250314-Export-Alle_Buchungen.xlsx"
    )
    output_dir = f"{current_file_path}/resources/output/"

    df = load_data(file_name=None)
    df_ing = df.filter(pl.col("Name Referenzkonto") == "Girokonto")
    df_ing_year_month = df_ing.filter(pl.col("Analyse-Monat") == YEAR_MONTH)

    df_monthly_spending = calc_monthly_spending(df_ing)
    df_monthly_top_spending = calc_monthly_top_spending(df_ing_year_month)
    df_category_spending = calc_category_spending(df_ing_year_month)
    df_sub_category_spending = calc_sub_category_spending(df_ing_year_month)

    generate_pdf(
        YEAR_MONTH,
        df_monthly_spending,
        df_monthly_top_spending,
        df_category_spending,
        df_sub_category_spending,
    )
