import io
import logging
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from dateutil.relativedelta import relativedelta
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    Image,
    PageBreak,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

matplotlib.use("Agg")  # Use non-interactive backend

current_file_path = Path(__file__).parent.resolve()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _calc_previous_month(YEAR_MONTH: str, number_of_month: int = 5) -> list[str]:
    date_obj = datetime.strptime(YEAR_MONTH, "%Y-%m")

    previous_months = [
        (date_obj - relativedelta(months=i)).strftime("%Y-%m")
        for i in range(1, number_of_month + 1)
    ]
    return previous_months


def plot_balance_over_time(df, YEAR_MONTH):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    df = df.with_columns(pl.col("Buchungstag").dt.day().alias("Tag"))
    df_current_month = df.filter(pl.col("Analyse-Monat") == YEAR_MONTH)
    previous_months = _calc_previous_month(YEAR_MONTH)
    dfs_to_plot_prev = [
        df.filter(pl.col("Analyse-Monat") == month) for month in previous_months
    ]
    sns.lineplot(
        x=df_current_month["Tag"],
        y=df_current_month["Summe"],
        marker="o",
        linestyle="-",
        linewidth=2,
        color="#1f77b4",
    )
    for df_prev in dfs_to_plot_prev:
        sns.lineplot(
            x=df_prev["Tag"],
            y=df_prev["Summe"],
            marker="o",
            linestyle="-",
            linewidth=1,
            color="grey",
        )
    plt.title("Kontostand über die Zeit", fontsize=14, fontweight="bold")
    plt.xlabel("Datum", fontsize=10)
    plt.ylabel("Ausgaben (€)", fontsize=10)
    plt.xticks(rotation=45)
    plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%d"))
    plt.tight_layout()

    return save_plot_to_buffer()


def plot_category_spending(df, cut: bool = False):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")

    color_values = df["Analyse-Hauptkategorie"].to_list()
    unique_color_values = list(set(color_values))
    color_palette = sns.color_palette("viridis", len(unique_color_values))
    color_mapping = {
        unique_color_values[i]: color_palette[i]
        for i in range(len(unique_color_values))
    }
    mapped_colors = [color_mapping[val] for val in color_values]

    # plt.figure(figsize=(8, 4))
    sns.barplot(y=df["Summe"], x=df["Analyse-Hauptkategorie"], palette=mapped_colors)
    plt.title("Ausgaben nach Hauptkategorie")
    plt.xlabel("Betrag (€)")
    plt.xticks(rotation=45, ha="right")
    if cut:
        plt.ylim(0, 750)

    return save_plot_to_buffer()


def plot_sub_category_spending(df, cut: bool = False):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")

    color_values = df["Analyse-Hauptkategorie"].to_list()
    unique_color_values = list(set(color_values))
    color_palette = sns.color_palette("viridis", len(unique_color_values))
    color_mapping = {
        unique_color_values[i]: color_palette[i]
        for i in range(len(unique_color_values))
    }
    mapped_colors = [color_mapping[val] for val in color_values]

    # plt.figure(figsize=(8, 6))
    sns.barplot(y=df["Summe"], x=df["Analyse-Unterkategorie"], palette=mapped_colors)
    plt.title("Ausgaben nach Unterkategorie")
    plt.xlabel("Betrag (€)")
    plt.xticks(rotation=45, ha="right")
    if cut:
        plt.ylim(0, 1000)
    return save_plot_to_buffer()


def save_plot_to_image():
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    plt.close()
    buf.seek(0)
    return ImageReader(buf)


def save_plot_to_buffer():
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    plt.close()
    buf.seek(0)
    return buf


def save_df_as_pdf_table(df: pl.DataFrame):
    data = []
    data.extend([df.columns])
    data.extend(df.rows())

    table = Table(data)

    table.setStyle(
        TableStyle(
            [
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    return table


def generate_pdf(
    YEAR_MONTH: str,
    df_monthly_spending: pl.DataFrame,
    df_monthly_top_spending: pl.DataFrame,
    df_category_spending: pl.DataFrame,
    df_sub_category_spending: pl.DataFrame,
):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    pdf = SimpleDocTemplate(
        f"{current_file_path}/resources/output/Spending_Report_{YEAR_MONTH}.pdf",
        pagesize=A4,
    )
    elements = []
    elements.append(Spacer(1, 1.25 * cm))
    # available_width = A4[0] - 2 * cm  # Subtract some margin

    img_1 = Image(plot_balance_over_time(df_monthly_spending, YEAR_MONTH))
    img_1.drawWidth = 16 * cm  # Explicitly set width
    img_1.drawHeight = 12 * cm  # Explicitly set height
    elements.append(img_1)
    elements.append(Spacer(1, 1.25 * cm))
    table_largest_spendings = save_df_as_pdf_table(df_monthly_top_spending)
    elements.append(table_largest_spendings)

    # INFO: Page 2
    elements.append(PageBreak())

    img_2 = Image(plot_category_spending(df_category_spending, cut=True))
    img_2.drawWidth = 16 * cm  # Explicitly set width
    img_2.drawHeight = 12 * cm  # Explicitly set height
    elements.append(img_2)

    img_3 = Image(plot_sub_category_spending(df_sub_category_spending, cut=True))
    img_3.drawWidth = 16 * cm  # Explicitly set width
    img_3.drawHeight = 12 * cm  # Explicitly set height
    elements.append(img_3)

    # INFO: Build pdf
    pdf.build(elements)
