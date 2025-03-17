import io
import logging
import sys
from pathlib import Path

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from spending_analysis.bank_transaction_etl import calc_previous_month

matplotlib.use("Agg")  # Use non-interactive backend

current_file_path = Path(__file__).parent.resolve()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def plot_income_expenses(df, YEAR_MONTH):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    previous_months = calc_previous_month(YEAR_MONTH, 5)
    months = [YEAR_MONTH] + previous_months
    df = df.filter(pl.col("Analyse-Monat").is_in(months)).sort(
        "Analyse-Monat", descending=True
    )

    unpivot_df = df.unpivot(
        on=["Einnahmen", "Ausgaben"],
        index=[
            "Analyse-Monat",
        ],
        variable_name="Metrik",
        value_name="Wert",
    ).sort(["Analyse-Monat", "Metrik"], descending=[False, True])

    sns.catplot(
        data=unpivot_df,
        x="Analyse-Monat",
        y="Wert",
        hue="Metrik",
        kind="bar",
        aspect=1.5,
        palette=[sns.color_palette("Paired")[2], sns.color_palette("Paired")[4]],
    )

    plt.title("Einnahmen vs. Ausgaben", fontsize=14, fontweight="bold")
    plt.xlabel("Monat", fontsize=10)
    plt.ylabel("Betrag (€)", fontsize=10)
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, linestyle="--", axis="y", linewidth=0.5, alpha=0.5)

    return save_plot_to_buffer()


def plot_balance_over_time(df, YEAR_MONTH):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    previous_months = calc_previous_month(YEAR_MONTH, 5)
    months = [YEAR_MONTH] + previous_months
    df = (
        df.with_columns(pl.col("Buchungstag").dt.day().alias("Tag"))
        .filter(pl.col("Analyse-Monat").is_in(months))
        .sort("Analyse-Monat", descending=True)
    )
    sns.lineplot(
        x=df["Tag"],
        y=df["Summe"],
        hue=df["Analyse-Monat"],
        style=df["Analyse-Monat"],
        markers=True,
        linewidth=1,  # Make it stand out
    )
    # Re-plot the highlighted month with a thicker line on top
    df_current_month = df.filter(pl.col("Analyse-Monat") == YEAR_MONTH)
    sns.lineplot(
        x=df_current_month["Tag"],
        y=df_current_month["Summe"],
        color=sns.color_palette()[0],  # Keep the same color as hue
        linewidth=3,  # Make it stand out
        label=None,  # Avoid duplicate legend entry
    )

    plt.title("Ausgaben Verlauf", fontsize=14, fontweight="bold")
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

    unpivot_df = df.unpivot(
        on=["Summe", "Mittelwert"],
        index=[
            "Analyse-Hauptkategorie",
        ],
        variable_name="Metrik",
        value_name="Wert",
    ).sort(["Wert", "Metrik"], descending=True)

    sns.catplot(
        data=unpivot_df,
        x="Analyse-Hauptkategorie",
        y="Wert",
        hue="Metrik",
        kind="bar",
        aspect=1.5,
        palette=[sns.color_palette("Paired")[1], sns.color_palette("Paired")[0]],
    )

    plt.title("Ausgaben nach Hauptkategorie", fontsize=14, fontweight="bold")
    plt.xlabel("Kategorie", fontsize=10)
    plt.ylabel("Betrag (€)", fontsize=10)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    if cut:
        plt.ylim(0, 750)

    return save_plot_to_buffer()


def plot_sub_category_spending(df, cut: bool = False):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")

    unpivot_df = df.unpivot(
        on=["Summe", "Mittelwert"],
        index=[
            "Analyse-Unterkategorie",
        ],
        variable_name="Metrik",
        value_name="Wert",
    ).sort(["Wert", "Metrik"], descending=True)

    sns.catplot(
        data=unpivot_df,
        x="Analyse-Unterkategorie",
        y="Wert",
        hue="Metrik",
        kind="bar",
        aspect=1.5,
        palette=[sns.color_palette("Paired")[1], sns.color_palette("Paired")[0]],
    )

    plt.title("Ausgaben nach Unterkategorie", fontsize=14, fontweight="bold")
    plt.xlabel("Unterkategorie", fontsize=10)
    plt.ylabel("Betrag (€)", fontsize=10)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
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
    df_income_expenses: pl.DataFrame,
    df_monthly_spending: pl.DataFrame,
    df_monthly_top_spending: pl.DataFrame,
    df_monthly_top_category: pl.DataFrame,
    df_category_spending: pl.DataFrame,
    df_sub_category_spending: pl.DataFrame,
):
    logger.info(f"Using function: {sys._getframe().f_code.co_name}")
    pdf = SimpleDocTemplate(
        f"{current_file_path}/resources/output/Spending_Report_{YEAR_MONTH}.pdf",
        pagesize=A4,
    )
    elements = []
    styles = getSampleStyleSheet()
    title_style = styles["Title"]

    title = Paragraph(
        f"Report for {YEAR_MONTH} without 'Bauen / Renovieren'", title_style
    )
    elements.append(title)
    # available_width = A4[0] - 2 * cm  # Subtract some margin

    img_ie = Image(plot_income_expenses(df_income_expenses, YEAR_MONTH))
    img_ie.drawWidth = 15 * cm  # Explicitly set width
    img_ie.drawHeight = 10 * cm  # Explicitly set height
    elements.append(img_ie)

    elements.append(Spacer(1, 1.25 * cm))

    img_bot = Image(plot_balance_over_time(df_monthly_spending, YEAR_MONTH))
    img_bot.drawWidth = 15 * cm  # Explicitly set width
    img_bot.drawHeight = 10 * cm  # Explicitly set height
    elements.append(img_bot)

    # INFO: Page 2
    elements.append(PageBreak())

    img_cs = Image(plot_category_spending(df_category_spending))
    img_cs.drawWidth = 15 * cm  # Explicitly set width
    img_cs.drawHeight = 10 * cm  # Explicitly set height
    elements.append(img_cs)

    elements.append(Spacer(1, 1 * cm))

    img_scs = Image(plot_sub_category_spending(df_sub_category_spending))
    img_scs.drawWidth = 15 * cm  # Explicitly set width
    img_scs.drawHeight = 10 * cm  # Explicitly set height
    elements.append(img_scs)

    # INFO: Page 3
    elements.append(PageBreak())
    table_largest_spendings = save_df_as_pdf_table(df_monthly_top_spending)
    elements.append(table_largest_spendings)
    elements.append(Spacer(1, 1 * cm))
    table_category = save_df_as_pdf_table(df_monthly_top_category)
    elements.append(table_category)

    # INFO: Build pdf
    pdf.build(elements)
