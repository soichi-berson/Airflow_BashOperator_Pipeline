"""
amazon/amazon_daily_class.py

Callable from the command line by Airflow BashOperator:
    python amazon_daily_class.py --step loading
    python amazon_daily_class.py --step clean
    python amazon_daily_class.py --step prepare   --date 2026-02-07
    python amazon_daily_class.py --step week
    python amazon_daily_class.py --step month     --date 2026-02-07
    python amazon_daily_class.py --step pdf
    python amazon_daily_class.py --step upload
"""

import os
import json
import logging
import argparse
from datetime import datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, Image
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import pagesizes
from reportlab.lib.units import inch

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Shared temp paths  (all tasks read/write from these fixed locations)
# ------------------------------------------------------------------
PATHS = {
    "raw":           "/tmp/raw_data.csv",
    "cleaned":       "/tmp/cleaned_data.csv",
    "one_week":      "/tmp/one_week.csv",
    "one_month":     "/tmp/one_month.csv",
    "week_results":  "/tmp/one_week_results.json",
    "month_results": "/tmp/one_month_results.json",
    "file_paths":    "/tmp/file_paths.json",
}

# S3 config
S3_BUCKET   = os.environ["S3_BUCKET"]
S3_RAW_KEY  = os.environ["S3_RAW_KEY"]

# Demo mode
DEMO_MODE   = os.environ["DEMO_MODE"].lower() == "true"
FORCED_DATE = os.environ["FORCED_DATE"]


# ==================================================================
class ReportAnalysis:
# ==================================================================

    def __init__(self, file_path: str = PATHS["raw"]):
        self.file_path = file_path

    # --------------------------------------------------------------
    # Step 1 — Load from S3
    # --------------------------------------------------------------
    def loading(self) -> None:
        """Download CSV from S3 and save to /tmp/raw_data.csv"""
        import boto3  

        logger.info(f"Loading data from S3: s3://{S3_BUCKET}/{S3_RAW_KEY}")

        s3  = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_RAW_KEY)
        df  = pd.read_csv(obj["Body"])

        df.to_csv(PATHS["raw"], index=False)
        logger.info(f"Raw data saved → {PATHS['raw']}  shape={df.shape}")

    # --------------------------------------------------------------
    # Step 2 — Clean
    # --------------------------------------------------------------
    def clean_data(self) -> None:
        """Read raw CSV, clean it, save to /tmp/cleaned_data.csv"""
        logger.info("Cleaning data.")

        df = pd.read_csv(PATHS["raw"])

        # Parse delivery_date
        if "delivery_date" not in df.columns:
            raise ValueError("Missing required column: delivery_date")
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")

        # Convert numeric columns
        numeric_cols  = ["total_sales", "shipping_cost", "discount", "quantity"]
        existing_cols = [c for c in numeric_cols if c in df.columns]
        df[existing_cols] = df[existing_cols].apply(pd.to_numeric, errors="coerce")

        # Gross sales
        df["gross_sales"] = (
            (df["total_sales"] - df["shipping_cost"]) /
            (1 - df["discount"]).replace(0, pd.NA)
        )

        df.to_csv(PATHS["cleaned"], index=False)
        logger.info(f"Cleaned data saved → {PATHS['cleaned']}")

    # --------------------------------------------------------------
    # Step 3 — Prepare reporting windows
    # --------------------------------------------------------------
    def prepare_reporting_data(self, today: datetime) -> None:
        """
        Split cleaned data into one_week and one_month windows.
        Saves:  /tmp/one_week.csv  and  /tmp/one_month.csv
        """
        logger.info("Preparing reporting windows.")

        df    = pd.read_csv(PATHS["cleaned"])
        today = pd.Timestamp(today).normalize()

        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")

        one_week_before  = today - timedelta(days=6)
        one_month_before = today - timedelta(days=27)

        one_week_df  = df.loc[df["delivery_date"].between(one_week_before, today)].copy()
        one_month_df = df.loc[df["delivery_date"].between(one_month_before, today)].copy()

        # Add week label to monthly df
        one_month_df["week"] = (
            "week-" +
            (((today - one_month_df["delivery_date"]).dt.days // 7) + 1).astype(str)
        )

        one_week_df.to_csv(PATHS["one_week"],   index=False)
        one_month_df.to_csv(PATHS["one_month"], index=False)

        logger.info(
            f"one_week rows={len(one_week_df)}, "
            f"one_month rows={len(one_month_df)}"
        )

    # --------------------------------------------------------------
    # Step 4 — One-week KPI analysis
    # --------------------------------------------------------------
    def one_week_analysis(self) -> None:
        """
        Read /tmp/one_week.csv, calculate KPIs.
        Saves results → /tmp/one_week_results.json
        """
        logger.info("Running one-week analysis.")

        df = pd.read_csv(PATHS["one_week"])

        required_cols = ["total_sales", "gross_sales", "quantity", "sub_category"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        total_revenue = df["total_sales"].sum()
        gross_revenue = df["gross_sales"].sum()
        total_orders  = len(df)
        aov           = total_revenue / total_orders if total_orders > 0 else 0

        aggregation = (
            df.groupby("sub_category")
            .agg(total_quantity=("quantity", "sum"), total_revenue=("total_sales", "sum"))
            .reset_index()
            .sort_values("total_revenue", ascending=False)
            .round(2)
        )

        results = {
            "total_revenue": round(total_revenue, 2),
            "gross_revenue": round(gross_revenue, 2),
            "total_orders":  total_orders,
            "aov":           round(aov, 2),
            # Save aggregation as a list of dicts (JSON-serialisable)
            "aggregation":   aggregation.to_dict(orient="records"),
        }

        with open(PATHS["week_results"], "w") as f:
            json.dump(results, f)

        logger.info(f"Week results saved → {PATHS['week_results']}")

    # --------------------------------------------------------------
    # Step 5 — One-month analysis
    # --------------------------------------------------------------
    def one_month_analysis(self, today: datetime) -> None:
        """
        Read /tmp/one_month.csv, calculate 4-week trends.
        Saves results → /tmp/one_month_results.json
        """
        logger.info("Running one-month analysis.")

        df    = pd.read_csv(PATHS["one_month"])
        today = pd.Timestamp(today).normalize()

        required_cols = ["week", "total_sales", "quantity"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        # Weekly revenue summary
        weekly_summary = (
            df.groupby("week")
            .agg(total_revenue=("total_sales", "sum"), total_quantity=("quantity", "sum"))
            .reset_index()
        )
        weekly_summary["week_number"] = weekly_summary["week"].str.extract(r"(\d+)").astype(int)
        weekly_summary["week_start"]  = (
            today - pd.to_timedelta((weekly_summary["week_number"] - 1) * 7, unit="D")
        )
        weekly_summary = weekly_summary.sort_values("week_start")
        weekly_summary["week_start"] = weekly_summary["week_start"].astype(str)  # JSON-safe

        # WoW growth
        weekly_rev = weekly_summary.set_index("week")["total_revenue"]
        wow_growth = None
        if {"week-1", "week-2"}.issubset(weekly_rev.index):
            prev = weekly_rev["week-2"]
            curr = weekly_rev["week-1"]
            if prev != 0:
                wow_growth = round((curr - prev) / prev, 4)

        # Weekly orders
        if "order_id" in df.columns:
            weekly_orders = (
                df.groupby("week")
                .agg(total_orders=("order_id", "nunique"))
                .reset_index()
            )
        else:
            weekly_orders = df.groupby("week").size().reset_index(name="total_orders")

        weekly_orders["week_number"] = weekly_orders["week"].str.extract(r"(\d+)").astype(int)
        weekly_orders["week_start"]  = (
            today - pd.to_timedelta((weekly_orders["week_number"] - 1) * 7, unit="D")
        )
        weekly_orders = weekly_orders.sort_values("week_start")
        weekly_orders["week_start"] = weekly_orders["week_start"].astype(str)  # JSON-safe

        results = {
            "weekly_summary": weekly_summary.round(2).to_dict(orient="records"),
            "weekly_orders":  weekly_orders.to_dict(orient="records"),
            "wow_growth":     wow_growth,
        }

        with open(PATHS["month_results"], "w") as f:
            json.dump(results, f)

        logger.info(f"Month results saved → {PATHS['month_results']}")

    # --------------------------------------------------------------
    # Step 6 — Generate PDF
    # --------------------------------------------------------------
    def generate_pdf(self, pdf_filename: str = "Weekly_Performance_Report.pdf") -> None:
        """
        Read results from /tmp/*.json, build charts and PDF.
        Saves file paths → /tmp/file_paths.json
        """
        logger.info("Generating PDF report.")

        with open(PATHS["week_results"],  "r") as f:
            week_results = json.load(f)
        with open(PATHS["month_results"], "r") as f:
            month_results = json.load(f)

        # Reconstruct DataFrames from JSON
        aggregation_df = pd.DataFrame(week_results["aggregation"])
        weekly_summary = pd.DataFrame(month_results["weekly_summary"])
        weekly_orders  = pd.DataFrame(month_results["weekly_orders"])

        total_revenue = week_results["total_revenue"]
        gross_revenue = week_results["gross_revenue"]
        aov           = week_results["aov"]
        wow_growth    = month_results.get("wow_growth")

        timestamp         = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir          = "/tmp"
        pdf_path          = os.path.join(base_dir, f"{timestamp}_{pdf_filename}")
        subcat_chart_path = os.path.join(base_dir, f"subcategory_chart_{timestamp}.png")
        rev_chart_path    = os.path.join(base_dir, f"revenue_trend_{timestamp}.png")
        orders_chart_path = os.path.join(base_dir, f"orders_trend_{timestamp}.png")

        # Charts
        plt.figure()
        plt.bar(aggregation_df["sub_category"], aggregation_df["total_revenue"])
        plt.xticks(rotation=45); plt.tight_layout()
        plt.savefig(subcat_chart_path); plt.close()

        plt.figure()
        plt.plot(weekly_summary["week_start"], weekly_summary["total_revenue"], marker="o")
        plt.xticks(rotation=45); plt.tight_layout()
        plt.savefig(rev_chart_path); plt.close()

        plt.figure()
        plt.plot(weekly_orders["week_start"], weekly_orders["total_orders"], marker="o")
        plt.xticks(rotation=45); plt.tight_layout()
        plt.savefig(orders_chart_path); plt.close()

        # PDF
        styles   = getSampleStyleSheet()
        elements = []
        doc      = SimpleDocTemplate(pdf_path, pagesize=pagesizes.A4)

        elements.append(Paragraph("Executive Weekly Performance Report", styles["Heading1"]))
        elements.append(Spacer(1, 0.3 * inch))

        elements.append(Paragraph("One Week KPI Summary", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))

        kpi_data = [
            ["Metric", "Value"],
            ["Total Revenue",  f"{total_revenue:,.2f}"],
            ["Gross Revenue",  f"{gross_revenue:,.2f}"],
            ["AOV",            f"{aov:,.2f}"],
            ["WoW Growth",     f"{wow_growth:.2%}" if wow_growth is not None else "N/A"],
        ]
        kpi_table = Table(kpi_data)
        kpi_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID",       (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN",      (1, 1), (-1, -1), "RIGHT"),
        ]))
        elements.append(kpi_table)
        elements.append(Spacer(1, 0.4 * inch))

        elements.append(Paragraph("Sub-Category Revenue (Last 7 Days)", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(subcat_chart_path, width=5*inch, height=3*inch))
        elements.append(Spacer(1, 0.4 * inch))

        elements.append(Paragraph("Sub-Category Performance (Last 7 Days)", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        agg_data = [["Sub Category", "Total Quantity", "Total Revenue"]]
        for _, row in aggregation_df.iterrows():
            agg_data.append([row["sub_category"], int(row["total_quantity"]), f"{row['total_revenue']:,.2f}"])
        agg_table = Table(agg_data)
        agg_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID",       (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN",      (1, 1), (-1, -1), "RIGHT"),
        ]))
        elements.append(agg_table)
        elements.append(Spacer(1, 0.4 * inch))

        elements.append(Paragraph("4-Week Revenue Trend", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(rev_chart_path, width=5*inch, height=3*inch))
        elements.append(Spacer(1, 0.4 * inch))

        elements.append(Paragraph("4-Week Orders Trend", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(orders_chart_path, width=5*inch, height=3*inch))

        doc.build(elements)
        logger.info(f"PDF saved → {pdf_path}")

        file_paths = {
            "pdf":               pdf_path,
            "subcategory_chart": subcat_chart_path,
            "revenue_chart":     rev_chart_path,
            "orders_chart":      orders_chart_path,
        }

        with open(PATHS["file_paths"], "w") as f:
            json.dump(file_paths, f)

        logger.info(f"File paths saved → {PATHS['file_paths']}")

    # --------------------------------------------------------------
    # Step 7 — Upload to S3
    # --------------------------------------------------------------
    def upload_to_s3(self) -> None:
        """Read file_paths.json and upload all files to S3."""
        import boto3  # ✅ boto3 only — no Airflow ORM initialization

        with open(PATHS["file_paths"], "r") as f:
            file_paths = json.load(f)

        s3 = boto3.client("s3")

        for name, path in file_paths.items():
            filename = os.path.basename(path)
            s3_key   = f"reports/{name}/{filename}"
            s3.upload_file(Filename=path, Bucket=S3_BUCKET, Key=s3_key)
            logger.info(f"Uploaded {name} → s3://{S3_BUCKET}/{s3_key}")


# ==================================================================
# CLI entry point — called by BashOperator
# ==================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amazon Report Analysis Pipeline")
    parser.add_argument(
        "--step",
        required=True,
        choices=["loading", "clean", "prepare", "week", "month", "pdf", "upload"],
        help="Pipeline step to execute"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date override in YYYY-MM-DD format (used in demo mode)"
    )
    args = parser.parse_args()

    # Resolve today
    if DEMO_MODE:
        today = FORCED_DATE
    elif args.date:
        today = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        today = datetime.today()

    report = ReportAnalysis()

    if args.step == "loading":
        report.loading()

    elif args.step == "clean":
        report.clean_data()

    elif args.step == "prepare":
        report.prepare_reporting_data(today)

    elif args.step == "week":
        report.one_week_analysis()

    elif args.step == "month":
        report.one_month_analysis(today)

    elif args.step == "pdf":
        report.generate_pdf()

    elif args.step == "upload":
        report.upload_to_s3()