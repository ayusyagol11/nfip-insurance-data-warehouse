"""Generate professional matplotlib charts from the NFIP Gold analytics views.

Usage:
    python scripts/visualize/generate_charts.py

Prerequisites:
    - Docker container running: docker-compose up -d
    - Full pipeline executed: ./scripts/run_all_sql.sh
    - pip install -r requirements.txt

Outputs to Images/charts/:
    chart_loss_ratio_heatmap.png
    chart_claims_development.png
    chart_large_loss_concentration.png
    chart_severity_by_zone.png
    chart_portfolio_summary.png
    chart_frequency_severity.png
    chart_premium_adequacy.png
"""

import os
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# --- Config ------------------------------------------------------------------

ODBC_DRIVER = None
for _driver in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
    if _driver in pyodbc.drivers():
        ODBC_DRIVER = _driver
        break

if ODBC_DRIVER is None:
    raise RuntimeError(
        "No suitable ODBC driver found. Install ODBC Driver 17 or 18 for SQL Server."
    )

CONNECTION_STRING = (
    f"Driver={{{ODBC_DRIVER}}};"
    "Server=localhost,1433;"
    "Database=NfipInsuranceWarehouse;"
    "UID=sa;"
    "PWD=NfipWarehouse2026!;"
    "TrustServerCertificate=yes"
)

_HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.normpath(os.path.join(_HERE, "..", "..", "Images", "charts"))

DPI = 150
BLUE   = "#2171b5"
ORANGE = "#e6550d"
GREEN  = "#31a354"
RED    = "#de2d26"

# --- Helpers -----------------------------------------------------------------

def get_connection():
    return pyodbc.connect(CONNECTION_STRING)

def fetch(conn, sql):
    return pd.read_sql(sql, conn)

def save(fig, name, out_dir):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")

# --- Chart 1: Loss Ratio Heatmap (vw_loss_ratio_by_state) --------------------

def chart_loss_ratio_heatmap(conn, out_dir):
    df = fetch(conn, """
        SELECT state_name, year, loss_ratio
        FROM gold.vw_loss_ratio_by_state
        WHERE year BETWEEN 2009 AND 2024
          AND total_premium > 0
          AND loss_ratio IS NOT NULL
    """)
    pivot = df.pivot_table(index="state_name", columns="year", values="loss_ratio")

    fig, ax = plt.subplots(figsize=(14, 4))
    sns.heatmap(
        pivot, annot=True, fmt=".2f",
        cmap="RdYlGn_r", center=1.0, vmin=0, vmax=3,
        linewidths=0.5, linecolor="#cccccc",
        ax=ax, cbar_kws={"label": "Loss Ratio"}
    )
    ax.set_title(
        "Loss Ratio by State and Year (2009–2024)\n"
        "Red = claims exceed premiums  |  Green = profitable",
        pad=12, fontsize=13
    )
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.tick_params(axis="x", rotation=45)
    save(fig, "chart_loss_ratio_heatmap.png", out_dir)

# --- Chart 2: Claims Development (vw_claims_development) ---------------------

def chart_claims_development(conn, out_dir):
    df = fetch(conn, """
        SELECT year_of_loss, claim_count, total_paid, avg_paid_per_claim
        FROM gold.vw_claims_development
        WHERE year_of_loss BETWEEN 1978 AND 2024
        ORDER BY year_of_loss
    """)
    df["total_paid_B"] = df["total_paid"] / 1e9

    fig, ax1 = plt.subplots(figsize=(14, 5))
    ax1.bar(df["year_of_loss"], df["total_paid_B"], color=BLUE, alpha=0.8, label="Total Paid ($B)")
    ax1.set_ylabel("Total Paid ($B)", color=BLUE)
    ax1.tick_params(axis="y", labelcolor=BLUE)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.1f}B"))

    ax2 = ax1.twinx()
    ax2.plot(
        df["year_of_loss"], df["avg_paid_per_claim"] / 1000,
        color=ORANGE, linewidth=2, label="Avg Severity ($K)"
    )
    ax2.set_ylabel("Avg Severity ($K)", color=ORANGE)
    ax2.tick_params(axis="y", labelcolor=ORANGE)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.0f}K"))

    for yr, label in [(2005, "Katrina\n2005"), (2017, "Harvey\n2017")]:
        row = df[df["year_of_loss"] == yr]
        if not row.empty:
            y_val = row["total_paid_B"].values[0]
            ax1.annotate(
                label,
                xy=(yr, y_val),
                xytext=(yr + 1, y_val + 0.8),
                ha="left", fontsize=8, color="#333333",
                arrowprops=dict(arrowstyle="->", color="#888888", lw=0.8)
            )

    ax1.set_title("Claims Development by Accident Year (1978–2024)", pad=12, fontsize=13)
    ax1.set_xlabel("Year of Loss")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    save(fig, "chart_claims_development.png", out_dir)

# --- Chart 3: Large Loss Concentration (vw_large_loss_concentration) ---------

def chart_large_loss_concentration(conn, out_dir):
    df = fetch(conn, """
        SELECT state_name, pct_of_total_paid, large_loss_count, large_loss_total
        FROM gold.vw_large_loss_concentration
        ORDER BY pct_of_total_paid DESC
    """)

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(df["state_name"], df["pct_of_total_paid"], color=BLUE, alpha=0.85)
    for bar, val in zip(bars, df["pct_of_total_paid"]):
        ax.text(
            bar.get_width() + 0.4,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}%", va="center", fontsize=10
        )
    ax.set_xlabel("% of Total Claims Paid from P95+ Losses")
    ax.set_title(
        "Large Loss Concentration by State\n"
        "Share of total paid from catastrophic claims  |  P95 threshold: $185,607",
        pad=12, fontsize=13
    )
    ax.set_xlim(0, df["pct_of_total_paid"].max() * 1.18)
    ax.invert_yaxis()
    save(fig, "chart_large_loss_concentration.png", out_dir)

# --- Chart 4: Severity by Flood Zone (vw_severity_by_flood_zone) -------------

def chart_severity_by_zone(conn, out_dir):
    df = fetch(conn, """
        SELECT zone_category, avg_severity, avg_building_paid, avg_contents_paid
        FROM gold.vw_severity_by_flood_zone
        ORDER BY avg_severity DESC
    """)

    x = list(range(len(df)))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar([i - width / 2 for i in x], df["avg_building_paid"], width,
           label="Avg Building Paid", color=BLUE, alpha=0.85)
    ax.bar([i + width / 2 for i in x], df["avg_contents_paid"], width,
           label="Avg Contents Paid", color=ORANGE, alpha=0.85)
    ax.plot(x, df["avg_severity"], "D-", color="#333333",
            linewidth=2, markersize=7, label="Avg Total Severity", zorder=5)

    ax.set_xticks(x)
    ax.set_xticklabels(df["zone_category"])
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_ylabel("Average Paid per Claim ($)")
    ax.set_title(
        "Average Claim Severity by Flood Zone Category\nBuilding vs. contents split",
        pad=12, fontsize=13
    )
    ax.legend()
    save(fig, "chart_severity_by_zone.png", out_dir)

# --- Chart 5: Portfolio Summary (vw_portfolio_summary) -----------------------

def chart_portfolio_summary(conn, out_dir):
    df = fetch(conn, """
        SELECT year, total_claims_paid, total_premium, loss_ratio, avg_severity, claim_count
        FROM gold.vw_portfolio_summary
        WHERE year BETWEEN 2009 AND 2024
          AND total_premium > 0
          AND loss_ratio IS NOT NULL
        ORDER BY year
    """)
    df["total_claims_paid_B"] = df["total_claims_paid"] / 1e9

    fig, ax1 = plt.subplots(figsize=(13, 5))
    ax1.bar(df["year"], df["total_claims_paid_B"], color=BLUE, alpha=0.7, label="Total Claims Paid ($B)")
    ax1.set_ylabel("Total Claims Paid ($B)", color=BLUE)
    ax1.tick_params(axis="y", labelcolor=BLUE)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:.1f}B"))

    ax2 = ax1.twinx()
    ax2.plot(df["year"], df["loss_ratio"], color=RED, linewidth=2.5,
             marker="o", markersize=5, label="Loss Ratio")
    ax2.axhline(1.0, color=RED, linestyle="--", linewidth=1, alpha=0.5)
    ax2.set_ylabel("Loss Ratio", color=RED)
    ax2.tick_params(axis="y", labelcolor=RED)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}x"))

    ax1.set_title(
        "Portfolio Loss Ratio and Claims Paid (2009–2024)\n"
        "Dashed line = loss ratio of 1.0 (breakeven)",
        pad=12, fontsize=13
    )
    ax1.set_xlabel("Year")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    save(fig, "chart_portfolio_summary.png", out_dir)

# --- Chart 6: Frequency vs Severity Scatter (vw_claims_frequency_severity) --

def chart_frequency_severity(conn, out_dir):
    df = fetch(conn, """
        SELECT
            zone_category,
            SUM(claim_count)  AS total_claims,
            SUM(total_exposure) AS total_exposure,
            SUM(total_paid) / NULLIF(SUM(claim_count), 0) AS avg_severity
        FROM gold.vw_claims_frequency_severity
        WHERE total_exposure > 0
        GROUP BY zone_category
    """)
    df["frequency"] = df["total_claims"] / df["total_exposure"]
    df = df.dropna(subset=["frequency", "avg_severity"])

    fig, ax = plt.subplots(figsize=(9, 6))
    colors = [BLUE, ORANGE, GREEN, RED, "#756bb1", "#636363"]
    for i, (_, row) in enumerate(df.iterrows()):
        ax.scatter(
            row["frequency"], row["avg_severity"] / 1000,
            s=row["total_claims"] / df["total_claims"].max() * 3000,
            color=colors[i % len(colors)], alpha=0.75,
            edgecolors="white", linewidths=1.5
        )
        ax.annotate(
            row["zone_category"],
            xy=(row["frequency"], row["avg_severity"] / 1000),
            xytext=(8, 4), textcoords="offset points",
            fontsize=10, fontweight="bold"
        )

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.3f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}K"))
    ax.set_xlabel("Claims Frequency (claims per unit of earned exposure)")
    ax.set_ylabel("Average Severity ($K per claim)")
    ax.set_title(
        "Claims Frequency vs. Severity by Flood Zone\nBubble size proportional to total claim count",
        pad=12, fontsize=13
    )
    save(fig, "chart_frequency_severity.png", out_dir)

# --- Chart 7: Premium Adequacy (vw_premium_adequacy) ------------------------

def chart_premium_adequacy(conn, out_dir):
    df = fetch(conn, """
        SELECT
            occupancy_type,
            SUM(total_claims)   AS total_claims,
            SUM(total_exposure) AS total_exposure,
            SUM(avg_premium * policy_count) / NULLIF(SUM(policy_count), 0) AS weighted_avg_premium
        FROM gold.vw_premium_adequacy
        WHERE total_exposure > 0 AND avg_premium IS NOT NULL
        GROUP BY occupancy_type
    """)
    df["pure_premium"] = df["total_claims"] / df["total_exposure"]
    df = df.dropna(subset=["pure_premium", "weighted_avg_premium"])
    df = df.sort_values("pure_premium", ascending=False).head(10)

    x = list(range(len(df)))
    width = 0.35
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar([i - width / 2 for i in x], df["pure_premium"], width,
           label="Pure Premium (losses ÷ exposure)", color=RED, alpha=0.8)
    ax.bar([i + width / 2 for i in x], df["weighted_avg_premium"], width,
           label="Avg Premium Charged", color=GREEN, alpha=0.8)

    ax.set_xticks(x)
    ax.set_xticklabels(df["occupancy_type"], rotation=35, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_ylabel("Amount ($)")
    ax.set_title(
        "Premium Adequacy by Occupancy Type\nRed = pure premium required  |  Green = avg premium charged",
        pad=12, fontsize=13
    )
    ax.legend()
    save(fig, "chart_premium_adequacy.png", out_dir)

# --- Main --------------------------------------------------------------------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Output directory: {OUT_DIR}")

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("seaborn-whitegrid")

    print("Connecting to NfipInsuranceWarehouse...")
    conn = get_connection()
    print("Connected. Generating 7 charts...\n")

    chart_loss_ratio_heatmap(conn, OUT_DIR)
    chart_claims_development(conn, OUT_DIR)
    chart_large_loss_concentration(conn, OUT_DIR)
    chart_severity_by_zone(conn, OUT_DIR)
    chart_portfolio_summary(conn, OUT_DIR)
    chart_frequency_severity(conn, OUT_DIR)
    chart_premium_adequacy(conn, OUT_DIR)

    conn.close()
    print("\nAll 7 charts generated successfully.")

if __name__ == "__main__":
    main()
