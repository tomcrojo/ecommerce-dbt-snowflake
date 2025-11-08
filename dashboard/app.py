#!/usr/bin/env python3
"""E-Commerce Analytics Dashboard — Streamlit + Plotly.

Reads processed CSV data (or simulates it if unavailable) and renders
interactive charts for revenue, orders, products, and customer segments.

Usage:
    streamlit run dashboard/app.py
"""

import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
SCRIPTS_DIR = BASE_DIR / "scripts"

SPAIN_REGIONS = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Sevilla",
    "Zaragoza",
    "Málaga",
    "Bilbao",
    "Murcia",
    "Alicante",
    "Córdoba",
    "Granada",
    "Valladolid",
    "Vitoria",
    "Palma de Mallorca",
    "Las Palmas",
]

CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]

# ---------------------------------------------------------------------------
# Data loading / simulation
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def load_or_simulate_data() -> pd.DataFrame:
    """Try loading from CSV; fall back to generated simulated data."""
    orders_path = DATA_DIR / "orders_enriched.csv"
    if orders_path.exists():
        df = pd.read_csv(orders_path, parse_dates=["order_date"])
        return df
    return _simulate_data()


def _simulate_data() -> pd.DataFrame:
    """Generate realistic simulated data when no CSV is available."""
    np.random.seed(42)
    n = 50_000

    start = date(2023, 1, 1)
    end = date(2026, 3, 21)
    date_range = (end - start).days

    dates = [
        start + timedelta(days=int(d)) for d in np.random.randint(0, date_range, n)
    ]
    regions = np.random.choice(SPAIN_REGIONS, n, p=_region_weights())
    statuses = np.random.choice(
        ["completed", "pending", "cancelled", "refunded"], n, p=[0.65, 0.18, 0.10, 0.07]
    )
    categories = np.random.choice(CATEGORIES, n, p=[0.30, 0.25, 0.18, 0.15, 0.12])
    customers = np.random.randint(1, 10_001, n)

    # Revenue per order: log-normal distribution centred around 50 EUR
    revenue = np.random.lognormal(mean=3.9, sigma=0.8, size=n).clip(5, 2000).round(2)

    # Customer segments
    segments = np.random.choice(
        ["new", "returning", "churned"], n, p=[0.25, 0.55, 0.20]
    )

    df = pd.DataFrame(
        {
            "order_date": dates,
            "region": regions,
            "status": statuses,
            "category": categories,
            "customer_id": customers,
            "total_amount": revenue,
            "customer_segment": segments,
        }
    )
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


def _region_weights() -> np.ndarray:
    """Heavier weight on Madrid + Barcelona."""
    base = np.array([1.0] * len(SPAIN_REGIONS))
    base[0] = 3.0  # Madrid
    base[1] = 2.5  # Barcelona
    base[2] = 1.8  # Valencia
    return base / base.sum()


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------


def kpi_card(label: str, value: str, delta: str | None = None):
    st.metric(label, value, delta=delta)


def chart_revenue_over_time(df: pd.DataFrame) -> go.Figure:
    daily = (
        df.groupby(df["order_date"].dt.to_period("M").astype(str))["total_amount"]
        .sum()
        .reset_index()
    )
    daily.columns = ["month", "revenue"]
    fig = px.line(daily, x="month", y="revenue", title="Revenue Over Time (Monthly)")
    fig.update_layout(
        xaxis_title="Month", yaxis_title="Revenue (€)", template="plotly_white"
    )
    fig.update_traces(line_color="#FF6B6B", line_width=2.5)
    return fig


def chart_orders_by_region(df: pd.DataFrame) -> go.Figure:
    region_counts = df["region"].value_counts().reset_index()
    region_counts.columns = ["region", "orders"]
    fig = px.bar(
        region_counts,
        x="region",
        y="orders",
        title="Orders by Region",
        color="orders",
        color_continuous_scale="Tealgrn",
    )
    fig.update_layout(
        xaxis_title="Region",
        yaxis_title="Number of Orders",
        template="plotly_white",
        showlegend=False,
    )
    return fig


def chart_top_products(df: pd.DataFrame) -> go.Figure:
    if "category" in df.columns:
        cat_rev = (
            df.groupby("category")["total_amount"]
            .sum()
            .sort_values(ascending=True)
            .tail(10)
        )
        fig = px.bar(
            x=cat_rev.values,
            y=cat_rev.index,
            orientation="h",
            title="Top Categories by Revenue",
            labels={"x": "Revenue (€)", "y": "Category"},
            color=cat_rev.values,
            color_continuous_scale="Sunset",
        )
    else:
        fig = go.Figure()
        fig.update_layout(title="Top Products (no category data)")
    fig.update_layout(template="plotly_white", showlegend=False)
    return fig


def chart_customer_segments(df: pd.DataFrame) -> go.Figure:
    if "customer_segment" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="Customer Segments (no data)")
        return fig
    seg = df["customer_segment"].value_counts().reset_index()
    seg.columns = ["segment", "count"]
    fig = px.pie(
        seg, names="segment", values="count", title="Customer Segment Breakdown"
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(template="plotly_white")
    return fig


def chart_order_status(df: pd.DataFrame) -> go.Figure:
    status = df["status"].value_counts().reset_index()
    status.columns = ["status", "count"]
    colors = {
        "completed": "#2ecc71",
        "pending": "#f39c12",
        "cancelled": "#e74c3c",
        "refunded": "#9b59b6",
    }
    fig = go.Figure(
        data=[
            go.Pie(
                labels=status["status"],
                values=status["count"],
                hole=0.5,
                marker=dict(
                    colors=[colors.get(s, "#95a5a6") for s in status["status"]]
                ),
                textinfo="percent+label",
            )
        ]
    )
    fig.update_layout(title="Order Status Distribution", template="plotly_white")
    return fig


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="E-Commerce Analytics Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("E-Commerce Analytics Dashboard")
    st.caption("Madrid-based e-commerce — powered by dbt + Snowflake")

    tab1, tab2 = st.tabs(["Business Metrics", "Data Quality"])

    # Sidebar
    st.sidebar.header("Filters")

    df = load_or_simulate_data()

    # Date range
    min_date = df["order_date"].min().date()
    max_date = df["order_date"].max().date()
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Region filter
    all_regions = sorted(df["region"].unique().tolist())
    selected_regions = st.sidebar.multiselect(
        "Regions", all_regions, default=all_regions
    )

    # Refresh button
    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # Run batch ingestor button
    if st.sidebar.button("Run Batch Ingestor (1 batch)"):
        with st.spinner("Running batch ingestor …"):
            try:
                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPTS_DIR / "batch_ingestor.py"),
                        "--count",
                        "1",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if result.returncode == 0:
                    st.sidebar.success("Batch ingested successfully!")
                else:
                    st.sidebar.error(f"Ingestor failed:\n{result.stderr[:500]}")
            except Exception as e:
                st.sidebar.error(f"Error: {e}")

    # Apply filters
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
    else:
        start_d, end_d = min_date, max_date

    mask = (df["order_date"].dt.date >= start_d) & (df["order_date"].dt.date <= end_d)
    mask &= df["region"].isin(selected_regions)
    filtered = df.loc[mask]

    with tab1:
        # KPIs
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)

        total_revenue = filtered["total_amount"].sum()
        total_orders = len(filtered)
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        active_customers = filtered["customer_id"].nunique()

        with col1:
            kpi_card("Total Revenue", f"€{total_revenue:,.2f}")
        with col2:
            kpi_card("Total Orders", f"{total_orders:,}")
        with col3:
            kpi_card("Avg Order Value", f"€{avg_order_value:,.2f}")
        with col4:
            kpi_card("Active Customers", f"{active_customers:,}")

        st.markdown("---")

        # Charts — 2-column layout
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            st.plotly_chart(chart_revenue_over_time(filtered), use_container_width=True)
        with row1_col2:
            st.plotly_chart(chart_orders_by_region(filtered), use_container_width=True)

        row2_col1, row2_col2 = st.columns(2)
        with row2_col1:
            st.plotly_chart(chart_top_products(filtered), use_container_width=True)
        with row2_col2:
            st.plotly_chart(chart_customer_segments(filtered), use_container_width=True)

        st.plotly_chart(chart_order_status(filtered), use_container_width=True)

        # Footer
        st.markdown("---")
        st.caption(
            "Data source: DummyJSON API (batch ingestion) · "
            "Transformed with dbt Core on Snowflake · "
            "Visualized with Streamlit + Plotly"
        )

    with tab2:
        st.header("Data Quality Monitor")

        # --- dbt Test Results ---
        st.subheader("dbt Test Results")
        run_results_path = BASE_DIR / "target" / "run_results.json"
        if run_results_path.exists():
            with open(run_results_path) as f:
                results = json.load(f)
            results_df = pd.DataFrame(results.get("results", []))
            if not results_df.empty:
                passed = len(results_df[results_df["status"] == "pass"])
                failed = len(results_df[results_df["status"] == "fail"])
                total = len(results_df)

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Tests", total)
                col2.metric("Passed", passed, delta=f"{passed / total * 100:.0f}%")
                col3.metric("Failed", failed, delta_color="inverse")

                if failed > 0:
                    st.error("Failed tests:")
                    failed_df = results_df[results_df["status"] == "fail"][
                        ["unique_id", "status", "message"]
                    ]
                    st.dataframe(failed_df, hide_index=True)
                else:
                    st.success("All tests passed!")
        else:
            st.info("No dbt test results found. Run `dbt test` first.")

        # --- Dataset Volume ---
        st.subheader("Dataset Volume")
        seeds_dir = BASE_DIR / "seeds"
        if seeds_dir.exists():
            volumes = []
            for csv in sorted(seeds_dir.glob("*.csv")):
                try:
                    csv_df = pd.read_csv(csv)
                    volumes.append(
                        {
                            "Dataset": csv.stem.replace("raw_", "")
                            .replace("_", " ")
                            .title(),
                            "Rows": len(csv_df),
                            "Columns": len(csv_df.columns),
                        }
                    )
                except Exception:
                    continue
            if volumes:
                vol_df = pd.DataFrame(volumes)
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(vol_df, hide_index=True)
                with col2:
                    fig = px.bar(
                        vol_df,
                        x="Dataset",
                        y="Rows",
                        title="Row Counts by Dataset",
                    )
                    st.plotly_chart(fig, use_container_width=True)

        # --- Null Rate Analysis ---
        st.subheader("Null Rate Analysis")
        if seeds_dir.exists():
            for csv in sorted(seeds_dir.glob("*.csv")):
                try:
                    csv_df = pd.read_csv(csv)
                    null_rates = csv_df.isnull().mean() * 100
                    null_rates = null_rates[null_rates > 0].sort_values(ascending=False)
                    if not null_rates.empty:
                        st.write(f"**{csv.stem}**")
                        nr_df = pd.DataFrame(
                            {
                                "column": null_rates.index,
                                "null_rate_pct": null_rates.values.round(2),
                            }
                        )
                        fig = px.bar(
                            nr_df,
                            x="column",
                            y="null_rate_pct",
                            title=f"Null Rates: {csv.stem}",
                        )
                        fig.update_layout(yaxis_title="Null Rate %")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.success(f"**{csv.stem}** — No nulls detected")
                except Exception:
                    continue

        # --- Referential Integrity ---
        st.subheader("Referential Integrity")
        try:
            orders = pd.read_csv(seeds_dir / "raw_orders.csv")
            customers = pd.read_csv(seeds_dir / "raw_customers.csv")
            products = pd.read_csv(seeds_dir / "raw_products.csv")
            items = pd.read_csv(seeds_dir / "raw_order_items.csv")

            orphaned_orders = orders[
                ~orders["customer_id"].isin(customers["customer_id"])
            ]
            orphaned_items = items[~items["order_id"].isin(orders["order_id"])]
            orphaned_products = items[~items["product_id"].isin(products["product_id"])]

            integrity_data = [
                {
                    "Check": "Orders → Customers",
                    "Orphaned": len(orphaned_orders),
                    "Status": "OK" if len(orphaned_orders) == 0 else "ISSUE",
                },
                {
                    "Check": "Items → Orders",
                    "Orphaned": len(orphaned_items),
                    "Status": "OK" if len(orphaned_items) == 0 else "ISSUE",
                },
                {
                    "Check": "Items → Products",
                    "Orphaned": len(orphaned_products),
                    "Status": "OK" if len(orphaned_products) == 0 else "ISSUE",
                },
            ]
            st.dataframe(pd.DataFrame(integrity_data), hide_index=True)
        except Exception as e:
            st.info(f"Could not check referential integrity: {e}")


if __name__ == "__main__":
    main()
