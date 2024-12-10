import logging
import os
import traceback

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
import streamlit as st
import trino

log = logging.getLogger(__name__)


STARBURST_HOST = os.environ["STARBURST_HOST"]
STARBURST_PORT = os.environ["STARBURST_PORT"]
STARBURST_USER = os.environ["STARBURST_USER"]
STARBURST_PASSWORD = os.environ["STARBURST_PASSWORD"]
STARBURST_CATALOG = os.environ["STARBURST_CATALOG"]
STARBURST_HTTP_SCHEME = os.environ["STARBURST_HTTP_SCHEME"]

ENV = os.environ["ENV"]

SCHEMA = "analytics" if ENV == "prod" else "dbt_dev"


sql_query = f"""
        SELECT *
        FROM vinty.{SCHEMA}.inc_vsp__sold_products
    """


def query_trino(query: str):
    """
    Connects to Trino managed by Starburst with OAuth2 (API auth) and fetches the results of the SQL query.
    """
    log.info(f"Starting to execute {query}...")
    conn = trino.dbapi.connect(
        host=STARBURST_HOST,
        port=STARBURST_PORT,
        user=STARBURST_USER,
        http_scheme=STARBURST_HTTP_SCHEME,
        catalog=STARBURST_CATALOG,
        auth=trino.auth.BasicAuthentication(STARBURST_USER, STARBURST_PASSWORD),
    )
    cursor = conn.cursor()
    cursor.execute(query)

    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return pd.DataFrame(data, columns=column_names)


def vsp_page():
    st.set_page_config(
        page_title="VSP Handbag Analytics",
        page_icon="👜",
        layout="centered",
        initial_sidebar_state="expanded",
    )
    st.title("VSP Resale Handbag Analytics")
    try:
        df = query_trino(sql_query)
        df["price"] = df["price"].astype(int)
        df = df.drop(columns=["id", "store"])
        df["sold_date"] = pd.to_datetime(df["sold_date"])
        days_on_market_min = df["days_on_market"].min()
        days_on_market_max = df["days_on_market"].max()
        price_min = df["price"].min()
        price_max = df["price"].max()
        vendors = list(df["vendor"].unique())

        (
            col1a,
            col2a,
        ) = st.columns(2)

        if "vendors_list" not in st.session_state:
            st.session_state.vendors_list = ["All"] + list(df["vendor"].unique())

        if "selected_vendor" not in st.session_state:
            st.session_state.selected_vendor = "All"

        with col1a:
            vendor_selected = st.selectbox(
                "Select Vendor",
                st.session_state.vendors_list,
                index=st.session_state.vendors_list.index(
                    st.session_state.selected_vendor
                ),
            )

        with col2a:
            selected_dates = st.date_input(
                "Select Date Range",
                min_value=df["sold_date"].min(),
                max_value=df["sold_date"].max(),
                value=(df["sold_date"].min(), df["sold_date"].max()),
            )

            if len(selected_dates) > 1:
                start_date, end_date = selected_dates[0], selected_dates[1]
            else:
                start_date, end_date = selected_dates[0], selected_dates[0]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            price_min = st.number_input(
                "Min Price", min_value=price_min, max_value=price_max, value=price_min
            )

        with col2:
            price_max = st.number_input(
                "Max Price", min_value=price_min, max_value=price_max, value=price_max
            )

        with col3:
            days_on_market_min = st.number_input(
                "Min Days on Market",
                min_value=days_on_market_min,
                max_value=days_on_market_max,
                value=days_on_market_min,
            )

        with col4:
            days_on_market_max = st.number_input(
                "Max Days on Market",
                min_value=days_on_market_min,
                max_value=days_on_market_max,
                value=days_on_market_max,
            )
        col1b, col2b = st.columns(2)

        with col1b:
            title_search = st.text_input("Search by title (e.g., 'mama')", "")

        filtered_df = df[
            (df["price"] >= price_min)
            & (df["price"] <= price_max)
            & (df["days_on_market"] >= days_on_market_min)
            & (df["days_on_market"] <= days_on_market_max)
            & (df["sold_date"] >= pd.to_datetime(start_date))
            & (df["sold_date"] <= pd.to_datetime(end_date))
            & (df["title"].str.contains(title_search, case=False, na=False))
        ]

        if vendor_selected != "All":
            filtered_df = filtered_df[filtered_df["vendor"] == vendor_selected]

        st.write(filtered_df)
        st.text(f"Row count: {len(filtered_df)}")

        fig, ax = plt.subplots(figsize=(10, 5))
        sns.boxplot(x=filtered_df["price"], ax=ax, color="skyblue")

        q1, median, q3 = np.percentile(
            filtered_df["price"], [25, 50, 75]
        )  # Q1, Median (Q2), Q3
        iqr = q3 - q1
        box_plot_max = q3 + (1.5 * iqr)
        box_plot_min = q1 - (1.5 * iqr)
        box_plot_min = filtered_df["price"].min() if box_plot_min < 0 else box_plot_min

        ax.set_title("Price Distribution of Bags", fontsize=16)
        ax.set_xlabel("Price ($)", fontsize=14)
        ax.grid(axis="x", linestyle="--", alpha=0.7)

        st.pyplot(fig)
        st.text(
            f"Min: {box_plot_min}, Q1: {q1}, Median: {median}, Q3: {q3}, Max: {box_plot_max}, IQR: {iqr}"
        )

        fig, ax = plt.subplots(figsize=(10, 5))
        sns.boxplot(x=filtered_df["days_on_market"], ax=ax, color="skyblue")

        q1, median, q3 = np.percentile(filtered_df["days_on_market"], [25, 50, 75])
        iqr = q3 - q1
        box_plot_max = q3 + (1.5 * iqr)
        box_plot_min = q1 - (1.5 * iqr)
        box_plot_min = (
            filtered_df["days_on_market"].min() if box_plot_min < 0 else box_plot_min
        )

        ax.set_title("Distribution of Days on Market", fontsize=16)
        ax.set_xlabel("Days on Market (days)", fontsize=14)
        ax.grid(axis="x", linestyle="--", alpha=0.7)

        st.pyplot(fig)
        st.text(
            f"Min: {box_plot_min}, Q1: {q1}, Median: {median}, Q3: {q3}, Max: {box_plot_max}, IQR: {iqr}"
        )

        filtered_df["sold_date"] = pd.to_datetime(filtered_df["sold_date"]).dt.date

        st.title("Total Sales by Day of the Week")

        sales_per_day = filtered_df.copy()
        sales_per_day["sold_date"] = pd.to_datetime(sales_per_day["sold_date"])

        sales_per_day["day_of_week"] = sales_per_day["sold_date"].dt.day_name()

        sales_by_day = (
            sales_per_day.groupby("day_of_week").size().reset_index(name="sales_count")
        )

        days_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        sales_by_day = (
            sales_by_day.set_index("day_of_week")
            .reindex(days_order, fill_value=0)
            .reset_index()
        )

        fig, ax = plt.subplots(figsize=(10, 6))

        sns.barplot(
            data=sales_by_day,
            x="day_of_week",
            y="sales_count",
            palette="husl",
            ax=ax,
        )

        ax.set_title("Total Sales by Day of the Week", fontsize=16)
        ax.set_xlabel("Day of the Week", fontsize=14)
        ax.set_ylabel("Number of Sales", fontsize=14)
        ax.grid(axis="y", linestyle="--", alpha=0.6)

        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")

        fig.tight_layout()

        st.pyplot(fig)

        st.title("Total Sales Per Day")

        daily_sales = (
            filtered_df.groupby(["sold_date"]).size().reset_index(name="sales_count")
        )

        fig, ax = plt.subplots(figsize=(12, 6))

        sns.lineplot(
            data=daily_sales,
            x="sold_date",
            y="sales_count",
            marker="o",
            ax=ax,
        )

        ax.set_title("Number of Sales Per Day", fontsize=16)
        ax.set_xlabel("Date", fontsize=14)
        ax.set_ylabel("Number of Sales", fontsize=14)
        ax.grid(True, linestyle="--", alpha=0.6)

        x_labels = [
            f"{date}\n({pd.to_datetime(date).strftime('%A')})"
            for date in daily_sales["sold_date"]
        ]
        ax.set_xticks(daily_sales["sold_date"])
        ax.set_xticklabels(x_labels, rotation=45, ha="right")

        fig.tight_layout()

        st.pyplot(fig)

        sales_per_day = (
            filtered_df.groupby(["sold_date", "vendor"])
            .size()
            .reset_index(name="sales_count")
        )

        st.title("Sales Per Day Per Vendor")

        fig, ax = plt.subplots(figsize=(12, 6))

        vendor_sales = (
            sales_per_day.groupby("vendor")["sales_count"].sum().reset_index()
        )
        vendor_sales = vendor_sales.sort_values(by="sales_count", ascending=False)

        sns.lineplot(
            data=sales_per_day,
            x="sold_date",
            y="sales_count",
            hue="vendor",
            hue_order=vendor_sales["vendor"],
            marker="o",  # Add markers for better visibility
            ax=ax,
        )

        ax.set_title("Number of Sales Per Day Per Vendor", fontsize=16)
        ax.set_xlabel("Date", fontsize=14)
        ax.set_ylabel("Number of Sales", fontsize=14)
        ax.legend(title="Vendor", loc="upper left", bbox_to_anchor=(1.05, 1))
        ax.grid(True, linestyle="--", alpha=0.6)

        x_labels = [
            f"{date}\n({pd.to_datetime(date).strftime('%A')})"
            for date in sales_per_day["sold_date"].unique()
        ]
        ax.set_xticks(sales_per_day["sold_date"].unique())
        ax.set_xticklabels(x_labels, rotation=45, ha="right")

        fig.tight_layout()

        st.pyplot(fig)

        revenue_per_day = (
            filtered_df.groupby(df["sold_date"].dt.date)["price"].sum().reset_index()
        )
        revenue_per_day.columns = ["Date", "Total Revenue"]

        def currency_formatter(x, _):
            if x >= 1_000_000:
                return f"${x / 1e6:.1f}M"
            elif x >= 1_000:
                return f"${x / 1e3:.0f}K"
            else:
                return f"${x:.0f}"

        st.title("Revenue Per Day")
        st.dataframe(revenue_per_day)

        fig, ax = plt.subplots(figsize=(10, 6))
        sns.lineplot(
            data=revenue_per_day, x="Date", y="Total Revenue", marker="o", ax=ax
        )

        ax.yaxis.set_major_formatter(mticker.FuncFormatter(currency_formatter))

        ax.set_title("Total Revenue Per Day", fontsize=16)
        ax.set_xlabel("Date", fontsize=14)
        ax.set_ylabel("Total Revenue ($)", fontsize=14)
        ax.grid(True, linestyle="--", alpha=0.6)

        plt.xticks(rotation=45)
        st.pyplot(fig)

        st.title("Products Sold by Vendor")

        vendor_sales = filtered_df["vendor"].value_counts().reset_index()
        vendor_sales.columns = ["vendor", "products_sold"]

        vendor_sales_sorted = vendor_sales.sort_values(
            by="products_sold", ascending=False
        )

        plt.figure(figsize=(10, 10))
        sns.barplot(
            x="products_sold", y="vendor", data=vendor_sales_sorted, palette="Set1"
        )

        plt.title("Products Sold by Vendor")
        plt.xlabel("Number of Products Sold")
        plt.ylabel("Vendor")
        plt.grid(True, axis="x")

        st.pyplot(plt)

        csv = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Filtered Data as CSV",
            csv,
            "filtered_results.csv",
            "text/csv",
        )

    except Exception as e:
        log.error(
            f"An error occurred while fetching data: {e}, {traceback.format_exc()}"
        )


if __name__ == "__main__":
    vsp_page()
