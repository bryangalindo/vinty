import logging
import os

import pandas as pd
import streamlit as st
import trino

log = logging.getLogger(__name__)

ENV = os.environ["ENV"]

if ENV == "dev":
    import dotenv

    dotenv.load_dotenv("./.env")

SCHEMA = "analytics" if ENV == "prod" else "dbt_dev"

STARBURST_HOST = os.environ["STARBURST_HOST"]
STARBURST_PORT = os.environ["STARBURST_PORT"]
STARBURST_USER = os.environ["STARBURST_USER"]
STARBURST_PASSWORD = os.environ["STARBURST_PASSWORD"]
STARBURST_CATALOG = os.environ["STARBURST_CATALOG"]
STARBURST_HTTP_SCHEME = os.environ["STARBURST_HTTP_SCHEME"]

ENV = os.environ["ENV"]

SCHEMA = "analytics" if ENV == "prod" else "dbt_dev"


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


def main():
    st.set_page_config(
        page_title="Big Bag Data",
        page_icon="👜",
        layout="centered",
        initial_sidebar_state="expanded",
    )

    st.title("Big Bag Data: Data-Driven Decisions for Luxury Resale")

    st.markdown(
        """
    ## 👋 Introduction
    My name is **Bryan Galindo**. I'm a ex-Software Engineer at Bank of America. For the capstone project, I've partnered with a small business called [Vinty](https://getvinty.com), which specializes in reselling second-hand luxury handbags. 

    After a couple of conversations with the business owner, here's the business problem I'm trying to solve.
    """
    )

    # Problem Statement
    st.header("🔍 Problem Statement")
    st.markdown(
        """
    **Vinty** wants to **identify current trends in the second-hand luxury handbag resale market** to help guide their procurement department on which bags to purchase to **minimize their average product time to sell** and ultimately **increase their sales volume**. 

    To identify the trends, Vinty needs at least **daily snapshots** of all the handbags that are for sale on the market, along with prior sold handbag data (if available).
    """
    )

    # Conceptual Data Model
    st.header("🧠 Conceptual Data Model")
    st.markdown(
        """
    I've created the conceptual data model below, which includes the data sources, the ingestion process + any challenges associated with them, and the KPIs we need to extract from the data models. 

    Due to time constraints, the initial build will only focus on two data sources: **Rebag** and **VSP**.
    """
    )
    st.image(
        "https://i.imgur.com/PrvhRoC.png",
        caption="Conceptual Data Model",
        use_container_width=True,
    )

    # Technologies Section
    st.header("📟 Technologies")
    st.markdown(
        """
    I will be working with the following technologies. Due to time constraints, I may have to build an initial version of the pipeline using only the core technologies below.

    ### Core Technologies
    - **Python**
    - **Apache Airflow** (managed by Astronomer)
    - **Apache Iceberg**
    - **Apache Spark**
    - **Amazon S3**
    - **dbt**
    - **Trino** (managed by Starburst)
    - **Tableau**
    """
    )

    # System Design
    st.header("🖥️ System Design")
    st.markdown(
        """
    This is the ideal system design for the capstone, although I may have to further simplify this design due to time constraints. The primary goal should be to get an initial build out on time, rather than worry about using the best tech and optimizations.
    """
    )
    st.image(
        "https://i.imgur.com/i4PU4dy.png",
        caption="System Design",
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
