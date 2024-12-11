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

    st.title("📈 Big Bag Data: Data-Driven Insights for Luxury Resale")
    body_text_1 = (
        "My name is Bryan Galindo 👋. I'm a former Software Engineer at Bank of America. "
        "Before venturing into my next role, I took time off to join Zach Wilson's 5-week Analytics Engineering bootcamp to learn about the modern data stack. "
    )
    body_text_2 = (
        "During this brief period, I had the opportunity to work with a small business called Vinty, a secondhand luxury handbag resale company, "
        "where I built an end-to-end, fully automated analytics platform that provided insights into the products being sold in the market."
    )
    body_text_3 = (
        "As a result of the analytics platform, Vinty was able to improve its decision-making in purchase strategies "
        "+ pricing strategies, discover how its store’s performance aligns with the industry, "
        "and establish benchmarks for its store’s performance."
    )
    body_text_4 = "Feel free to see the slides below for more details or explore the rest website to see the full story behind this project!"
    st.text(body_text_1)
    st.text(body_text_2)
    st.text(body_text_3)
    st.text(body_text_4)

    st.markdown(
        """
    <div style="position: relative; width: 100%; height: 0; padding-top: 56.2500%;
     padding-bottom: 0; box-shadow: 0 2px 8px 0 rgba(63,69,81,0.16); margin-top: 1.6em; margin-bottom: 0.9em; overflow: hidden;
     border-radius: 8px; will-change: transform;">
      <iframe loading="lazy" style="position: absolute; width: 100%; height: 100%; top: 0; left: 0; border: none; padding: 0;margin: 0;"
        src="https://www.canva.com/design/DAGY0jztmXk/xtqDdOjjCnesrV-xemr1LQ/view?embed" allowfullscreen="allowfullscreen" allow="fullscreen">
      </iframe>
    </div>
    <a href="https:&#x2F;&#x2F;www.canva.com&#x2F;design&#x2F;DAGY0jztmXk&#x2F;xtqDdOjjCnesrV-xemr1LQ&#x2F;view?utm_content=DAGY0jztmXk&amp;utm_campaign=designshare&amp;utm_medium=embeds&amp;utm_source=link" target="_blank" rel="noopener">Vintage Handbag Analytics</a> by Bryan Galindo
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
