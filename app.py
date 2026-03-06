import streamlit as st
import pandas as pd
import time
from config import AppConfig
from core import DatabaseManager
from core import SheetIngestor

config = AppConfig()
db = DatabaseManager(config.db_path)
sheet_ingestor = SheetIngestor(config, db)

st.set_page_config(page_title="TrendLens Pipeline",
                   page_icon="🎣", layout="centered")

st.title("🎣 TrendLens Pipeline")
st.markdown(
    "Manage your ETL pipeline: Fetch creators, ingest Apify data, and extract AI hooks.")

# We use session state to remember data between button clicks (so the screen doesn't clear)
if 'scrape_list' not in st.session_state:
    st.session_state['scrape_list'] = ""

# Workflow tabs
tab1, tab2, tab3 = st.tabs([
    "1️⃣ Fetch & Scrape",
    "2️⃣ Ingest Data",
    "3️⃣ Run AI Insights"
])


# TAB 1: FETCH FROM SHEETS
with tab1:
    st.header("1. Get Creators to Scrape")
    st.write("Fetch creators from Google Sheets who haven't been scraped recently.")

    if st.button("Fetch from Google Sheets", type="primary"):
        with st.spinner("Syncing Google Sheet to Database..."):

            # 1. Pull new data from the published sheet into SQLite
            new_additions = sheet_ingestor.sync_creators_to_db()
            st.toast(
                f"Synced Sheet! {new_additions} new profiles added.", icon="✅")

            # 2. Ask the database who is due for a scrape
            apify_urls = sheet_ingestor.generate_scrape_list(
                platform='instagram')

            # 3. Save to session state for the UI
            st.session_state['scrape_list'] = "\n".join(apify_urls)

            if apify_urls:
                st.success(
                    f"Found {len(apify_urls)} profiles requiring updates!")
            else:
                st.info("All profiles are up to date! No scraping needed.")

    if st.session_state['scrape_list']:
        st.write("### Paste these into Apify:")
        st.info(
            "Hover over the box below and click the 'Copy' icon in the top right. 📋")
        st.code(st.session_state['scrape_list'], language="text")
        st.markdown(
            "[👉 Click here to open Apify Instagram Scraper](https://apify.com/apify/instagram-scraper)", unsafe_allow_html=True)

# TAB 2: INGEST APIFY DATA
with tab2:
    st.header("2. Upload Apify Results")
    st.write(
        "Drag and drop the CSV downloaded from Apify to ingest it into the SQLite database.")

    uploaded_file = st.file_uploader("Upload Apify CSV", type=["csv"])

    if uploaded_file is not None:
        # Show a preview of the uploaded csv
        df = pd.read_csv(uploaded_file)
        st.write(f"Preview: Found {len(df)} rows.")
        st.dataframe(df.head(3))

        if st.button("Save to SQLite Database", type="primary"):
            with st.spinner("Ingesting data, removing duplicates, and updating metrics..."):
                # TODO: Plug in the SQLite insertion logic here
                time.sleep(2)  # Simulating database writing
                st.success("✅ Data successfully saved to the database!")


# TAB 3: AI INSIGHTS & HOOK EXTRACTION
with tab3:
    st.header("3. Extract Viral Hooks")
    st.write(
        "Find outliers in the database and run Whisper AI to extract audio hooks.")

    # Let the user tweak the threshold before running (not sure about keeping this)
    z_score_threshold = st.slider("Z-Score Threshold (Higher = more strictly viral)",
                                  min_value=1.0, max_value=3.0, value=1.5, step=0.1)

    if st.button("Run AI Insights Engine", type="primary"):
        with st.status("Running AI Engine...", expanded=True) as status:
            st.write("🔍 Identifying outliers based on Z-Score...")
            time.sleep(1)  # Mock delay

            st.write("🎧 Downloading audio tracks from CDN...")
            time.sleep(1)  # Mock delay

            st.write("🤖 Transcribing with Whisper...")
            time.sleep(2)  # Mock delay

            status.update(label="AI Extraction Complete!",
                          state="complete", expanded=False)

        # Mock Results Table
        st.success("Extracted 2 new hooks!")
        mock_results = pd.DataFrame({
            "Creator": ["zuck", "mosseri"],
            "Z-Score": [2.1, 1.8],
            "Hook": ["This is the future of mixed reality.", "We are making some changes to the algorithm."]
        })
        st.dataframe(mock_results, use_container_width=True)
