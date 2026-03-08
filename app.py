import streamlit as st
import pandas as pd
import time
from analyzers import InstagramAnalyzer
from config import AppConfig
from core import DatabaseManager
from core import SheetIngestor
from core import ApifyIngestor
from core import PipelineOrchestrator
from core import TrendLensRepository

config = AppConfig()
db = DatabaseManager(config.db_path)
repo = TrendLensRepository(db)
sheet_ingestor = SheetIngestor(config, repo)
insta_analyzer = InstagramAnalyzer(config, repo)
apify_ingestor = ApifyIngestor(config, repo)

# Caching the AI model so it doesn't reload on every button click!


@st.cache_resource
def get_transcriber():
    from core import TranscriptionService
    return TranscriptionService(config.whisper_model)


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
                stats = apify_ingestor.ingest_dataframe(df)

                st.success(f"✅ Data successfully saved to the database!")
                st.info(
                    f"📊 Added or updated {stats['new_videos']} new videos to the catalog.")
                st.info(
                    f"📈 Logged {stats['new_metrics']} new daily metric snapshots.")


# TAB 3: AI INSIGHTS & HOOK EXTRACTION
with tab3:
    st.header("3. Extract Viral Hooks")
    st.write(
        "Find outliers in the database and run Whisper AI to extract audio hooks.")

    # Let the user tweak the threshold before running (not sure about keeping this)
    z_score_threshold = st.slider("Z-Score Threshold (Higher = more strictly viral)",
                                  min_value=1.0, max_value=3.0, value=1.5, step=0.1)

    if st.button("Run AI Insights Engine", type="primary"):
        # Override the config temporarily for this run
        insta_analyzer.config.z_score_threshold = z_score_threshold

        # Initialize the Analyzer and Pipeline with the DB connection
        insta_analyzer = InstagramAnalyzer(config, repo)
        transcriber = get_transcriber()
        pipeline = PipelineOrchestrator(
            config, repo, insta_analyzer, transcriber)

        # Setup UI Progress elements
        progress_bar = st.progress(0)
        status_text = st.empty()

        def update_ui(current, total, current_video_id):
            """Callback function to update Streamlit from inside the pipeline loop."""
            # Prevent division by zero if total is somehow 0
            if total > 0:
                percent_complete = int((current / total) * 100)
                progress_bar.progress(percent_complete)
            status_text.text(
                f"Processed {current} of {total} viral videos... (Latest: {current_video_id})")

        with st.spinner("Analyzing metrics and powering up AI..."):
            # Run the extraction using your updated PipelineOrchestrator!
            total_extracted = pipeline.run(progress_callback=update_ui)

        if total_extracted > 0:
            st.success(
                f"🎉 Successfully extracted {total_extracted} new viral hooks!")
            st.balloons()

            # Show a preview of the newest hooks directly from SQLite
            preview_df = repo.get_latest_hooks_preview(limit=10)

            st.write("### Latest Extracted Hooks:")
            st.dataframe(preview_df, use_container_width=True)

        else:
            st.info("No new viral videos found above the current Z-score threshold. Try lowering the threshold or importing new Apify data.")
