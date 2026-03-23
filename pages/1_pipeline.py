import streamlit as st
import pandas as pd
from analyzers.trend_analyzer import TrendAnalyzer
from config.mappings import get_available_platforms
from core.sheet_ingestor import SheetIngestor
from core.ingestors import DataIngestor
from core.pipeline import PipelineOrchestrator
import plotly.express as px

# Grab the database and config from the global router
repo = st.session_state.repo
config = st.session_state.config

# Initialize the business logic classes
sheet_ingestor = SheetIngestor(config, repo)
trend_analyzer = TrendAnalyzer(config, repo)
apify_ingestor = DataIngestor(config, repo)


@st.cache_resource
def get_transcriber():
    from core.transcriber import TranscriptionService
    return TranscriptionService(config.whisper_model)


st.title("Data Pipeline")

st.set_page_config(page_title="TrendDelta.co Pipeline",
                   page_icon="📈", layout="centered")

st.markdown(
    "Manage your ETL pipeline: Fetch creators, ingest Apify data, and extract AI hooks.")

# We use session state to remember data between button clicks (so the screen doesn't clear)
if 'scrape_list' not in st.session_state:
    st.session_state['scrape_list'] = ""

# Workflow tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "1️⃣ Manage Sheets",
    "2️⃣ Ingest Data",
    "3️⃣ AI Extraction",
    "4️⃣ 📈 Visual Dashboard"
])


# ==========================================
# TAB 1: MANAGE & FETCH SHEETS
# ==========================================
with tab1:
    st.header("1. Manage & Fetch Sheets")

    # --- ADD NEW SHEET UI ---
    with st.expander("➕ Add a New Client/Niche Sheet"):
        new_sheet_name = st.text_input("Sheet Name (e.g., Tech Founders)")
        new_sheet_url = st.text_input("Published CSV URL")
        if st.button("Save Sheet"):
            if new_sheet_name and new_sheet_url:
                if repo.add_sheet(new_sheet_name, new_sheet_url):
                    st.success("Sheet saved! Refreshing...")
                    st.rerun()
                else:
                    st.error("A sheet with that name already exists.")

    st.divider()

    # --- SYNC ACTIVE SHEET (Driven by the Sidebar!) ---
    if st.session_state.get('active_sheet_id') is None:
        st.warning(
            "👈 Please select or add a Client Sheet in the sidebar to get started.")
        st.stop()

    active_sheet_name = st.session_state['active_sheet_name']
    active_sheet_id = st.session_state['active_sheet_id']
    active_sheet_url = st.session_state['active_sheet_url']

    st.info(f"Currently managing data for: **{active_sheet_name}**")

    if st.button(f"Fetch & Generate Apify Links for '{active_sheet_name}'", type="primary"):
        with st.spinner("Syncing creators and linking them to this sheet..."):

            # We now pass the active ID and URL straight from the session state!
            new_additions = sheet_ingestor.sync_creators_to_db(
                active_sheet_id, active_sheet_url)
            st.toast(
                f"Synced! {new_additions} brand new profiles added to the database.", icon="✅")

            # Generate the list for this specific sheet
            # TODO: rn we have instagram hardcoded in the function, we should make it dynamic for tiktok and youtube as well
            apify_urls = sheet_ingestor.generate_scrape_list(
                platform='instagram', sheet_id=active_sheet_id)

            st.session_state['scrape_list'] = "\n".join(apify_urls)

            if apify_urls:
                st.success(
                    f"Found {len(apify_urls)} profiles requiring updates!")
            else:
                st.info(
                    "All profiles in this sheet are up to date! No scraping needed.")

    if st.session_state.get('scrape_list'):
        st.write("### Paste these into Apify:")
        st.code(st.session_state['scrape_list'], language="text")


# TAB 2: INGEST APIFY DATA
with tab2:
    st.header("2. Upload Apify Results")
    st.write(
        "Drag and drop the CSV downloaded from Apify to ingest it into the SQLite database.")

    # Dynamically populate the dropdown from  registry
    platform_choice = st.selectbox("Data Source", get_available_platforms())
    uploaded_file = st.file_uploader("Upload Apify CSV", type=["csv"])

    if uploaded_file is not None and st.button("Normalize & Save to Database"):
        # Show a preview of the uploaded csv
        df = pd.read_csv(uploaded_file)
        st.write(f"Preview: Found {len(df)} rows.")
        st.dataframe(df.head(3))

        with st.spinner(f"Ingesting {platform_choice} data..."):
            unified_ingestor = DataIngestor(config, repo)
            
            # 2. Just pass the string name. The ingestor handles the rest!
            stats = unified_ingestor.ingest_dataframe(df, platform_name=platform_choice)
            
            st.success("✅ Data normalized and ingested successfully!")
            st.metric("New Videos Tracked", stats["new_videos"])
            st.metric("New Daily Metrics", stats["new_metrics"])


# TAB 3: AI INSIGHTS & HOOK EXTRACTION
with tab3:
    st.header("3. Extract Viral Hooks")
    st.write(
        "Find outliers in the database and run Whisper AI to extract audio hooks.")

    # Make sure we actually have a sheet selected
    if 'active_sheet_id' not in st.session_state:
        st.warning("Please select a sheet on the sidebar.")
        st.stop()

    st.info(
        f"📊 Currently analyzing isolated data for: **{st.session_state['active_sheet_name']}**")

    # Let the user tweak the threshold before running (not sure about keeping this)
    z_score_threshold = st.slider("Z-Score Threshold (Higher = more strictly viral)",
                                  min_value=1.0, max_value=3.0, value=config.z_score_threshold, step=0.1)

    if st.button("Run AI Insights Engine", type="primary"):
        # Override the config temporarily for this run
        config.z_score_threshold = z_score_threshold

        # Initialize the Analyzer and Pipeline with the DB connection
        trend_analyzer = TrendAnalyzer(config, repo)
        transcriber = get_transcriber()
        pipeline = PipelineOrchestrator(
            config, repo, trend_analyzer, transcriber)

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
            total_extracted = pipeline.run(
                sheet_id=st.session_state['active_sheet_id'], progress_callback=update_ui)

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


# ==========================================
# TAB 4: VISUAL DASHBOARD & EXPORT
# ==========================================
with tab4:
    st.header("4. Visual Dashboard & Export")

    if 'active_sheet_id' not in st.session_state:
        st.warning("Please go to Tab 1 and select an active sheet first.")
        st.stop()

    st.info(
        f"📈 Visualizing data for: **{st.session_state['active_sheet_name']}**")

    # 1. Fetch the data
    dash_df = repo.get_dashboard_data(st.session_state['active_sheet_id'])

    if dash_df.empty:
        st.write("No data available to chart yet.")
    else:
        # Create a dynamic threshold slider just for the chart
        chart_threshold = st.slider(
            "Highlight Outliers (Z-Score >)", min_value=1.0, max_value=3.5, value=1.5, step=0.1)

        # Create a new column to color-code the dots (Viral vs Normal)
        dash_df['Is Viral'] = dash_df['view_z_score'] >= chart_threshold

        # 2. Draw the Interactive Scatter Plot!
        fig = px.scatter(
            dash_df,
            x="published_date",
            y="views",
            color="Is Viral",
            # Bright green for viral, gray for normal
            color_discrete_map={True: '#00ff00', False: '#555555'},
            hover_name="username",
            hover_data={
                "views": True,
                "view_z_score": ':.2f',
                "hook_text": True,
                "Is Viral": False,
                "published_date": False
            },
            title="Channel Performance: Views vs Published Date",
            labels={"published_date": "Date Published", "views": "Total Views"}
        )

        # Make the chart look sleek
        fig.update_traces(marker=dict(size=10, opacity=0.8,
                          line=dict(width=1, color='DarkSlateGrey')))
        st.plotly_chart(
            fig,
            use_container_width=True,
            key=f"plotly_chart_{st.session_state['active_sheet_id']}"
        )

        st.divider()

        # 3. The "Export Report" Feature!
        st.subheader("📥 Export Final Report")
        st.write("Download the extracted viral hooks to send to your clients.")

        # Filter down to just the highly viral videos that actually have a hook transcribed
        export_df = dash_df[(dash_df['Is Viral'] == True)
                            & (dash_df['hook_text'].notna())]

        if not export_df.empty:
            st.dataframe(export_df[['username', 'views', 'view_z_score',
                         'hook_text', 'url']], use_container_width=True)

            # Convert to CSV for downloading
            csv = export_df.to_csv(index=False).encode('utf-8')

            st.download_button(
                label=f"Download {len(export_df)} Viral Hooks as CSV",
                data=csv,
                file_name=f"viral_report_{st.session_state['active_sheet_name']}.csv",
                mime="text/csv",
                type="primary"
            )
        else:
            st.info(
                "No viral hooks available to export at this threshold. Run the AI pipeline in Tab 3 first!")
