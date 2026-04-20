import streamlit as st
import pandas as pd

from config.settings import AppConfig
from config.mappings import get_available_platforms
from core.database import DatabaseManager
from core.repository import TrendLensRepository
from core.sheet_ingestor import SheetIngestor
from core.ingestors import DataIngestor
from core.apify_adapter import ApifyAdapter
from core.automation import AutomationOrchestrator
from core.pipeline import PipelineOrchestrator
from analyzers.trend_analyzer import TrendAnalyzer


st.set_page_config(
    page_title="TrendDelta.co Control Panel",
    page_icon="🎛️",
    layout="centered",
)


# ==========================================
# BOOTSTRAP (runs once per session)
# ==========================================
if 'repo' not in st.session_state:
    config = AppConfig()
    db = DatabaseManager(config.db_path)
    st.session_state.config = config
    st.session_state.repo = TrendLensRepository(db)

config: AppConfig = st.session_state.config
repo: TrendLensRepository = st.session_state.repo


@st.cache_resource
def get_transcriber(model_name: str):
    from core.transcriber import TranscriptionService
    return TranscriptionService(model_name)


# ==========================================
# SIDEBAR — the single global control
# ==========================================
with st.sidebar:
    st.title("🎛️ TrendDelta.co")
    st.caption("Control panel — analytics live in Metabase.")
    st.divider()

    all_sheets = repo.get_all_sheets()

    if all_sheets:
        sheet_names = list(all_sheets.keys())
        current_index = 0
        if st.session_state.get('active_sheet_name') in sheet_names:
            current_index = sheet_names.index(st.session_state['active_sheet_name'])

        selected = st.selectbox("Active Client / Niche", sheet_names, index=current_index)
        st.session_state['active_sheet_name'] = selected
        st.session_state['active_sheet_id'] = all_sheets[selected]["id"]
        st.session_state['active_sheet_url'] = all_sheets[selected]["url"]
    else:
        st.info("No sheets yet — add one below.")
        st.session_state['active_sheet_id'] = None
        st.session_state['active_sheet_name'] = None
        st.session_state['active_sheet_url'] = None

    with st.expander("➕ Add new sheet"):
        new_name = st.text_input("Name", key="new_sheet_name").strip()
        new_url = st.text_input("Published CSV URL", key="new_sheet_url").strip()
        if st.button("Save sheet", use_container_width=True):
            if not (new_name and new_url):
                st.error("Both fields are required.")
            elif repo.add_sheet(new_name, new_url):
                st.success("Sheet saved.")
                st.rerun()
            else:
                st.error("A sheet with that name already exists.")


# ==========================================
# MAIN — Control Panel
# ==========================================
active_sheet_id = st.session_state.get('active_sheet_id')
active_sheet_name = st.session_state.get('active_sheet_name')
active_sheet_url = st.session_state.get('active_sheet_url')

st.title("Control Panel")

if active_sheet_id is None:
    st.warning("👈 Add and select a sheet in the sidebar to get started.")
    st.stop()

st.caption(f"Operating on: **{active_sheet_name}**")
st.divider()


# ------------------------------------------
# 1. SYNC CREATORS & FETCH PROFILE LINKS
# ------------------------------------------
st.subheader("1. Sync creators & fetch profile links")
st.write("Pull the latest creator list from the Google Sheet and list which profiles are due for a scrape.")

platform_for_links = st.selectbox(
    "Platform",
    ["instagram", "tiktok", "youtube"],
    key="link_platform",
)

col_a, col_b = st.columns(2)
with col_a:
    sync_clicked = st.button("Sync from Google Sheet", use_container_width=True)
with col_b:
    links_clicked = st.button("Generate profile links", use_container_width=True, type="primary")

if sync_clicked:
    with st.spinner("Syncing creators from Google Sheet..."):
        ingestor = SheetIngestor(config, repo)
        added = ingestor.sync_creators_to_db(active_sheet_id, active_sheet_url)
    st.success(f"Sync complete — {added} new profile(s) added.")

if links_clicked:
    with st.spinner("Computing profiles due for a scrape..."):
        ingestor = SheetIngestor(config, repo)
        urls = ingestor.generate_scrape_list(platform=platform_for_links, sheet_id=active_sheet_id)
        st.session_state['scrape_list'] = "\n".join(urls)

if st.session_state.get('scrape_list'):
    urls_text = st.session_state['scrape_list']
    count = len([u for u in urls_text.splitlines() if u.strip()])
    if count:
        st.write(f"**{count} profile(s) due** — paste into Apify:")
        st.code(urls_text, language="text")
    else:
        st.info("All profiles are up to date.")

st.divider()


# ------------------------------------------
# 2. RUN FULL AUTOMATED PIPELINE
# ------------------------------------------
st.subheader("2. Run full automated pipeline")
st.write("Triggers an Apify cloud scrape for profiles due, then ingests the results.")

auto_platform = st.selectbox("Platform source", get_available_platforms(), key="auto_platform")

if st.button("Run full pipeline", type="primary", use_container_width=True):
    with st.status("Running automated pipeline...", expanded=True) as status:
        scraper = ApifyAdapter(config.apify_api_token)
        ingestor = DataIngestor(config, repo)
        orchestrator = AutomationOrchestrator(repo, scraper, ingestor)

        result = orchestrator.run_auto_sync(
            platform_name=auto_platform,
            sheet_id=active_sheet_id,
        )

        if result["status"] == "success":
            status.update(label="✅ Pipeline complete", state="complete", expanded=False)
            st.write(result["message"])
            if result.get("new_videos") is not None:
                m1, m2 = st.columns(2)
                m1.metric("New videos", result["new_videos"])
                m2.metric("New daily metrics", result["new_metrics"])
        else:
            status.update(label="❌ Pipeline failed", state="error", expanded=True)
            st.error(result["message"])

st.divider()


# ------------------------------------------
# 3. MANUAL CSV INGEST
# ------------------------------------------
st.subheader("3. Manual CSV ingest")
st.write("Upload a CSV exported from Apify to ingest it directly.")

csv_platform = st.selectbox("Data source", get_available_platforms(), key="csv_platform")
uploaded_file = st.file_uploader("Upload Apify CSV", type=["csv"])

if uploaded_file is not None:
    df_preview = pd.read_csv(uploaded_file)
    st.caption(f"Preview: {len(df_preview)} rows")
    st.dataframe(df_preview.head(3), use_container_width=True)

    if st.button("Normalize & ingest", type="primary", use_container_width=True):
        with st.spinner(f"Ingesting {csv_platform} data..."):
            ingestor = DataIngestor(config, repo)
            stats = ingestor.ingest_dataframe(df_preview, platform_name=csv_platform)
        st.success("Ingested.")
        m1, m2 = st.columns(2)
        m1.metric("New videos", stats["new_videos"])
        m2.metric("New daily metrics", stats["new_metrics"])

st.divider()


# ------------------------------------------
# 4. AI INSIGHTS — OUTLIER HOOK EXTRACTION
# ------------------------------------------
st.subheader("4. AI insights — extract viral hooks")
st.write("Identify outliers for the active sheet and transcribe their audio hooks via Whisper.")

z_threshold = st.slider(
    "Z-score threshold",
    min_value=1.0, max_value=3.0,
    value=float(config.z_score_threshold), step=0.1,
)

if st.button("Run AI insights", type="primary", use_container_width=True):
    config.z_score_threshold = z_threshold

    analyzer = TrendAnalyzer(config, repo)
    transcriber = get_transcriber(config.whisper_model)
    pipeline = PipelineOrchestrator(config, repo, analyzer, transcriber)

    progress_bar = st.progress(0)
    status_text = st.empty()

    def update_ui(current, total, current_video_id):
        if total > 0:
            progress_bar.progress(int((current / total) * 100))
        status_text.text(f"Processed {current} / {total} — {current_video_id}")

    with st.spinner("Analyzing metrics and running Whisper..."):
        total_extracted = pipeline.run(sheet_id=active_sheet_id, progress_callback=update_ui)

    if total_extracted > 0:
        st.success(f"Extracted {total_extracted} new hook(s).")
        preview_df = repo.get_latest_hooks_preview(limit=10)
        st.write("**Latest hooks:**")
        st.dataframe(preview_df, use_container_width=True)
    else:
        st.info("No new outliers above the current threshold.")
