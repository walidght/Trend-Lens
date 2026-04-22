from datetime import date, datetime, time, timedelta

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
            elif not repo.add_sheet(new_name, new_url):
                st.error("A sheet with that name already exists.")
            else:
                new_sheet_id = repo.get_all_sheets()[new_name]["id"]
                with st.spinner("Syncing creators from sheet..."):
                    sheet_ingestor = SheetIngestor(config, repo)
                    new_creators = sheet_ingestor.sync_creators_to_db(new_sheet_id, new_url)
                st.success(f"Sheet saved — {len(new_creators)} new profile(s) added.")
                if new_creators:
                    with st.spinner(f"Backfilling {len(new_creators)} new creator(s)..."):
                        scraper = ApifyAdapter(config.apify_api_token)
                        data_ingestor = DataIngestor(config, repo)
                        orchestrator = AutomationOrchestrator(config, repo, scraper, data_ingestor)
                        result = orchestrator.run_backfill(new_creators)
                    if result["status"] == "success":
                        st.success(f"Backfill done — {result['new_videos']} videos, {result['new_metrics']} metrics.")
                    elif result["status"] == "partial":
                        st.warning(f"Backfill partial — {result['message']}")
                    else:
                        st.error(f"Backfill failed — {result['message']}")
                st.rerun()


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

col_a, col_b, col_c = st.columns(3)
with col_a:
    sync_clicked = st.button("Sync from Google Sheet", use_container_width=True)
with col_b:
    all_links_clicked = st.button("All profile links", use_container_width=True)
with col_c:
    due_links_clicked = st.button("Due for scrape", use_container_width=True, type="primary")

if sync_clicked:
    with st.spinner("Syncing creators from Google Sheet..."):
        sheet_ingestor = SheetIngestor(config, repo)
        new_creators = sheet_ingestor.sync_creators_to_db(active_sheet_id, active_sheet_url)
    st.success(f"Sync complete — {len(new_creators)} new profile(s) added.")
    if new_creators:
        st.session_state['pending_backfill'] = new_creators

if all_links_clicked:
    from config.mappings import build_profile_url
    usernames = repo.get_all_creators_for_sheet(active_sheet_id, platform_for_links)
    if usernames:
        urls = [build_profile_url(platform_for_links, u) for u in usernames]
        st.session_state['profile_links'] = "\n".join(urls)
        st.session_state['profile_links_label'] = f"all {len(urls)} profile(s)"
    else:
        st.session_state['profile_links'] = ""
        st.session_state['profile_links_label'] = "none"

if due_links_clicked:
    with st.spinner("Computing profiles due for a scrape..."):
        sheet_ingestor = SheetIngestor(config, repo)
        urls = sheet_ingestor.generate_scrape_list(platform=platform_for_links, sheet_id=active_sheet_id)
    st.session_state['profile_links'] = "\n".join(urls)
    st.session_state['profile_links_label'] = f"{len(urls)} profile(s) due for scrape"

if st.session_state.get('profile_links') is not None:
    urls_text = st.session_state['profile_links']
    label = st.session_state.get('profile_links_label', '')
    if urls_text.strip():
        st.write(f"**{label}:**")
        st.code(urls_text, language="text")
    else:
        st.info("No profiles found for the selected platform / filter.")

# --- Backfill confirmation for newly-added creators ---
pending = st.session_state.get('pending_backfill') or []
if pending:
    st.warning(
        f"**{len(pending)} new creator(s)** need a history backfill "
        f"({config.backfill_max_items} posts each) to seed their baseline metrics."
    )
    c1, c2 = st.columns(2)
    with c1:
        run_backfill_clicked = st.button("Run backfill now", type="primary", use_container_width=True, key="run_pending_backfill")
    with c2:
        skip_backfill_clicked = st.button("Skip for now", use_container_width=True, key="skip_pending_backfill")

    if skip_backfill_clicked:
        st.session_state['pending_backfill'] = []
        st.info("Skipped. These profiles remain retryable (last_scraped_at is NULL).")

    if run_backfill_clicked:
        with st.status("Running backfill...", expanded=True) as status_box:
            scraper = ApifyAdapter(config.apify_api_token)
            data_ingestor = DataIngestor(config, repo)
            orchestrator = AutomationOrchestrator(config, repo, scraper, data_ingestor)
            result = orchestrator.run_backfill(pending)

            if result["status"] == "success":
                status_box.update(label="✅ Backfill complete", state="complete", expanded=False)
            elif result["status"] == "partial":
                status_box.update(label="⚠️ Backfill partial", state="complete", expanded=True)
            else:
                status_box.update(label="❌ Backfill failed", state="error", expanded=True)
            st.write(result["message"])
            m1, m2 = st.columns(2)
            m1.metric("New videos", result.get("new_videos", 0))
            m2.metric("New daily metrics", result.get("new_metrics", 0))

        st.session_state['pending_backfill'] = []

# --- Retry backfill for creators not scraped within the candidate window ---
pending_in_db = repo.get_creators_needing_backfill(sheet_id=active_sheet_id, candidate_days=config.candidate_days)
if pending_in_db and not pending:
    if st.button(f"Backfill {len(pending_in_db)} creator(s) not scraped in last {config.candidate_days} days", use_container_width=True):
        with st.status("Running backfill...", expanded=True) as status_box:
            scraper = ApifyAdapter(config.apify_api_token)
            data_ingestor = DataIngestor(config, repo)
            orchestrator = AutomationOrchestrator(config, repo, scraper, data_ingestor)
            result = orchestrator.run_backfill(pending_in_db)

            if result["status"] == "success":
                status_box.update(label="✅ Backfill complete", state="complete", expanded=False)
            elif result["status"] == "partial":
                status_box.update(label="⚠️ Backfill partial", state="complete", expanded=True)
            else:
                status_box.update(label="❌ Backfill failed", state="error", expanded=True)
            st.write(result["message"])
            m1, m2 = st.columns(2)
            m1.metric("New videos", result.get("new_videos", 0))
            m2.metric("New daily metrics", result.get("new_metrics", 0))

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
        orchestrator = AutomationOrchestrator(config, repo, scraper, ingestor)

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

st.divider()


# ------------------------------------------
# 5. GENERATE CLIENT REPORT
# ------------------------------------------
st.subheader("5. Generate client report")
st.write("Export viral hooks flagged within a date range for the active sheet.")

today = date.today()
col_start, col_end = st.columns(2)
with col_start:
    start_date = st.date_input("Start date", value=today - timedelta(days=7), key="report_start")
with col_end:
    end_date = st.date_input("End date", value=today, key="report_end")

if st.button("Generate report", type="primary", use_container_width=True):
    if start_date > end_date:
        st.error("Start date must be on or before end date.")
    else:
        start_dt = datetime.combine(start_date, time.min)
        end_dt = datetime.combine(end_date, time.max)
        report_df = repo.get_viral_hooks_for_report(active_sheet_id, start_dt, end_dt)

        if report_df.empty:
            st.info("No viral hooks found in this date range.")
        else:
            st.success(f"Found {len(report_df)} viral hook(s).")
            st.dataframe(report_df, use_container_width=True)
            filename = f"viral_hooks_{active_sheet_name}_{start_date}_to_{end_date}.csv"
            st.download_button(
                label="Download CSV",
                data=report_df.to_csv(index=False).encode("utf-8"),
                file_name=filename,
                mime="text/csv",
                use_container_width=True,
            )
