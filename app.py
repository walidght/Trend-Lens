import streamlit as st
from config.settings import AppConfig
from core.database import DatabaseManager
from core.repository import TrendLensRepository

# Global page config
st.set_page_config(page_title="TrendDelta.co Analytics",
                   page_icon="🎣", layout="wide")

# We use session state so the database doesn't reconnect every time we change pages
if 'repo' not in st.session_state:
    config = AppConfig()
    db = DatabaseManager(config.db_path)
    st.session_state.repo = TrendLensRepository(db)
    st.session_state.config = config

repo = st.session_state.repo

# GLOBAL SIDEBAR (PERSISTS ACROSS ALL PAGES)
with st.sidebar:
    st.title("📈 TrendDelta.co")
    st.markdown("---")

    # Fetch sheets and create the global dropdown
    all_sheets = repo.get_all_sheets()

    if not all_sheets:
        st.warning("No sheets found. Go to Pipeline to add one.")
        st.session_state['active_sheet_id'] = None
    else:
        sheet_names = list(all_sheets.keys())

        # Keep the dropdown selection persistent if they click around
        current_index = 0
        if 'active_sheet_name' in st.session_state and st.session_state['active_sheet_name'] in sheet_names:
            current_index = sheet_names.index(
                st.session_state['active_sheet_name'])

        selected_sheet_name = st.selectbox(
            "🏢 Select Client / Niche", sheet_names, index=current_index)

        # Save to session state so all pages instantly know which client we are looking at!
        st.session_state['active_sheet_id'] = all_sheets[selected_sheet_name]["id"]
        st.session_state['active_sheet_name'] = selected_sheet_name
        st.session_state['active_sheet_url'] = all_sheets[selected_sheet_name]["url"]

# MULTIPAGE NAVIGATION 
pipeline_page = st.Page("pages/1_pipeline.py",
                        title="Data Pipeline", icon="⚙️")
channel_page = st.Page("pages/2_channel.py",
                       title="Channel Dashboard", icon="📊")
video_page = st.Page("pages/3_video.py", title="Video Deep Dive", icon="🎬")

# Setup the router and run it
pg = st.navigation([pipeline_page, channel_page, video_page])
pg.run()
